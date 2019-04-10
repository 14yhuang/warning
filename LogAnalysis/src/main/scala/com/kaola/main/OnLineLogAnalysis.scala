package com.kaola.main

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.influxdb.{InfluxDB, InfluxDBFactory}
import org.json.JSONObject

/**
  * Created by k on 2019/2/13.
  *
  * 主要使用spark streaming and spark sql来实现:
  * 1.从kafka0.10 cluster读取json格式的log ，其格式: 机器名称 服务名称 时间 日志级别 日志信息
  * {"time":"2017-04-11 22:40:47,981","logtype":"INFO","loginfo":"org.apache.hadoop.hdfs.server.datanode.DataNode:PacketResponder: BP-469682100-172.16.101.55-1489763711932:blk_1073775313_34497, type=HAS_DOWNSTREAM_IN_PIPELINE terminating"}
  * {"time":"2017-04-11 22:40:48,120","logtype":"INFO","loginfo":"org.apache.hadoop.hdfs.server.datanode.DataNode:Receiving BP-469682100-172.16.101.55-1489763711932:blk_1073775314_34498 src: /172.16.101.59:49095 dest: /172.16.101.60:50010"}
  *
  * 2.每隔5秒统计最近15秒出现的机器，日志级别为info,debug,warn,error次数
  *
  * 3.每隔5秒统计最近15秒出现的机器，日志信息出现自定义alert词的次数
  *
  *  1.消费kafka json数据转换为DF,然后show()
  *  2.group by语句
  *  3.写入到InfluxDB
  *  4.广播变量+更新(自定义预警关键词)
  *
  */
object OnLineLogAnalysis {

  //定义滑动间隔InfluxDBUtils.defaultRetentionPolicy(influxDB.version());为5秒,窗口时间为30秒，即为计算每5秒的过去15秒的数据
  val slide_interval = new Duration(5 * 1000)
  val window_length = new Duration(5 * 1000)
//
//  val regexSpace = Pattern.compile(" ")
  var cdhRoleLog: CDHRoleLog = _
  var sqlstr:String = _
//  var value:String = _
//  var host_service_logtype:String = _
  private var influxDB: InfluxDB = _
  private val dbName = "online_log_analysis"
//  private var jsonlogline: JSONObject = _
//  var alertsql = ""
  var bcAlertList:Broadcast[Array[String]] = _

  def main(args: Array[String]): Unit = {
    onlineLogsAnalysis()
  }

  def onlineLogsAnalysis() = {
    try {
      //定义连接influxdb
      influxDB =  InfluxDBFactory.connect("http://" + InfluxDBUtils.getInfluxIP() + ":" + InfluxDBUtils.getInfluxPORT(true), "admin", "admin")
      val rp = InfluxDBUtils.defaultRetentionPolicy(influxDB.version())

      //1.使用 SparkSession,SparkContext, StreamingContext来定义 对象 ssc
      val spark = SparkSession.builder
        //.master("local[2]")
        //.master("yarn")
        .appName("OnLineLogAnalysis")
        .getOrCreate()

      val sc = spark.sparkContext
      val ssc = new StreamingContext(sc, slide_interval)
      /* 2.开启checkpoint机制，把checkpoint中的数据目录设置为hdfs目录
              hdfs dfs -mkdir -p hdfs://ns/spark/checkpointdata
              hdfs dfs -chmod -R 777 hdfs://ns/spark/checkpointdata
              hdfs dfs -ls hdfs://ns/spark/checkpointdata
               */
      ssc.checkpoint("hdfs://ns/user/checkpointdata")

      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "bigdata-pro01.kfk.com:9092,bigdata-pro02.kfk.com:9092,bigdata-pro03.kfk.com:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "use_a_separate_group_id_for_each_stream",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )
      val topics = Array("DSHS")

      val lines = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )

      val cdhRoleLogDStream = lines.map(logline=>{
        if(logline.value().contains("INFO")==true || logline.value().contains("WARN")==true || logline.value().contains("ERROR")==true || logline.value().contains("DEBUG")==true|| logline.value().contains("FATAL")==true) {
          try {
            //转换为json格式
            val jsonLogLine = new JSONObject(logline.value())

            cdhRoleLog = new CDHRoleLog(jsonLogLine.getString("hostname"),
              jsonLogLine.getString("servicename"),
              jsonLogLine.getString("time"),
              jsonLogLine.getString("logtype"),
              jsonLogLine.getString("loginfo"))
          }catch {
            case e: Exception=>println(e)
              cdhRoleLog == null
          }
        }else{
          cdhRoleLog == null
        }
        cdhRoleLog
      })

      val cdhRoleLogFilterDStream = cdhRoleLogDStream.filter(v1=>{
        if(v1!=null) true else false
      })

      val windowDStream = cdhRoleLogFilterDStream.window(window_length, slide_interval)

      windowDStream.foreachRDD(cdhRoleLogRDD=>{
        import spark.implicits._
        //8.1判断rdd的数目
        if (cdhRoleLogRDD.count() == 0) {
          println("No cdh role logs in this time interval")
        }else{
          // 8.2从RDD创建Dataset
          val cdhRoleLogDR = cdhRoleLogRDD.toDF()
          //8.3注册为临时表
          cdhRoleLogDR.createOrReplaceTempView("cdhrolelogs")
          //8.4调用自定义alert广播变量
          val alertInfoArr = BroadcastAlert.updateAndGet(spark,bcAlertList).value
          //8.5拼接SQL
          if(alertInfoArr.length > 0 ){
            var alertsql=""
            println("print custom alert words:")
            for (alertInfo<-alertInfoArr){
              alertsql=alertsql+" logInfo like '%"+alertInfo+"%' or"
            }
            alertsql=alertsql.substring(0,alertsql.length()-2)
            //定义sql
            sqlstr="SELECT hostName,serviceName,logType,COUNT(logType) FROM cdhrolelogs GROUP BY hostName,serviceName,logType union all " +
              "SELECT t.hostName,t.serviceName,t.logType,COUNT(t.logType) FROM " +
              "(SELECT hostName,serviceName,'alert' logType FROM cdhrolelogs where "+alertsql+") t " +
              " GROUP BY t.hostName,t.serviceName,t.logType"
          }else{
            //定义sql
            sqlstr="SELECT hostName,serviceName,logType,COUNT(logType) FROM cdhrolelogs GROUP BY hostName,serviceName,logType"
          }

          //8.6计算结果为List<Row>
          val logtypecount =  spark.sql(sqlstr).collect()

          var value = ""

          for(rowlog<-logtypecount){
            val host_service_logtype=rowlog.get(0)+"_"+rowlog.get(1)+"_"+rowlog.get(2);
            value=value + "logtype_count,host_service_logtype="+host_service_logtype +
              " count="+String.valueOf(rowlog.getLong(3))+"\n";
          }
          //8.8 存储至influxdb
          if(value.length()>0){
            value=value.substring(0,value.length()); //去除最后一个字符“,”
            //打印
            println(value)
            //保存
            influxDB.write(dbName, rp, InfluxDB.ConsistencyLevel.ONE, value);
          }

        }
      })
      ssc.start() //启动流式计算
      ssc.awaitTermination(); //等待中断
    }catch {
      case e:Exception=>println(e)
    }
  }
}
