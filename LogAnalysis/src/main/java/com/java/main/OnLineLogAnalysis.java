package com.java.main;



import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.json.JSONObject;

import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by jpwu on 2017/2/14.
 *
 * 主要使用spark streaming and spark sql来实现:
 * 1.从kafka0.10 cluster读取json格式的log ，其格式: 机器名称 服务名称 时间 日志级别 日志信息
 {"time":"2017-04-11 22:40:47,981","logtype":"INFO","loginfo":"org.apache.hadoop.hdfs.server.datanode.DataNode:PacketResponder: BP-469682100-172.16.101.55-1489763711932:blk_1073775313_34497, type=HAS_DOWNSTREAM_IN_PIPELINE terminating"}
 {"time":"2017-04-11 22:40:48,120","logtype":"INFO","loginfo":"org.apache.hadoop.hdfs.server.datanode.DataNode:Receiving BP-469682100-172.16.101.55-1489763711932:blk_1073775314_34498 src: /172.16.101.59:49095 dest: /172.16.101.60:50010"}

 * 2.每隔5秒统计最近15秒出现的机器，日志级别为info,debug,warn,error次数

 * 3.每隔5秒统计最近15秒出现的机器，日志信息出现自定义alert词的次数

 *  1.消费kafka json数据转换为DF,然后show()
 *  2.group by语句
 *  3.写入到InfluxDB
 *  4.广播变量+更新(自定义预警关键词)
 *
 */
public class OnLineLogAnalysis {

    //定义滑动间隔为5秒,窗口时间为30秒，即为计算每5秒的过去15秒的数据
    private static  final Duration slide_interval= new Duration(5 * 1000);
    private static  final Duration window_length= new Duration(5 * 1000);


    private static final Pattern regexSpace = Pattern.compile(" ");

    static String[] spiltstr;
    static CDHRoleLog cdhRoleLog;
    static String sqlstr;
    static Timestamp recordTimestamp;
    static String key;
    static String value;
    static String host_service_logtype;

    private static InfluxDB influxDB;
    private final static String dbName = "online_log_analysis";
    private static JSONObject jsonlogline;

    static List<String> alertList= new ArrayList<String>();
    static String alertsql="";
    static Broadcast<List> bcAlertList ;

    private static void onlineLogsAnalysis(){
        try {

            //定义连接influxdb
            influxDB = InfluxDBFactory.connect("http://" + InfluxDBUtils.getInfluxIP() + ":" + InfluxDBUtils.getInfluxPORT(true), "admin", "admin");
            final String rp = InfluxDBUtils.defaultRetentionPolicy(influxDB.version());

            //1.使用 SparkSession,JavaSparkContext, JavaStreamingContext来定义 对象 jssc
            final   SparkSession ss=  new SparkSession.Builder()
                    .master("local[2]")
//                    .master("yarn")
                    .appName("OnLineLogAnalysis")
                    .getOrCreate();
            JavaSparkContext sc=JavaSparkContext.fromSparkContext(ss.sparkContext());
            JavaStreamingContext jssc= new JavaStreamingContext(sc,slide_interval);


            /* 2.开启checkpoint机制，把checkpoint中的数据目录设置为hdfs目录
            hdfs dfs -mkdir -p hdfs://nameservice1/spark/checkpointdata
            hdfs dfs -chmod -R 777 hdfs://nameservice1/spark/checkpointdata
            hdfs dfs -ls hdfs://nameservice1/spark/checkpointdata
             */
           jssc.checkpoint("hdfs://ns/user/checkpointdata");


            //3.设置kafka的map参数
            Map<String,Object> kafkaParams = new HashMap<String,Object>();

            kafkaParams.put("bootstrap.servers","bigdata-pro01.kfk.com:9092,bigdata-pro02.kfk.com:9092,bigdata-pro03.kfk.com:9092"); //定义kakfa 服务的地址
            kafkaParams.put("key.deserializer",StringDeserializer.class);//key的序列化类
            kafkaParams.put("value.deserializer",StringDeserializer.class);//value的序列化类
            kafkaParams.put("group.id","use_a_separate_group_id_for_each_stream");//制定consumer group
            kafkaParams.put("auto.offset.reset","latest");
            kafkaParams.put("enable.auto.commit",false);//是否自动确认offset

            //3.创建要从kafka去读取的topic的集合对象
            //Collection<String> topics = Arrays.asList("online_log_analysis");
            Collection<String> topics = Arrays.asList("DSHS");


            //4.输入流
            JavaInputDStream<ConsumerRecord<String,String>> lines= KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String,String> Subscribe(topics,kafkaParams));

            //5.将DS的RDD解析为JavaDStream<CDHRoleLog>     A DStream of RDD's that contain parsed CDH Role Logs.
            JavaDStream<CDHRoleLog> cdhRoleLogDStream =
                    lines.map(new Function<ConsumerRecord<String, String>, CDHRoleLog>() {
                        @Override
                        public CDHRoleLog call(ConsumerRecord<String, String> logline) throws Exception {
                            if(logline.value().contains("INFO")==true || logline.value().contains("WARN")==true || logline.value().contains("ERROR")==true || logline.value().contains("DEBUG")==true|| logline.value().contains("FATAL")==true){
                                try {
                                    //转换为json格式
                                    jsonlogline = new JSONObject(logline.value());

                                    cdhRoleLog = new CDHRoleLog( jsonlogline.getString("hostname"),
                                            jsonlogline.getString("servicename"),
                                            jsonlogline.getString("time"),
                                            jsonlogline.getString("logtype"),
                                            jsonlogline.getString("loginfo"));
                                }catch (Exception ex){

                                    System.out.println(ex.toString());
                                    cdhRoleLog=null;
                                }


                            }else {
                                //一个log的输出的非第一行，项目中暂时计划丢弃非第一行的数据
                                cdhRoleLog=null;
                            }




                            return  cdhRoleLog;
                        }
                    });

            //6.过滤无效的RDD
            JavaDStream<CDHRoleLog>  cdhRoleLogFilterDStream= cdhRoleLogDStream.filter(new Function<CDHRoleLog, Boolean>() {
                @Override
                public Boolean call(CDHRoleLog v1) throws Exception {
                    return v1!=null?true:false;
                }
            });

            //7.Splits the cdhRoleLogFilterDStream into a dstream of time windowed rdd's.
            JavaDStream<CDHRoleLog> windowDStream =
                    cdhRoleLogFilterDStream.window(window_length, slide_interval);




            //8.使用foreachRDD
            windowDStream.foreachRDD(new VoidFunction<JavaRDD<CDHRoleLog>>() {
                @Override
                public void call(JavaRDD<CDHRoleLog> cdhRoleLogJavaRDD) throws Exception {

                    //8.1判断rdd的数目
                    if (cdhRoleLogJavaRDD.count() == 0) {
                        System.out.println("No cdh role logs in this time interval");
                        return;
                    }


                    // 8.2从RDD创建Dataset
                    Dataset<Row> cdhRoleLogDR=ss.createDataFrame(cdhRoleLogJavaRDD,CDHRoleLog.class);

                    //8.3注册为临时表
                    cdhRoleLogDR.createOrReplaceTempView("cdhrolelogs");

                    //8.4调用自定义alert广播变量
                    bcAlertList=BroadcastAlert.getInstance().updateAndGet(ss,bcAlertList);
                    List<String> alertInfoList=bcAlertList.getValue();

                    //8.5拼接SQL
                    if (alertInfoList.size()>0)
                    {
                        //定义alertsql
                        alertsql="";
                        System.out.println("print custom alert words:");
                        for(String alertInfo:alertInfoList)
                        {
                            System.out.println(alertInfo);
                            alertsql=alertsql+" logInfo like '%"+alertInfo+"%' or";
                        }
                        alertsql=alertsql.substring(0,alertsql.length()-2);

                        //定义sql
                        sqlstr="SELECT hostName,serviceName,logType,COUNT(logType) FROM cdhrolelogs GROUP BY hostName,serviceName,logType union all " +
                                "SELECT t.hostName,t.serviceName,t.logType,COUNT(t.logType) FROM " +
                                "(SELECT hostName,serviceName,'alert' logType FROM cdhrolelogs where "+alertsql+") t " +
                                " GROUP BY t.hostName,t.serviceName,t.logType";
                    }else{
                        //定义sql
                        sqlstr="SELECT hostName,serviceName,logType,COUNT(logType) FROM cdhrolelogs GROUP BY hostName,serviceName,logType";

                    }




                    //8.6计算结果为List<Row>
                    List<Row> logtypecount = ss.sql(sqlstr).collectAsList();

                    value="";
                    //8.7循环处理
                    for(Row rowlog:logtypecount){
                        host_service_logtype=rowlog.get(0)+"_"+rowlog.get(1)+"_"+rowlog.get(2);
                        value=value + "logtype_count,host_service_logtype="+host_service_logtype +
                                " count="+String.valueOf(rowlog.getLong(3))+"\n";
                    }
                    //8.8 存储至influxdb
                    if(value.length()>0){
                        value=value.substring(0,value.length()); //去除最后一个字符“,”
                        //打印
                        System.out.println(value);
                        //保存
                        influxDB.write(dbName, rp, InfluxDB.ConsistencyLevel.ONE, value);
                    }



                }
            });

            jssc.start(); //启动流式计算
            jssc.awaitTermination(); //等待中断
            jssc.close(); //关闭

        }catch (Exception e){

            e.printStackTrace();

        }



    }


    public static void main(String[] args) {
        onlineLogsAnalysis();
    }


}
