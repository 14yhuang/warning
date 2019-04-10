package com.kaola.main

import java.util.{Calendar, Date}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

object BroadcastAlert {
    val user = "root"
    val password = "123456"
    val url = "jdbc:mysql://192.168.2.151:3306/onlineloganalysis"
    val altertable = "alertinfo_config"
    var lastUpdatedAt = Calendar.getInstance().getTime()

  def updateAndGet(sparkSession:SparkSession,bcAlertList:Broadcast[Array[String]]):Broadcast[Array[String]] = {
    var bcAlertListCopy = bcAlertList //传入的bcAlertList为val，无法重新赋值
    val currentDate = Calendar.getInstance().getTime()  //当前time
    val diff = currentDate.getTime()-lastUpdatedAt.getTime() //time差值
    if (bcAlertListCopy == null || diff >= 10000) { //Lets say we want to refresh every 1 min = 60000 ms
      if (bcAlertListCopy != null) {
        bcAlertListCopy.unpersist() //删除存储
      }
      lastUpdatedAt = new Date(System.currentTimeMillis()); //再次更新上次time

      // 定义sqlcontext
      var alertArr= sparkSession.read.format("jdbc")
        .option("url", url)
        .option("dbtable", altertable)
        .option("user", user)
        .option("password", password)
        .load().collect().map(row=>row(0).toString)
      //定义广播变量bcAlertList
      bcAlertListCopy= sparkSession.sparkContext.broadcast(alertArr)
    }
    bcAlertListCopy
    }
}
