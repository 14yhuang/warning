package com.kaola.main

/**
  * Created by K on 2019/2/12.
  *
  * 基类
  *
  */
case class CDHRoleLog(var hostName:String,var serviceName:String,var lineTimestamp:String,
                 var logType:String,var logInfo:String) extends Serializable{


  override def toString = String.format("%s %s %s %s %s",
    hostName, serviceName, lineTimestamp, logType, logInfo)
}
