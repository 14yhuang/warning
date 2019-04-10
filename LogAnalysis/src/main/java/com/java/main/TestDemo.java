package com.java.main;

/**
 * Created by jpwu on 2017/4/13.
 *
 * json 测试
 */
public class TestDemo {

    public static void main(String[] args) {

        String line="{\"hostname\":\"sht-sgmhadoopdn-02\",\"servicename\":\"dn2\",\"time\":\"2017-04-12 16:16:02,481\",\"logtype\":\"INFO\",\"loginfo\":\"org.apache.hadoop.hdfs.server.datanode.DataNode:SHUTDOWN_MSG: <@@@>/************************************************************<@@@>SHUTDOWN_MSG: Shutting down DataNode at sht-sgmhadoopdn-02/172.16.101.59<@@@>************************************************************/}";
        System.out.println(line);

        line=line.replaceAll("(\r\n|\r|\n|\n\r|\"|\b|\f|\\|/)", "")
                .replaceAll("(\t)", "    ")
                .replaceAll("(\\$)", "@");

        System.out.println(line.substring(0,line.length()-1)+"\"}");


    }

}
