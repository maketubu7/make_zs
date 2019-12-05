package com.bbd.HotWords.main

import java.io.File

import com.bbd.HotWords.beans.Rows
import com.bbd.HotWords.service.FromEs2HiveServices
import com.bbd.HotWords.tools.TimeTool
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.{Row, SparkSession}
//import org.apache.hadoop.hive.ql.metadata.Hive
//import io.netty.bootstrap.Bootstrap
//import org.apache.lucene.util.SetOnce
//import org.apache.spark.streaming.kafka010.KafkaUtils
//import org.apache.lucene.util.SetOnce
//import org.apache.spark.sql.catalyst.encoders.RowEncoder
//import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
//import org.elasticsearch.action.TransportActionNodeProxy

object Test {
  def main(args: Array[String]): Unit = {
    //println(System.getProperty("java.class.path"))
    /*val sparkSession = SparkSession.builder()
      //.master("local[1]")
      .appName("HiveSparkTest")
      .enableHiveSupport().getOrCreate()

    //sparkSession.sql("insert into qmw_test1 values('~~~~~~')").printSchema()*/

   /* val relist = FromEs2HiveServices.fromEs2Bean(List("{\"query\":\"我们伟大的祖国永远强大\",\"time\":\"2018-05-08 16:21:14\"}"
      ,"{\"query\":\"我不知道你在说什么\",\"time\":\"2018-05-08 16:21:14\"}").iterator)

    for(pers<-relist){
      print(pers.year+pers.month+pers.day+":"+pers.word)
    }*/

    val time = TimeTool.getSqlTimeByString("2018-03-14 15:55:49")
    println(time.toString)
    println(TimeTool.isFromatStr("2018-03-99 44:55:49"))

   /* val arr = TimeTool.getYmddel1Arr()
    for(str <- arr){
      print(str+"---")
    }*/





  }
}
