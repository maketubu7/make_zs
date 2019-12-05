package com.bbd.IndicService.main

import com.bbd.IndicService.service.HbaseService
import com.bbd.IndicService.tool.PropertiesTool
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.sql.SparkSession

class IndicMain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("IndicServiceApp")
      .config("spark.network.timeout","1200s")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext

    val conf = HbaseService.getHbaseConf()
    conf.set(TableInputFormat.INPUT_TABLE,PropertiesTool.getPropertyValue("event_tabname","hbase.properties"))
    HbaseService.setScan(conf,null,null,Array[String]("cf"),Array[String]("cf:","cf:","cf:"))

    val hbaseRdd = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[ImmutableBytesWritable]
      ,classOf[Result])
//    hbaseRdd.mapPartitions(itr => RddService.getNeedObjects(itr).toIterator)
  }
}
