package com.bbd.HotWords.main

import com.bbd.HotWords.service.FromEs2HiveServices
import com.bbd.HotWords.tools.PropertiesTool
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._

class HotWordsMainBack {
}
object HotWordsMainBack {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      //.master("local[1]")
      .appName("Kfk2HiveSparkApp")
      .config("spark.network.timeout","1200s")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._
    val during = PropertiesTool.getPropertyValue("streaming_during", "word_app_conf.properties").toInt
    val topic = PropertiesTool.getPropertyValue("topic_name", "word_app_conf.properties")

    val ssc = new StreamingContext(sc,Seconds(during))

    val topics = Set[String](topic)
    val kafkaParams = PropertiesTool.getKafkaParamsMap("word_kafka_conf.properties").toMap

    val kafkaRdd = KafkaUtils.createDirectStream[String,String](ssc,PreferConsistent
      ,Subscribe[String,String](topics,kafkaParams))


    kafkaRdd.foreachRDD(rdd=>{
      val saveRdd = rdd.map(x => x.value())
      //分区进行关键词划分
      val beanRdd = saveRdd.mapPartitions(itr => FromEs2HiveServices.fromEs2Bean(itr).iterator)
      // struct (year, month, day, timestamp, word)
      val beanDf = beanRdd.toDF()
      beanDf.createOrReplaceTempView("hive_table_tmp")

      val disDf = spark.sql("select distinct year,month,day from hive_table_tmp where length(word)>1")
      val itrRdd = disDf.map(row => {
        row.getString(0)+","+row.getString(1)+","+row.getString(2)
      })
      val itrList = itrRdd.collectAsList().toList
      //遍历插入hive分区表中
      for(inStr <- itrList){
        val inArr = inStr.split(",")
        spark.sql("insert into zhanshen_i.es_hotword " +
          "partition(year='"+inArr(0)+"',month='"+inArr(1)+"',day='"+inArr(2)+"') " +
          "select time,word from hive_table_tmp t " +
          "where length(t.word)>1 and t.year='"+inArr(0)+"' and t.month='"+inArr(1)+"' and t.day='"+inArr(2)+"'")
      }


      //完成后手工保存offset
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      kafkaRdd.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })

    //启动流
    ssc.start()
    ssc.awaitTermination()
  }
}
