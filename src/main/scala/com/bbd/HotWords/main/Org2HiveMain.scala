package com.bbd.HotWords.main

import com.bbd.HotWords.service.FromEs2HiveServices
import com.bbd.HotWords.tools.PropertiesTool
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import scala.collection.JavaConversions._

object Org2HiveMain {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      //.master("local[1]")
      .appName("OrgSpiderData2HiveApp")
      .config("spark.network.timeout","1200s")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._
    val during = PropertiesTool.getPropertyValue("streaming_During", "org_app_conf.properties").toInt
    val topic = PropertiesTool.getPropertyValue("topic_Name", "org_app_conf.properties")

    val ssc = new StreamingContext(sc,Seconds(during))

    val topics = Set[String](topic)
    val kafkaParams = PropertiesTool.getKafkaParamsMap("org_kafka_conf.properties").toMap

    val kafkaRdd = KafkaUtils.createDirectStream[String,String](ssc,PreferConsistent
      ,Subscribe[String,String](topics,kafkaParams))

    kafkaRdd.foreachRDD(rdd => {
      val saveRdd = rdd.map(x => x.value())
      val beanRdd = saveRdd.mapPartitions(itr => FromEs2HiveServices.fromkafka2Bean(itr).iterator)
      val beanDf = beanRdd.toDF()
      //beanDf.show(2)
      //println(beanDf.count())
      beanDf.createOrReplaceTempView("org_spider_info_tmp")



      val disDf = spark.sql("select distinct sourcetype,year,month,day from org_spider_info_tmp")
      val itrRdd = disDf.map(row => {
        row.getString(0)+","+row.getString(1)+","+row.getString(2)+","+row.getString(3)
      })
      val itrList = itrRdd.collectAsList().toList
      //遍历插入hive分区表中
      for(inStr <- itrList){
        val inArr = inStr.split(",")
        spark.sql("insert into zhanshen_i.info " +
          "partition(sourcetype='"+inArr(0)+"',year='"+inArr(1)+"',month='"+inArr(2)+"',day='"+inArr(3)+"') " +
          "select author,content,contentlength,contenttranslated,contenttranslatedlength,id,infoid,language,pubdate" +
          ",pubday,sourcedetail,sourceid,sourceurl,tasktokens,title,titletranslated,tokens,updatetime,uptime from org_spider_info_tmp t " +
          "where t.sourcetype='"+inArr(0)+"' and t.year='"+inArr(1)+"' and t.month='"+inArr(2)+"' and t.day='"+inArr(3)+"'")
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
