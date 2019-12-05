package com.bbd.HotWords.main

import com.bbd.HotWords.tools.{PropertiesTool, SecurityTool}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import sun.security.provider.ConfigFile

import scala.collection.JavaConversions._

class HotWordsMain {

}
object HotWordsMain {
  private val LOG = Logger.getLogger(classOf[HotWordsMain])

  def main(args: Array[String]): Unit = {
    LOG.info("0.1 ******************* " + System.getProperty("java.security.auth.login.config"))
    //安全认证
    SecurityTool.securityPreUp()
    LOG.info("0.2 ******************* " + System.getProperty("java.security.auth.login.config"))

    val spark = SparkSession.builder()
//      .master("local[1]")
      .appName("Kfk2HiveSparkApp")
      .config("spark.network.timeout","1200s")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
//    sc.setLogLevel("WARN")

    import spark.implicits._
    val during = PropertiesTool.getPropertyValue("streaming_during", "word_app_conf.properties").toInt
    val topic = PropertiesTool.getPropertyValue("topic_name", "word_app_conf.properties")
    val hiveTable = PropertiesTool.getPropertyValue("hivetable_name", "word_app_conf.properties")
    LOG.info("1 ******************* " + System.getProperty("java.security.auth.login.config"))
    val ssc = new StreamingContext(sc,Seconds(during))

    val topics = Set[String](topic)
    val kafkaParams = PropertiesTool.getKafkaParamsMap("word_kafka_conf.properties").toMap
    LOG.info("2 ******************* " + System.getProperty("java.security.auth.login.config"))

    javax.security.auth.login.Configuration.setConfiguration(new ConfigFile)
    val kafkaRdd = KafkaUtils.createDirectStream[String,String](ssc,PreferConsistent
      ,Subscribe[String,String](topics,kafkaParams))

    LOG.info("3 ******************* " + System.getProperty("java.security.auth.login.config"))

    kafkaRdd.foreachRDD(rdd=>{
      val saveRdd = rdd.map(x => x.value())
      val beanDf = saveRdd.toDF("name")
      println("======================= dataframe counts : " + beanDf.count())
      beanDf.show()
      beanDf.createOrReplaceTempView("hive_table_tmp")
      spark.sql("insert into " + hiveTable +
          " select name from hive_table_tmp")

      //完成后手工保存offset
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      kafkaRdd.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })

    //启动流
    ssc.start()
    ssc.awaitTermination()
  }

}
