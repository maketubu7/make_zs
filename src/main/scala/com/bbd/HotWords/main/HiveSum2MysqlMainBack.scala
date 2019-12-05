package com.bbd.HotWords.main

import java.util.Properties

import com.bbd.HotWords.tools.PropertiesTool
import org.apache.spark.sql.{SaveMode, SparkSession}

object HiveSum2MysqlMainBack {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      //.master("local[1]")
      .appName("HiveSum2MysqlSparkApp")
      //.config("spark.network.timeout","1200s")
      .enableHiveSupport()
      .getOrCreate()

    val hiveDf = spark.sql("select t.word,count(1) as cou " +
      "from bbd.zhanshen_i_es_hotword t where length(t.word)>1 group by t.word order by cou desc limit 100")

    //println(hiveDf.count())

    hiveDf.createOrReplaceTempView("es_hotword_spark_tmp")

    val hiveDf1 = spark.sql("select row_number() over(order by t.cou desc) as zeh_id" +
      ",t.word as zeh_token,t.cou as zeh_count" +
      " from es_hotword_spark_tmp t ")

    val mysqlPro = new Properties()
    mysqlPro.put("user",PropertiesTool.getPropertyValue("username", "jdbc_hotword.properties"))
    mysqlPro.put("password",PropertiesTool.getPropertyValue("password", "jdbc_hotword.properties"))
    mysqlPro.put("driver",PropertiesTool.getPropertyValue("driver", "jdbc_hotword.properties"))

    hiveDf1.write.mode(SaveMode.Overwrite).jdbc(PropertiesTool.getPropertyValue("jdbc_url", "jdbc_hotword.properties")
      ,PropertiesTool.getPropertyValue("hot_wordtable", "jdbc_hotword.properties"),mysqlPro)
  }
}
