package com.bbd.HotWords.main

import java.security.PrivilegedAction
import java.util.Properties

import com.bbd.HotWords.tools.{PropertiesTool, SecurityTool}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{SaveMode, SparkSession}

object HiveSum2MysqlMain {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      //.master("local[1]")
      .appName("HiveSum2MysqlSparkApp")
      //.config("spark.network.timeout","1200s")
        .config("spark.sql.authorization.enabled",true)
        .config("hive.security.authorization.enabled",true)
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext

    //安全认证
    SecurityTool.securityPreUp()

    UserGroupInformation.getLoginUser.doAs(new PrivilegedAction[Void](){
      override def run(): Void = {
        val hiveDf = spark.sql("select t.name,count(1) as cou " +
          "from zhanshen_i.test2 t group by t.name")

        println(hiveDf.count())

        hiveDf.createOrReplaceTempView("test_spark_tmp")

        val hiveDf1 = spark.sql("select row_number() over(order by t.cou desc) as zeh_id" +
          ",t.name as zeh_token,t.cou as zeh_count" +
          " from test_spark_tmp t ")

        val mysqlPro = new Properties()
        mysqlPro.put("user",PropertiesTool.getPropertyValue("username", "jdbc_hotword.properties"))
        mysqlPro.put("password",PropertiesTool.getPropertyValue("password", "jdbc_hotword.properties"))
        mysqlPro.put("driver",PropertiesTool.getPropertyValue("driver", "jdbc_hotword.properties"))

        hiveDf1.show(4)

        hiveDf1.write.mode(SaveMode.Overwrite).jdbc(PropertiesTool.getPropertyValue("jdbc_url", "jdbc_hotword.properties")
          ,PropertiesTool.getPropertyValue("hot_wordtable", "jdbc_hotword.properties"),mysqlPro)

        null
      }
    })
//    hiveDf1.write.mode(SaveMode.Overwrite).jdbc(PropertiesTool.getPropertyValue("jdbc_url", "jdbc_hotword.properties")
//      ,PropertiesTool.getPropertyValue("hot_wordtable", "jdbc_hotword.properties"),mysqlPro)
  }
}
