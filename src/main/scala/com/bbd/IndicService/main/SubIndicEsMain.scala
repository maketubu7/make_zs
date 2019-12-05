package com.bbd.IndicService.main

import com.bbd.IndicService.service.{EsService, JdbcService, MySparkFuncs, RddService}
import com.bbd.IndicService.tool.DateUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}
class SubIndicEsMain{}
object SubIndicEsMain {
  val logger = Logger.getLogger(classOf[SubIndicEsMain])

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      //.master("local[1]")
      .appName("SubIndicEsServiceApp")
      .config("spark.network.timeout","1200s")
      .getOrCreate()

    //注册自定义函数
    MySparkFuncs.initUserFunctions(spark)

    var dateStr:String = null
    var endDateStr:String = null

    if(args.length>2){
      throw new Exception("最多只能传入两个参数")
    }else if(args.length==0){
      dateStr = DateUtils.getNowDateStr
      endDateStr = dateStr
    }else if(args.length==1 && DateUtils.isDateStr(args(0))){
      val str = args(0)
      if(str.length == 10){
        dateStr = str + " 00:00:00"
        endDateStr = str + " 00:00:00"
      }else if(str.length == 19){
        dateStr = str
        endDateStr = str
      }
    }else if(args.length==2 && DateUtils.isDateStr(args(0)) && DateUtils.isDateStr(args(1))
      && args(1).compareTo(args(0))>0){
      val str = args(0)
      val str1 = args(1)
      if(str.length == 10){
        dateStr = str + " 00:00:00"
      }else if(str.length == 19){
        dateStr = str
      }
      if(str1.length == 10){
        endDateStr = str1 + " 00:00:00"
      }else if(str1.length == 19){
        endDateStr = str1
      }
    }else{
      throw new Exception("参数格式错误")
    }

    val sc = spark.sparkContext
    import spark.implicits._
    import org.elasticsearch.spark._
    val idxMap = EsService.getEsReadEventMap()

    while(dateStr.compareTo(endDateStr)<=0) {
      /**
        * 按天计算分值
        */
      logger.info("开始执行：" + dateStr + " 的任务")
      val starttime = System.currentTimeMillis()
      val oneYearBefore = DateUtils.getOneYearBeforeStr(dateStr)

      //测试直接读取rdd
      val eventDfNeed = sc.esRDD(EsService.path, "{\"query\": { " +
        "   \"range\": {" +
        "      \"pubDate\": {" +
        "        \"gt\": \"" + oneYearBefore + "\"," +
        "        \"lte\": \"" + dateStr + "\"" +
        "      }" +
        "    }" +
        "  }}", idxMap)
      println("eventDfNeed counts:" + eventDfNeed.count())
      val eventDfNeedRdd = eventDfNeed.map(row => RddService.mapIndications(row))
      println("eventDfNeedRdd counts:" + eventDfNeedRdd.count())

      //获取mysql当中的所有配置表
      val alertEvents = JdbcService.getAlertEvent()
      val alertEventIndicationRules = JdbcService.getAlertEventIndicationRule()
      val alertEventElementRules = JdbcService.getAlertEventElementRule()

      //注册alert_event 表，注册成临时表
      val eventAlertRdd = sc.parallelize(alertEvents.toList).map(x => x._2)
      eventAlertRdd.toDF().createOrReplaceTempView("alert_event_tab")


      //计算衰减分
      val newEventDfNeedRdd = eventDfNeedRdd.mapPartitions(itr =>
        RddService.mapDecayScores(itr, alertEvents, alertEventIndicationRules, alertEventElementRules, dateStr).toIterator)

      //将新计算的衰减分存入ES
      EsService.saveIndicIndex(newEventDfNeedRdd)

      //计算合并分，并存入mysql 表中
      val scoreRdd = newEventDfNeedRdd.flatMap(event => RddService.flatMapScoreRdd(event))
      val scoreDf = scoreRdd.toDF()
      scoreDf.createOrReplaceTempView("base_score_tab")
      spark.sql(
        """
          |SELECT
          | alertEventId AS alert_event_id,
          | sum(score) AS raw_score,
          | 0.0 AS score
          |FROM base_score_tab
          |GROUP BY alertEventId
        """.stripMargin).createOrReplaceTempView("base_score_tab_mid")

      val scoreDf1 = spark.sql(
        """
          |SELECT
          | t.id as alert_event_id,
          | to_str_time('""".stripMargin + dateStr +
          """') AS day,
                                                   | NVL(t1.raw_score,0) AS raw_score,
                                                   | NVL(t1.score,0.0) AS score
                                                   |FROM alert_event_tab t
                                                   |LEFT JOIN base_score_tab_mid t1
                                                   |ON t.id = t1.alert_event_id
                                                 """.stripMargin)
      scoreDf1.show(100)

      //将本次结果插入到mysql结果表中，防止Smax < S
      JdbcService.deleteAlertEventDailyScoreByDay(dateStr)
      scoreDf1.write.mode(SaveMode.Append).jdbc(JdbcService.url,JdbcService.alertEventDailyScore,JdbcService.getProperties)

      val endTime = System.currentTimeMillis()
      logger.info(dateStr+"执行的时间是："+(endTime-starttime)/1000 + " 秒")
      //日期加一天
      dateStr = DateUtils.addOneDayAddStr(dateStr)
  }
 }
}
