package com.bbd.IndicService.main

import com.bbd.IndicService.bean.ComCaseClass
import com.bbd.IndicService.service.{EsService, JdbcService, MySparkFuncs, RddService}
import com.bbd.IndicService.tool.DateUtils
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

class IndicEsMain{}
object IndicEsMain {
  val logger = Logger.getLogger(classOf[IndicEsMain])

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      //.master("local[1]")
      .appName("IndicEsServiceApp")
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
    }
    else{
      throw new Exception("参数格式错误")
    }

    val sc = spark.sparkContext
    import org.elasticsearch.spark._
    import spark.implicits._
    val idxMap = EsService.getEsReadEventMap()

    while(dateStr.compareTo(endDateStr)<=0){
      /**
        * 按天计算分值
        */
      logger.info("开始执行："+dateStr+" 的任务")
      val starttime = System.currentTimeMillis()
      val oneYearBefore = DateUtils.getOneYearBeforeStr(dateStr)

      //测试直接读取rdd
      val eventDfNeed = sc.esRDD(EsService.path,"{\"query\": { " +
        "   \"range\": {" +
        "      \"pubDate\": {" +
        "        \"gt\": \""+oneYearBefore+"\"," +
        "        \"lte\": \""+dateStr+"\"" +
        "      }" +
        "    }" +
        "  }}",idxMap)
      println("eventDfNeed counts:"+eventDfNeed.count())
      val eventDfNeedRdd: RDD[ComCaseClass.AlertEventBase] = eventDfNeed.map(row => RddService.mapIndications(row))
      println("eventDfNeedRdd counts:"+eventDfNeedRdd.count())

      //获取mysql当中的所有配置表
      val alertEvents = JdbcService.getAlertEvent()
      val alertEventIndicationRules = JdbcService.getAlertEventIndicationRule()
      val alertEventElementRules = JdbcService.getAlertEventElementRule()

      //注册alert_event 表，注册成临时表
      val eventAlertRdd = sc.parallelize(alertEvents.toList).map(x => x._2)
      eventAlertRdd.toDF().createOrReplaceTempView("alert_event_tab")


      //计算衰减分
      val newEventDfNeedRdd = eventDfNeedRdd.mapPartitions(itr =>
        RddService.mapDecayScores(itr,alertEvents,alertEventIndicationRules,alertEventElementRules,dateStr).toIterator)

      //将新计算的衰减分存入ES
      EsService.saveIndicIndex(newEventDfNeedRdd)

      //计算合并分，并存入mysql 表中
      val scoreRdd = newEventDfNeedRdd.flatMap(event =>  RddService.flatMapScoreRdd(event))
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
          | to_str_time('""".stripMargin+dateStr+"""') AS day,
                                                 | NVL(t1.raw_score,0) AS raw_score,
                                                 | NVL(t1.score,0.0) AS score
                                                 |FROM alert_event_tab t
                                                 |LEFT JOIN base_score_tab_mid t1
                                                 |ON t.id = t1.alert_event_id
                                                 """.stripMargin)

      scoreDf1.show(100)

      scoreDf1.createOrReplaceTempView("base_score_tab_tmp")
      //将本次结果插入到mysql结果表中，防止Smax < S
      JdbcService.deleteAlertEventDailyScoreByDay(dateStr)
      scoreDf1.write.mode(SaveMode.Append).jdbc(JdbcService.url,JdbcService.alertEventDailyScore,JdbcService.getProperties)

      //读取历史mysql表数据
      val hisScoreDf = spark.read.jdbc(JdbcService.url,JdbcService.alertEventDailyScore,Array(" day>str_to_date('"
        + oneYearBefore +"','%Y-%m-%d %H:%i:%s') and day <= str_to_date('"+dateStr+"','%Y-%m-%d %H:%i:%s')")
        ,JdbcService.getProperties)
      //重新分区

      hisScoreDf.createOrReplaceTempView("his_score_tab")

      val df1 = spark.sql(
        """
          |SELECT
          | alert_event_id,
          | count(1) AS cou,
          | max(raw_score) as smax,
          | (CASE WHEN ROUND(count(1)*0.2) <=0 THEN 1 ELSE ROUND(count(1)*0.2) END) as num_s1,
          | (CASE WHEN ROUND(count(1)*0.05) <=0 THEN 1 ELSE ROUND(count(1)*0.05) END) as num_s2
          |FROM his_score_tab
          |GROUP BY alert_event_id
        """.stripMargin)
//      df1.show(100)
      df1.createOrReplaceTempView("his_score_tab_tmp1")

      val df2 = spark.sql(
        """
          |SELECT
          | alert_event_id,
          | raw_score,
          | row_number() over(partition by alert_event_id order by raw_score desc) as rownum
          |FROM his_score_tab
        """.stripMargin)
//      df2.show(100)
      df2.createOrReplaceTempView("his_score_tab_tmp2")

      val df3 = spark.sql(
        """
          |SELECT
          | t.alert_event_id,
          | t.smax,
          | t1.raw_score as s1,
          | t2.raw_score as s2
          |FROM his_score_tab_tmp1 t,
          |     his_score_tab_tmp2 t1,
          |     his_score_tab_tmp2 t2
          |WHERE t.alert_event_id = t1.alert_event_id
          |AND t.num_s1 = t1.rownum
          |AND t.alert_event_id = t2.alert_event_id
          |AND t.num_s2 = t2.rownum
        """.stripMargin)
      df3.show(100)
      df3.createOrReplaceTempView("his_score_tab_tmp3")

      val saveDf = spark.sql(
        """
          |SELECT
          |  t.alert_event_id,
          |  t.day,
          |  t.raw_score,
          |  CAST(ROUND(NVL((CASE WHEN (CASE WHEN t.raw_score >= 0 AND t.raw_score <= t1.s1
          |                     THEN t.raw_score/t1.s1*60
          |                   WHEN t.raw_score > t1.s1 AND t.raw_score <=t1.s2
          |                     THEN (t.raw_score-t1.s1)/(t1.s2-t1.s1)*20+60
          |                   ELSE (t.raw_score-t1.s2)/(t1.smax-t1.s2)*20+80
          |              END) > 100
          |        THEN 100
          |        ELSE (CASE WHEN t.raw_score >= 0 AND t.raw_score <= t1.s1
          |                     THEN t.raw_score/t1.s1*60
          |                   WHEN t.raw_score > t1.s1 AND t.raw_score <=t1.s2
          |                     THEN (t.raw_score-t1.s1)/(t1.s2-t1.s1)*20+60
          |                   ELSE (t.raw_score-t1.s2)/(t1.smax-t1.s2)*20+80
          |              END)
          |   END),0)) AS INTEGER) AS score
          |FROM base_score_tab_tmp t,
          |     his_score_tab_tmp3 t1
          |WHERE t.alert_event_id = t1.alert_event_id
        """.stripMargin)
      saveDf.show(100)

      //先删除，再插入都是操作同一张表的时候，由于rdd的懒加载，会产生不可预估的结果，所以需要更新分数
      //saveDf.show(100)
      saveDf.foreachPartition(itr => JdbcService.updateScoreTable(itr))
      val endTime = System.currentTimeMillis()
      logger.info(dateStr+"执行的时间是："+(endTime-starttime)/1000 + " 秒")
      //日期加一天
      dateStr = DateUtils.addOneDayAddStr(dateStr)
    }
  }
}
