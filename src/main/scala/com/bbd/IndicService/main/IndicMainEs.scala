package com.bbd.IndicService.main

import com.bbd.IndicService.service.MySparkFuncs
import org.apache.spark.sql.SparkSession

object IndicMainEs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("IndicServiceApp")
      .config("spark.network.timeout","1200s")
      .getOrCreate()

    val sc = spark.sparkContext
    MySparkFuncs.initUserFunctions(spark)

    val esconfMap = Map("path" -> "qmw_test/info",
    "pushdown" -> "true",
    "es.read.field.as.array.include"->"myarr",
    "es.nodes"->"galaxy04,galaxy05,galaxy06",
      "es.mapping.date.rich"->"false",
    "es.port" -> "9200")
    val esDf = spark.sqlContext.read.format("org.elasticsearch.spark.sql").options(esconfMap).load("qmw_test/info")
    esDf.createOrReplaceTempView("es_table")
    val df1 = spark.sql("select name,pubdate,myarr,getUnionScore(10.2,20,34) from es_table")
    df1.printSchema()
    df1.show(10)
    /*df1.rdd.foreach(row => {
      println("---------"+row.getString(1))

    })*/
    /*esDf.foreach(row => {
      val seq:java.util.List[GenericRowWithSchema] = row.getList[GenericRowWithSchema](0)
      if(seq != null){
        for(i <- 0 until seq.size()){
          print(seq.get(i).getAs[String]("type")+"-得分-"+seq.get(i).getAs[Double]("score"))
        }
      }

    })*/

   /* val inner = InnerType("esaaa")
    val inne1 = InnerType("esaaa1")
    val ilist = ListBuffer[InnerType]()
    ilist.append(inner)
    ilist.append(inne1)
    val outer = OuterType("445","es1","asas1",34l,ilist)
    val outer1 = OuterType1("445",ilist)
    val slist:ListBuffer[OuterType1] = ListBuffer()
    slist.append(outer1)
    import spark.implicits._
    val rdd = sc.parallelize(slist)
    /*val df = rdd.toDF()
    df.printSchema()*/

    /*EsSparkSQL.saveToEs(esDf,"qmw_test/info",Map("es.mapping.id"->"esid"
      ,"es.nodes"->"galaxy04,galaxy05,galaxy06",
      "es.port" -> "9200"))*/

    EsSpark.saveToEs(rdd,"qmw_test/info",Map("es.mapping.id"->"esid"
      ,"es.nodes"->"galaxy04,galaxy05,galaxy06"
      ,"es.port" -> "9200"
      ,"es.write.operation" -> "upsert"))*/



  }
}
