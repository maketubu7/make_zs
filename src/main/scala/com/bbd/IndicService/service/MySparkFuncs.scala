package com.bbd.IndicService.service

import java.sql.Timestamp
import java.text.SimpleDateFormat


object MySparkFuncs {
  val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  def initUserFunctions(spark:org.apache.spark.sql.SparkSession):Unit={

    spark.udf.register("to_str_time",(dateStr:String)=>{
      var re:Timestamp = null
      try{
        re = new Timestamp(format.parse(dateStr).getTime)
      }catch {
        case e:Exception => Unit
      }
      re
    })

  }
}
