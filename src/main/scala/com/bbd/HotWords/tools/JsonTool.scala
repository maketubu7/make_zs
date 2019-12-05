package com.bbd.HotWords.tools

import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer

object JsonTool {
  case class Inner(cleandText:String,codelevel1:Integer,codelevel2:Integer,elementid:String,frequency:Integer,infoid:String
                   ,text:String,type1:String)
 def getSeqByStr(inStr:String):Seq[Inner]={
   val list = ListBuffer[Inner]()
   try{
     val jsonArray = JSON.parseArray(inStr)
     for(i <- 0 until jsonArray.size()){
       val jsonObject = jsonArray.getJSONObject(i)
       val cleandText = jsonObject.getString("cleandText")
       var codeLevel1 = jsonObject.getInteger("codeLevel1")
       /*if(codeLevel1 == null){
         codeLevel1 = 0
       }*/
       var codeLevel2 = jsonObject.getInteger("codeLevel2")
       /*if(codeLevel2 == null){
         codeLevel2 = 0
       }*/

       val elementId = jsonObject.getString("elementId")
       var frequency = jsonObject.getInteger("frequency")
       /*if(frequency == null){
         frequency = 0
       }*/

       val infoId = jsonObject.getString("infoId")
       val text = jsonObject.getString("text")
       val type1 = jsonObject.getString("type")

       list.append(Inner(cleandText,codeLevel1,codeLevel2,elementId,frequency,infoId,text,type1))
       /*val schema = StructType(Array(StructField("cleandText",StringType,nullable = true)
         ,StructField("codelevel1",IntegerType,nullable = true)
       ,StructField("codelevel2",IntegerType,nullable = true)
       ,StructField("elementid",StringType,nullable = true)
       ,StructField("frequency",IntegerType,nullable = true)
       ,StructField("infoid",StringType,nullable = true)
       ,StructField("text",StringType,nullable = true)
         ,StructField("type1",StringType,nullable = true)))*/
     }


   }catch {
     case e:Exception => {}
   }
   return list
 }
}
