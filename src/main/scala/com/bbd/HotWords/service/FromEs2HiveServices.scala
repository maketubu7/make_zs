package com.bbd.HotWords.service

import java.net.InetAddress

import com.alibaba.fastjson.JSON
import com.bbd.HotWords.service.FromEs2HiveServices.client
import com.bbd.HotWords.tools.{JsonTool, PropertiesTool, TimeTool}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.elasticsearch.action.admin.indices.analyze.{AnalyzeAction, AnalyzeRequestBuilder}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient

import scala.collection.JavaConversions._
class FromEs2HiveServices{}
object FromEs2HiveServices {
  val logger = Logger.getLogger(classOf[FromEs2HiveServices])
  case class HotWord(year:String,month:String,day:String,time:java.sql.Timestamp,word:String)
  case class info(author:String,content:String,contentlength:Integer,contenttranslated:String,contenttranslatedlength:Integer
                  ,id:String,infoid:String,language:String,pubdate:java.sql.Timestamp,pubday:java.sql.Date,sourcedetail:String
                  ,sourceid:String,sourcetype:String,sourceurl:String,tasktokens:String,title:String
                  ,titletranslated:String,tokens:Seq[JsonTool.Inner],updatetime:java.sql.Timestamp,uptime:java.sql.Timestamp
                  ,year:String,month:String,day:String)

  val settings = Settings.builder().put("cluster.name", PropertiesTool.getPropertyValue("cluster.name", "es_conf.properties"))
    //.put("xpack.security.user", PropertiesTool.getPropertyValue("xpack.security.user", "es_conf.properties"))
    .put("client.transport.sniff", true)
    .build()

  val client:TransportClient = new PreBuiltTransportClient(settings)
  setClient(client)

  def setClient(client:TransportClient):Unit={
    val hostArr = PropertiesTool.getPropertyValue("inetAddress", "es_conf.properties").split(",")
    val port = PropertiesTool.getPropertyValue("port", "es_conf.properties").toInt
    for(name  <- hostArr){
      client.addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName(name), port))
    }
  }

  def fromEs2Bean(itr:Iterator[String]):scala.collection.mutable.ListBuffer[HotWord]={
    //创建ES链接
    val reList:scala.collection.mutable.ListBuffer[HotWord] = scala.collection.mutable.ListBuffer[HotWord]()


      itr.foreach(line => {

        //某一条记录出错，，记录错误而不影响整体执行
        try{
          val jsonObject = JSON.parseObject(line)
          val queryStr = jsonObject.getString("query")
          val time = jsonObject.getString("time")
          var year:String = null
          var month:String = null
          var day:String = null
          if(time != null && !"".equals(time) && TimeTool.isFromatStr(time)) {
            year = time.substring(0,4)
            month = time.substring(5,7)
            day = time.substring(8,10)

            val ikRequest = new AnalyzeRequestBuilder(client, AnalyzeAction.INSTANCE, "idx_zhansheni201810",
              queryStr)

            ikRequest.setTokenizer("ik_max_word")
            val tokenList = ikRequest.execute().actionGet().getTokens().toList
            for(token <- tokenList){
              val word = token.getTerm
              reList.append(HotWord(year,month,day,TimeTool.getSqlTimeByString(time),word))
            }
          }else{
            logger.error("出现无法通过time进行hive分区或者解析错误的json字符串："+line)
          }
        }catch {
          case e:Exception => {logger.error("出现无法通过time进行hive分区或者解析错误的json字符串："+line)}
        }
      })

    return reList
  }

  def fromkafka2Bean(itr:Iterator[String]):scala.collection.mutable.ListBuffer[info]={
    val reList:scala.collection.mutable.ListBuffer[info] = scala.collection.mutable.ListBuffer[info]()

      itr.foreach(line =>{

        try{
          val jsonObject = JSON.parseObject(line)
          val author = jsonObject.getString("author")
          val content = jsonObject.getString("content")
          var contentLength = jsonObject.getInteger("contentLength")
          /*if(contentLength == null){
            contentLength =  0;
          }*/
          val contentTranslated = jsonObject.getString("contentTranslated")
          var contentTranslatedLength = jsonObject.getInteger("contentTranslatedLength")
         /* if(contentTranslatedLength == null){
            contentTranslatedLength = 0;
          }*/

          val id = jsonObject.getString("id")
          val infoId = jsonObject.getString("infoId")
          val language = jsonObject.getString("language")
          val pubDate = TimeTool.getRealDateStr(jsonObject.getString("pubDate"))
          val pubDay = TimeTool.getRealDateStrDate(jsonObject.getString("pubDay"))
          val sourceDetail = jsonObject.getString("sourceDetail")
          val sourceId = jsonObject.getString("sourceId")
          val sourceType = jsonObject.getString("sourceType")
          val sourceUrl = jsonObject.getString("sourceUrl")
          val taskTokens = jsonObject.getString("taskTokens")
          val title = jsonObject.getString("title")
          val titleTranslated = jsonObject.getString("titleTranslated")

          val tokens = JsonTool.getSeqByStr(jsonObject.getString("tokens"))

          val updateTime = TimeTool.getRealDateStr(jsonObject.getString("updateTime"))
          val uptime = TimeTool.getRealDateStr(jsonObject.getString("uptime"))
          var year:String = null
          var month:String = null
          var day:String = null
          if(pubDate != null && !"".equals(pubDate) && TimeTool.isFromatStr(pubDate)){

            year = pubDate.substring(0,4)
            month = pubDate.substring(5,7)
            day = pubDate.substring(8,10)

            reList.append(info(author,content,contentLength,contentTranslated,contentTranslatedLength,id,infoId,language
              ,TimeTool.getSqlTimeByString(pubDate),TimeTool.getSqlDateByString(pubDay),sourceDetail,sourceId,sourceType,sourceUrl,taskTokens
              ,title,titleTranslated,tokens,TimeTool.getSqlTimeByString(updateTime),TimeTool.getSqlTimeByString(uptime)
              ,year,month,day))
          }else{
            logger.error("出现无法通过pubDate进行hive分区或者解析错误的json字符串："+line)
          }
        }catch {
          case e:Exception => {logger.error("出现无法通过pubDate进行hive分区或者解析错误的json字符串："+line)}
        }
      })

    return reList
  }

}
