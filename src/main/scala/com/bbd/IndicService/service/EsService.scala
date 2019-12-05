package com.bbd.IndicService.service

import com.bbd.IndicService.bean.ComCaseClass.AlertEventBase
import com.bbd.IndicService.tool.PropertiesTool
import org.apache.spark.rdd.RDD
import org.elasticsearch.spark.rdd.EsSpark

object EsService {
  val path = PropertiesTool.getPropertyValue("idx_event_path","es.properties")
  val pushdown = PropertiesTool.getPropertyValue("idx_event_pushdown","es.properties")
  val include = PropertiesTool.getPropertyValue("idx_event_array_include","es.properties")
  val nodes = PropertiesTool.getPropertyValue("idx_nodes","es.properties")
  val port = PropertiesTool.getPropertyValue("idx_port","es.properties")

  def saveIndicIndex(rdd:RDD[AlertEventBase]):Unit={
    EsSpark.saveToEs(rdd,path,Map("es.mapping.id"->"eventId"
      ,"es.nodes"-> nodes
      ,"es.port" -> port
      ,"es.write.operation" -> "upsert"))
  }

  def getEsReadEventMap():Map[String,String]={
    Map("pushdown" -> pushdown
      ,"es.nodes" -> nodes
      ,"es.port"->port
      ,"es.mapping.date.rich"->"false")
  }
}
