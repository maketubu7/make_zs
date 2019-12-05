package com.bbd.IndicService.bean


import scala.collection.mutable.ListBuffer

object ComCaseClass {
  case class IndicBean(`type`:String,`score`:Double)
  case class InnerType(`type`:String)
  case class OuterType(esid:String,name:String,addr:String,age:Long,myarr:ListBuffer[InnerType])
  case class OuterType1(esid:String,myarr:ListBuffer[InnerType])

  /**
    * ES  相关案例类
    */
  case class AlertEventIndicationRule(id:String,indicationId:String,alertEventId:String,name:String
                                      ,desc:String,baseScore:Long,effectDelayDays:Long,score:Double
                                      ,keyWords:ListBuffer[String],eventLable:String)
  case class AlertEventIndication(id:String,alertEventId:String,name:String
                                  ,desc:String,baseScoreWeight:Long,score:Double,scoreRuleId:String
                                  ,rules:ListBuffer[AlertEventIndicationRule])
  case class AlertEventBase(eventId:String, pubDate:String, indications:ListBuffer[AlertEventIndication])

  case class AlertEvent(id:String,dampingDays:Long)

  case class AlertEventElementRule(alertEventId:String,elementRule:String,baseScore:Long,weight:Double)
}
