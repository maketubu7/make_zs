package com.bbd.IndicService.service

import com.alibaba.fastjson.JSON
import com.bbd.IndicService.bean.ComCaseClass._
import com.bbd.IndicService.tool.DateUtils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
object RddService {

  def mapIndications(row:(String,scala.collection.Map[String,AnyRef])):AlertEventBase={
    val indexMap = row._2
    val eventId:String = indexMap.getOrElse("eventId",null).asInstanceOf[String]
    val pubDate:String = indexMap.getOrElse("pubDate",null).asInstanceOf[String]

    val indicationBuffer:mutable.Buffer[mutable.LinkedHashMap[String,AnyRef]] = indexMap.getOrElse("indications",null)
      .asInstanceOf[mutable.Buffer[mutable.LinkedHashMap[String,AnyRef]]]

    //遍历迹象列表
    val indicList = ListBuffer[AlertEventIndication]()
    if(indicationBuffer != null && indicationBuffer.size > 0){
      for(map <-indicationBuffer){
        if(map != null && map.size>0){
          val indicId:String = map.getOrElse("id",null).asInstanceOf[String]
          val indicAlertEventId:String = map.getOrElse("alertEventId",null).asInstanceOf[String]
          val indicName:String = map.getOrElse("name",null).asInstanceOf[String]
          val indicDesc:String = map.getOrElse("desc",null).asInstanceOf[String]
          val indicBaseScoreWeight:Long = map.getOrElse("baseScoreWeight",0l).asInstanceOf[Long]
          val indicScoreRuleId:String = map.getOrElse("scoreRuleId",null).asInstanceOf[String]
          val indicRulesBuffer:mutable.Buffer[mutable.LinkedHashMap[String,AnyRef]] = map.getOrElse("rules",null)
            .asInstanceOf[mutable.Buffer[mutable.LinkedHashMap[String,AnyRef]]]

          //遍历迹象规则
          val ruleList = ListBuffer[AlertEventIndicationRule]()
          var indicScore:Double = 0d
          if(indicRulesBuffer!=null && indicRulesBuffer.size>0){
            for(rule<-indicRulesBuffer){
              if(rule != null && rule.size>0){
                val ruleId:String = rule.getOrElse("id",null).asInstanceOf[String]
                val ruleIndicationId:String = rule.getOrElse("indicationId",null).asInstanceOf[String]
                val ruleAlertEventId:String = rule.getOrElse("alertEventId",null).asInstanceOf[String]
                val ruleName:String = rule.getOrElse("name",null).asInstanceOf[String]
                val ruleDesc:String = rule.getOrElse("desc",null).asInstanceOf[String]
                val ruleBaseScore:Long = rule.getOrElse("baseScore",0l).asInstanceOf[Long]
                val ruleEffectDelayDays:Long = rule.getOrElse("effectDelayDays",0l).asInstanceOf[Long]
                val ruleScore:Double = rule.getOrElse("score",0d).asInstanceOf[Float].toDouble
                val ruleEventLable:String = rule.getOrElse("eventLable",null).asInstanceOf[String]
                val ruleKeyWordsBuffer:mutable.Buffer[String] = rule.getOrElse("keyWords",null).asInstanceOf[mutable.Buffer[String]]
                //以规则列表中的分数最大值作为迹象的分值
                if(ruleScore>indicScore){indicScore = ruleScore}

                //遍历关键词列表
                val keywordList = ListBuffer[String]()
                if(ruleKeyWordsBuffer != null && ruleKeyWordsBuffer.size>0){
                  for(keyword<-ruleKeyWordsBuffer){
                    keywordList.append(keyword)
                  }
                }
                val irule = AlertEventIndicationRule(ruleId,ruleIndicationId,ruleAlertEventId,ruleName,ruleDesc,ruleBaseScore
                  ,ruleEffectDelayDays,ruleScore,keywordList,ruleEventLable)
                ruleList.append(irule)
              }
            }
          }

          val iIndic = AlertEventIndication(indicId,indicAlertEventId,indicName,indicDesc,indicBaseScoreWeight,indicScore
            ,indicScoreRuleId,ruleList)
          indicList.append(iIndic)
        }
      }
    }
    val base = AlertEventBase(eventId,pubDate,indicList)
    base
  }

  def mapDecayScores(itr:Iterator[AlertEventBase], alertEvents: mutable.HashMap[String,AlertEvent],
                     alertEventIndicationRules: mutable.HashMap[String,AlertEventIndicationRule],
                     alertEventElementRules:mutable.HashMap[String,ListBuffer[AlertEventElementRule]],
                     nowDateStr:String)
  :ListBuffer[AlertEventBase]={
    val reList = ListBuffer[AlertEventBase]()
    while(itr.hasNext){
      val base = itr.next()
      val eventId = base.eventId
      val pubDate = base.pubDate
      if(pubDate == null){
        throw new Exception(eventId+"事件的pubDate为空")
      }
      val pubDay = pubDate.substring(0,10)//yyyy-MM-dd
      val pubMD = pubDate.substring(5,10)//MM-dd

      val indications = base.indications
      val betweenDays = DateUtils.getBetweenDays(pubDay,nowDateStr.substring(0,10))
      //算分
      val indicListR = ListBuffer[AlertEventIndication]()
      if(indications != null){
        for(indicCation <- indications){
          val score = indicCation.score
          val alertEventId = indicCation.alertEventId
          val scoreRuleId = indicCation.scoreRuleId
          var decayDays:Long = 1
          var delayDays:Long = 0
          var wt:Double = 0

          //获取衰减天数
          val event = alertEvents.getOrElse(alertEventId,null)
          if(event != null){
            if(event.dampingDays != null && event.dampingDays>0){
              decayDays = event.dampingDays
            }
          }

          //获取迹象规则滞后天数
          val rule = alertEventIndicationRules.getOrElse(scoreRuleId,null)
          if(rule != null){
            delayDays = rule.effectDelayDays
          }

          //获取日期权重
          val timeList = alertEventElementRules.getOrElse(alertEventId,null)
          if(timeList!=null){
            var isFind = false
            var j=0
            while(!isFind && j<timeList.size){
              val eRule = timeList(j)
              val json = eRule.elementRule
              val arr = JSON.parseArray(json)
              var i = 0
              while(!isFind && i<arr.size()){
                val arr1 = arr.getJSONArray(i)
                val start = arr1.getString(0)
                val end = arr1.getString(1)
                if(pubMD.compareTo(start)>=0 && pubMD.compareTo(end)<=0){
                  wt = eRule.baseScore*eRule.weight
                  isFind = true
                }else{
                  i = i+1
                }
              }
              j=j+1
            }
          }
          //计算衰减分
          val abs = Math.abs((betweenDays - delayDays)*1.0d/decayDays)
          val finalScoreDouble = score*(Math.pow(0.5d,abs))*wt
          val bg = BigDecimal(finalScoreDouble)
          val finalScore = bg.setScale(2,BigDecimal.RoundingMode.HALF_UP).doubleValue()

          //将计算后的分值设置进新Rdd中
          val newInd = AlertEventIndication(indicCation.id,indicCation.alertEventId,indicCation.name
            ,indicCation.desc,indicCation.baseScoreWeight,finalScore,indicCation.scoreRuleId,indicCation.rules)
          indicListR.append(newInd)
        }
      }
      //返回
      val newBase = AlertEventBase(base.eventId,base.pubDate,indicListR)
      reList.append(newBase)
    }

    reList
  }

  def flatMapScoreRdd(eventBase:AlertEventBase):ListBuffer[AlertEventIndication]={
    val reList = ListBuffer[AlertEventIndication]()
    val oldList = eventBase.indications
    for(old<-oldList){
      val newIn = AlertEventIndication(null,old.alertEventId,null,null,0,old.score,null,null)
      reList.append(newIn)
    }
    reList
  }

}
