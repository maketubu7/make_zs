package com.bbd.IndicService.service

import java.sql._
import java.util.Properties

import com.bbd.IndicService.bean.ComCaseClass.{AlertEvent, AlertEventElementRule, AlertEventIndicationRule}
import com.bbd.IndicService.tool.PropertiesTool
import org.apache.spark.sql.Row

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object JdbcService {
  val driver = PropertiesTool.getPropertyValue("driver", "jdbc_indic.properties")
  val url = PropertiesTool.getPropertyValue("jdbc_url", "jdbc_indic.properties")
  val username = PropertiesTool.getPropertyValue("username", "jdbc_indic.properties")
  val password = PropertiesTool.getPropertyValue("password", "jdbc_indic.properties")
  val alertEventTabName = PropertiesTool.getPropertyValue("alert_event_table", "jdbc_indic.properties")
  val alertEventIndicationRuleName = PropertiesTool.getPropertyValue("alert_event_indication_rule", "jdbc_indic.properties")
  val alertEventElementRuleName = PropertiesTool.getPropertyValue("alert_event_element_rule", "jdbc_indic.properties")
  val alertEventDailyScore = PropertiesTool.getPropertyValue("alert_event_daily_score", "jdbc_indic.properties")

  def getProperties():Properties={
    val pro = new Properties()
    pro.put("user",username)
    pro.put("password",password)
    pro.put("driver",driver)
    pro
  }

  def getAlertEvent():mutable.HashMap[String,AlertEvent] ={
    val reMap = mutable.HashMap[String,AlertEvent]()

    var conn:Connection = null
    var stmt:PreparedStatement = null
    var rs:ResultSet = null
    val sql:String = "select id,damping_days from "+alertEventTabName
    try{
      Class.forName(driver)
      conn = DriverManager.getConnection(url,username,password)
      stmt = conn.prepareStatement(sql)
      rs = stmt.executeQuery()
      //遍历 封装对象返回
      while(rs.next()){
        val id = rs.getString("id")
        val dampingDays = rs.getLong("damping_days")
        val alertEvent = AlertEvent(id,dampingDays)
        reMap.put(id,alertEvent)
      }
    }catch {
      case e:Exception => {throw e}
    }finally {
      try{rs.close()}catch {case e:Exception => Unit}
      try{stmt.close()}catch {case e:Exception => Unit}
      try{conn.close()}catch {case e:Exception => Unit}
    }
    reMap
  }

  def deleteAlertEventDailyScoreByDay(dateStr:String):Unit={
    var conn:Connection = null
    var stmt:PreparedStatement = null
    val sql = "delete from "+alertEventDailyScore+" where day = str_to_date('"+dateStr+"','%Y-%m-%d %H:%i:%s')"
    try{
      Class.forName(driver)
      conn = DriverManager.getConnection(url,username,password)
      stmt = conn.prepareStatement(sql)
      stmt.executeUpdate()
    }catch {
      case e:Exception => throw e
    }finally {
      try{stmt.close()}catch {case e:Exception => Unit}
      try{conn.close()}catch {case e:Exception => Unit}
    }
  }

  def getAlertEventIndicationRule():mutable.HashMap[String,AlertEventIndicationRule]={
    val reMap = mutable.HashMap[String,AlertEventIndicationRule]()
    var conn:Connection = null
    var stmt:PreparedStatement = null
    var rs:ResultSet = null
    val sql:String = "select id,effect_delay_days from " + alertEventIndicationRuleName
    try{
      Class.forName(driver)
      conn = DriverManager.getConnection(url,username,password)
      stmt = conn.prepareStatement(sql)
      rs = stmt.executeQuery()
      //遍历 封装对象返回
      while(rs.next()){
        val id = rs.getString("id")
        val effectDelayDays = rs.getLong("effect_delay_days")
        val alertEventI = AlertEventIndicationRule(id,null,null,null,null,0,effectDelayDays,0,null,null)
        reMap.put(id,alertEventI)
      }
    }catch {
      case e:Exception => {throw e}
    }finally {
      try{rs.close()}catch {case e:Exception => Unit}
      try{stmt.close()}catch {case e:Exception => Unit}
      try{conn.close()}catch {case e:Exception => Unit}
    }
    reMap
  }

  def getAlertEventElementRule():mutable.HashMap[String,ListBuffer[AlertEventElementRule]]={
    val reMap = mutable.HashMap[String,ListBuffer[AlertEventElementRule]]()
    //val reList = ListBuffer[AlertEventElementRule]()
    var conn:Connection = null
    var stmt:PreparedStatement = null
    var rs:ResultSet = null
    val sql:String = "select alert_event_id,element_rule,base_score,weight from " + alertEventElementRuleName +
      " where element_type='Time'"
    try{
      Class.forName(driver)
      conn = DriverManager.getConnection(url,username,password)
      stmt = conn.prepareStatement(sql)
      rs = stmt.executeQuery()
      //遍历 封装对象返回
      while(rs.next()){
        val alertEventId = rs.getString("alert_event_id")
        val elementRule = rs.getString("element_rule")
        val baseScore = rs.getLong("base_score")
        val weight = rs.getDouble("weight")
        val alertEventE = AlertEventElementRule(alertEventId,elementRule,baseScore,weight)
        var l = reMap.getOrElse(alertEventId,null)
        if(l == null) {//未获取到则新建List，并且插入Map
          l = ListBuffer[AlertEventElementRule]()
          l.append(alertEventE)
          reMap.put(alertEventId,l)
        }else{//如果存在，直接插入Map中的List
          l.append(alertEventE)
        }
      }
    }catch {
      case e:Exception => {throw e}
    }finally {
      try{rs.close()}catch {case e:Exception => Unit}
      try{stmt.close()}catch {case e:Exception => Unit}
      try{conn.close()}catch {case e:Exception => Unit}
    }
    reMap
  }

  def updateScoreTable(itr:Iterator[Row]):Unit={
    var conn:Connection = null
    var stmt:PreparedStatement = null
    val sql = "update " + alertEventDailyScore + " t set t.score = ? where t.alert_event_id = ? and t.day = ?"
    try{
      Class.forName(driver)
      conn = DriverManager.getConnection(url,username,password)
      stmt = conn.prepareStatement(sql)

      var i:Int = 0
      while(itr.hasNext){
        val row = itr.next()
        val alert_event_id = row.getAs[String]("alert_event_id")
        val day = row.getAs[Timestamp]("day")
        val score = row.getAs[Int]("score")

        stmt.setInt(1,score)
        stmt.setString(2,alert_event_id)
        stmt.setTimestamp(3,day)
        stmt.addBatch()
        if(i%200 == 0){
          stmt.executeBatch()
        }
        i = i+1
      }
      stmt.executeBatch()
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      try{stmt.close()}catch {case e:Exception => Unit}
      try{conn.close()}catch {case e:Exception => Unit}
    }
  }
}
