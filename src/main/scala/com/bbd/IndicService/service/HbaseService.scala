package com.bbd.IndicService.service

import com.bbd.IndicService.tool.{PropertiesTool, StringUtils}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}

object HbaseService {
  def getHbaseConf():org.apache.hadoop.conf.Configuration={
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",PropertiesTool.getPropertyValue("hbase_zk_quorum","hbase.properties"))
    conf.set("hbase.zookeeper.port",PropertiesTool.getPropertyValue("hbase_zk_port","hbase.properties"))
    conf.set("zookeeper.znode.parent",PropertiesTool.getPropertyValue("hbase_zk_parent","hbase.properties"))
    conf
  }

  def setScan(conf:org.apache.hadoop.conf.Configuration,startRowkey: String,endRowkey:String
              ,families:Array[String],columns:Array[String]):Unit={
    var scan:Scan = null
    if(StringUtils.isEmpty(startRowkey) || StringUtils.isEmpty(endRowkey)){
      scan = new Scan()
    }else{
      scan = new Scan(Bytes.toBytes(startRowkey),Bytes.toBytes(endRowkey))
    }
    if(families != null){
      for(family <- families){
        scan.addFamily(Bytes.toBytes(family))
      }
    }
    if(columns != null){
      for(column<-columns){
        val cols = column.split(":")
        scan.addColumn(Bytes.toBytes(cols(0)),Bytes.toBytes(cols(1)))
      }
    }
    val proto = ProtobufUtil.toScan(scan)
    val scan2String = Base64.encodeBytes(proto.toByteArray)
    conf.set(TableInputFormat.SCAN,scan2String)

  }
}
