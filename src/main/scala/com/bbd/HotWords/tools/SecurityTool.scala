package com.bbd.HotWords.tools

import com.bbd.huawei.LoginUtil
import org.apache.hadoop.conf.Configuration

object SecurityTool {
  def securityPreUp(): Unit ={
    val directory = System.getProperty("user.dir")
    println("directory:=== " + directory)
    val krbFile = directory + "/krb5.conf"
    val userKeyTableFile = directory + "/user.keytab"
    val userPrincipal = "bbd"
    val ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client"
    println("keytab:===== " + krbFile)
    println("userkey:==== " + userKeyTableFile)
    LoginUtil.setKrb5Config(krbFile)
    LoginUtil.setZookeeperServerPrincipal("zookeeper/hadoop.hadoop.com")
    LoginUtil.setJaasFile(userPrincipal, userKeyTableFile)
    LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userPrincipal, userKeyTableFile)
    val hadoopConf: Configuration = new Configuration()
    hadoopConf.addResource("hive-site.xml")
    hadoopConf.addResource("core-site.xml")
    hadoopConf.addResource("hdfs-site.xml")
    LoginUtil.login(userPrincipal, userKeyTableFile, krbFile, hadoopConf)
    println("login util successs")
  }

  def securityPreLocal(): Unit ={
    val directory = System.getProperty("user.dir")
    println("directory:=== " + directory)
    val krbFile = directory + "/src/main/resources/krb5.conf"
    val userKeyTableFile = directory + "/src/main/resources/user.keytab"
    val userPrincipal = "bbd"
    println("keytab:===== " + krbFile)
    println("userkey:==== " + userKeyTableFile)
    LoginUtil.setKrb5Config(krbFile)
    LoginUtil.setZookeeperServerPrincipal("zookeeper/hadoop.hadoop.com")
    LoginUtil.setJaasFile(userPrincipal, userKeyTableFile)
    val hadoopConf: Configuration = new Configuration()
    hadoopConf.addResource("hive-site.xml")
    hadoopConf.addResource("core-site.xml")
    hadoopConf.addResource("hdfs-site.xml")
    LoginUtil.login(userPrincipal, userKeyTableFile, krbFile, hadoopConf)
    println("login util successs")
  }
}
