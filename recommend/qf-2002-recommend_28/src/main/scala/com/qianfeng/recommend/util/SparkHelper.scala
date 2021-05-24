package com.qianfeng.recommend.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @constructor 根据环境变量参数创建Spark Session ： dev \ test \ pro
  * @author QF
  * @date 2020/6/9 2:59 PM
  * @version V1.0
  */
object SparkHelper {

  /**
   * 根据传入的dev和应用名称--->获取spark不同平台下的sparksession
   * @param env
   * @param appName
   * @return
   */
  def getSparkSession(env: String, appName: String) = {
    env match {
        //返回生产级别的配置
      case "prod" => {
        //获取sparkconf对象
        val conf = new SparkConf()
          .setAppName(appName)
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          //          .set("spark.sql.hive.metastore.version","2.3.0")
          .set("spark.sql.cbo.enabled", "true")
          .set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.enable", "true")
          .set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
          .set("spark.debug.maxToStringFields","200")

        //返回sparksession
        SparkSession
          .builder()
          .config(conf)
          .enableHiveSupport()
          .getOrCreate()
      }

        //返回测试环境的sparksession
      case "dev" => {
        val conf = new SparkConf()
          .setAppName(appName + " DEV")
          .setMaster("local[6]")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          //          .set("spark.sql.hive.metastore.version","1.2.1")
          .set("spark.sql.cbo.enabled", "true")
          .set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.enable", "true")
          .set("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
            .set("spark.debug.maxToStringFields","200")
        //返回sparksession
        SparkSession
          .builder()
          .config(conf)
          .enableHiveSupport()
          .getOrCreate()
      }
        //其它情况
      case _ => {
        println("not match env, exits")
        System.exit(-1)
        null
      }
    }
  }

  /**
   * 测试
   * @param args
   */
  def main(args: Array[String]): Unit = {
    print(getSparkSession("dev","test-sparksession"))
  }
}
