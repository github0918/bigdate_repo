package com.qianfeng.recommend


import com.qianfeng.recommend.config.Config
import com.qianfeng.recommend.hbase.HBaseUtil
import com.qianfeng.recommend.transform.UnionFeatureModelData
import com.qianfeng.recommend.util.SparkHelper
import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory

/**
  * @Description: 合并召回算法的用户及物品特征存储到HBASE
  * @Author: QF    
  * @Date: 2020/8/5 6:20 PM   
  * @Version V1.0 
  */
object UnionFeature {

  private val log = LoggerFactory.getLogger("union-feature")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    System.setProperty("HADOOP_USER_NAME", "root")

    // 解析命令行参数
    val params = Config.parseConfig(UnionFeature, args)
    log.warn("job running please wait ... ")
    // init spark session
    val ss = SparkHelper.getSparkSession(params.env, "union-feature")
    val unionData = UnionFeatureModelData(ss, params.env)

    // 关联用户特征  用户id、user_base_vec
    val userFeatureDF = unionData.genUserFeature()
    userFeatureDF.show(false)

    val userFeatureHFileRDD = unionData.userFeaturesDF2HFile(userFeatureDF,"uf")

    log.warn("load user feature result to hbase ! ")
    // 将特征向量存入到HBASE,先将其换换为HFile
    val hBaseUtil = HBaseUtil(ss, params.zkHost, params.zkPort)
    hBaseUtil.loadHfileRDD2Hbase(userFeatureHFileRDD, params.htableName, params.hfilePath)

    // itemcf 关联文章特征 uid,sim_aid,pred_rate,features,embedding
    val itemCFFeature = unionData.genItemCFFeature()
    itemCFFeature.show(false)
    val itemCFConvert = unionData.featureDataConvert(itemCFFeature)
    val itemCFHFileRDD = unionData.featuresDF2HFile(itemCFConvert, "itemcf")

    // HFile RDD 生成文件后直接加载到HBASE中
    log.warn("load itemcf feature result to hbase ! ")
    hBaseUtil.loadHfileRDD2Hbase(itemCFHFileRDD, params.htableName, params.hfilePath)

    // als 关联文章特征
    val alsFeature = unionData.genALSFeature()
    alsFeature.show(false)
    val alsConvert = unionData.featureDataConvert(alsFeature)
    val alsHFileRDD = unionData.featuresDF2HFile(alsConvert, "als")
    // HFile RDD 生成文件后直接加载到HBASE中
    log.warn("load als feature result to hbase ! ")
    hBaseUtil.loadHfileRDD2Hbase(alsHFileRDD, params.htableName, params.hfilePath)

    ss.stop()
    log.warn("job success! ")
  }
}
