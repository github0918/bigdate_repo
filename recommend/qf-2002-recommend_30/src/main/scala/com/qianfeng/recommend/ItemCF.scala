package com.qianfeng.recommend

import com.qianfeng.recommend.config.Config
import com.qianfeng.recommend.hbase.HBaseUtil
import com.qianfeng.recommend.transform.ItemCFModelData
import com.qianfeng.recommend.util.SparkHelper
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.LoggerFactory

/**
 * 基于物品的相似度进行推荐
 * 1、命令行参数解析
 * 2、使用spark查询hive中的dwb_news.user_article_action，并将该表中的action转换为评分
 * 3、将第二步中的DF转换成矩阵
 * 4、求矩阵中物品的相似度
 *
 */
object ItemCF {
  //日志打印对象
  val log = LoggerFactory.getLogger(ItemCF.getClass)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    System.setProperty("HADOOP_USER_NAME", "root")

    // 解析命令行参数
    val params = Config.parseConfig(ItemCF, args)
    log.warn("job running please wait ... ")
    // init spark session
    val ss = SparkHelper.getSparkSession(params.env, "item-cf")

    //查询hive中用户物品行为数据：dwb_news.user_article_action
    // 基础数据处理工具类
    val modelData = ItemCFModelData(ss, params.env)

    //查询dwb_news.user_article_action
    //val uaaDF: DataFrame = modelData.loadSourceUserArticleActionData()

    //将用户文章行为数据生成总评分
    val uaDF: DataFrame = modelData.genUserRatingData()


    //将评分数据随机划分训练数据和测试数据  ---训练数据80%  测试数据20%
    val Array(training, test) = uaDF.randomSplit(Array(0.8, 0.2))
    training.cache()  //将训练数据缓存
    //training.orderBy("uid")show(false)
    // 没有真实数据时，可以加载一些简单测试数据，运行算法流程
    //val ratingDF = modelData.someTestData().randomSplit(Array(0.8,0.2))

    //用户行为评分DataFrame 转换为分布式坐标矩阵
    val ratingMatrix:CoordinateMatrix = modelData.ratingDF2Matrix(training)

    // 分布式坐标矩阵转换为行索引矩阵，计算行索引矩阵各个列之间的相似度，得到相似度矩阵
    val similarityMartix=  ratingMatrix.toRowMatrix().columnSimilarities()
    println(similarityMartix)  //相似矩阵

    // 相似度矩阵转换为DataFrame
    val simDF = modelData.similarityMatrix2DF(similarityMartix)
    simDF.sort("aid","sim_aid").show(100,false)

    // 评分DataFrame和相似度DataFrame关联，同时计算评分和相似度的乘积
    val joinDF = modelData.joinRatingAndSimilarity(training,simDF)
    //joinDF.sort("uid","aid_x")show(100,false)
    joinDF.cache()
    joinDF.show(false)
    //使用测试数据进行预测
    val predictDF = modelData.predictForTestData(joinDF,test)
    predictDF.show(false)
    // 评估器，这里用来计算预测出来的评分和原始测试集的评分的均方误差，来衡量训练出的模型优劣
    val evaluator = new RegressionEvaluator()
      .setLabelCol("rating")  //真实的评分列
      .setPredictionCol("pred_rating")  //预测评分列
    // 计算均方误差
    val rmse = evaluator.setMetricName("rmse").evaluate(predictDF)
    log.warn(s"[ItemCF算法] 均方根误差(Root-mean-square error) = $rmse")
    println(s"[ItemCF算法] 均方根误差(Root-mean-square error) = $rmse")

    //为所有用户推荐topK的内容
    val recoDF = modelData.recommendForAllUser(joinDF,params.topK)
    recoDF.show(false)
    // 推荐结果保存一份到到HDFS
    recoDF.write.mode(SaveMode.Overwrite).format("ORC").saveAsTable("dwb_news.itemcf")

    //将结果存储到hbase中
    val convertDF: DataFrame = modelData.recommendDataConvert(recoDF)

    //将df转换成RDD
    log.warn("start gen hfile, load itemcf result to hbase ! ")
    // 将推荐结果的DataFrame转换为HFile RDD
    val hfileRDD:RDD[(ImmutableBytesWritable,KeyValue)] = modelData.itemCFDF2HFile(convertDF)

    //获取hbase的工具
    val hBaseUtil =  HBaseUtil(ss,params.zkHost,params.zkPort)
    hBaseUtil.loadHfileRDD2Hbase(hfileRDD,params.htableName,params.hfilePath)

    ss.stop()
  }
}
