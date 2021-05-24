package com.qianfeng.recommend

import com.qianfeng.recommend.config.Config
import com.qianfeng.recommend.hbase.HBaseUtil
import com.qianfeng.recommend.transform.ALSModelData
import com.qianfeng.recommend.util.SparkHelper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.LoggerFactory


/**
  * @Description: 基于模型的协同过滤 ALS 算法
  * @Author: QF    
  * @Date: 2020/7/20 5:54 PM   
  * @Version V1.0 
  */
object ALSCF {

  private val log = LoggerFactory.getLogger("als")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    System.setProperty("HADOOP_USER_NAME", "root")

    // 解析命令行参数
    val params = Config.parseConfig(ALSCF, args)
    log.warn("job running please wait ... ")
    // init spark session
    val ss = SparkHelper.getSparkSession(params.env, "als")
    // 防止ALS算法迭代次数过多，DAG过深，RDD的lineage过长，从而造成StackOverflowError异常
    ss.sparkContext.setCheckpointDir("/checkpoint/als")


    // 基础数据处理
    val modelData = ALSModelData(ss, params.env)

    // 将用户原始行为数据转换为评分数据
    val ratingDF = modelData.genUserRatingData()

    // 没有真实数据时，可以加载一些简单测试数据，运行算法流程
    //val ratingDF = modelData.someTestData()

    // 将训练数据分为训练集合测试集,4:1
    val Array(training, test) = ratingDF.randomSplit(Array(0.8, 0.2))

    //缓存训练数据集
    training.cache()

    // ALS 模型训练
    val als = new ALS()
      //交替最小二乘求解最大迭代次数
      .setMaxIter(6)
      // 正则化系数，避免过拟合
      .setRegParam(0.01)
      //用户列
      .setUserCol("uid")
      //物品列
      .setItemCol("aid")
      //评分列
      .setRatingCol("rating")
      // 当用训练出来的模型做预测时，如果测试集中包含了训练集中没有的用户或者物品(这在生成环境会很常见).对这些用户或物品做推荐时，如何取值
      // drop 策略表示这就删除掉对这些用户或物品的数据，Nan策略表示，用NaN表示这些值
      .setColdStartStrategy("drop")
      // 限制最小二乘解不出现负值
      .setNonnegative(true)
      //是否是隐式反馈数据集  -- 显示是显示隐式集
      .setImplicitPrefs(true)
      // 物品和用户特征的维度
      .setRank(16)

    // 训练模型
    val model:ALSModel = als.fit(training)

    // 在测试集上预测数据
    val predictions:DataFrame = model.transform(test)

    // 评估器，这里用来计算，预测出来的评分和原始测试集的评分的均方误差，来衡量训练出的模型优劣
    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")  //均方误差
      .setLabelCol("rating")  //真实的得分
      .setPredictionCol("prediction")  //预测得分
    // 计算均方误差
    val rmse = evaluator.evaluate(predictions)
    log.warn(s"[ALS算法] 均方根误差(Root-mean-square error) = $rmse")
    println(s"[ALS算法] 均方根误差(Root-mean-square error) = $rmse")

    // 为所有用户推荐topK结果，注意这里的结果包含用户已经有过行为的物品
    val userRecs: DataFrame = model.recommendForAllUsers(params.topK)

    // 从ALS结果中，过滤掉用户已经有过行为的物品
    val recoDF = modelData.filterALSRecommendForAllUser(ratingDF,userRecs)
    recoDF.show(false)

    // 推荐结果保存一份到到HDFS
    recoDF.write.mode(SaveMode.Overwrite).format("ORC").saveAsTable("dwb_news.als")
    //将已经过滤好的推荐列表转换数据格式
    val convertDF = modelData.recommendDataConvert(recoDF)

    // 结果存储到HBASE中
    val hBaseUtil =  HBaseUtil(ss,params.zkHost,params.zkPort)
    log.warn("start gen hfile, load als result to hbase ! ")
    // 将推荐结果的DataFrame转换为HFile RDD
    val hfileRDD = modelData.alsDF2HFile(convertDF)
    // HFile RDD 生成文件后直接加载到HBASE中
    hBaseUtil.loadHfileRDD2Hbase(hfileRDD,params.htableName,params.hfilePath)


    ss.stop()
    log.warn("job success! ")
  }
}
