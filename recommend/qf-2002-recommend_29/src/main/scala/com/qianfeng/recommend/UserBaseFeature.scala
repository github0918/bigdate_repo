package com.qianfeng.recommend

import com.qianfeng.recommend.config.Config
import com.qianfeng.recommend.hbase.HBaseUtil
import com.qianfeng.recommend.transform.UserBaseFeatureModelData
import com.qianfeng.recommend.udfs.FeatureUDF
import com.qianfeng.recommend.util.SparkHelper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.udf
import org.slf4j.LoggerFactory

/**
  * @Description: 生成用户基本特征向量，并存储到HBASE中
  * @Author: QF    
  * @Date: 2020/7/28 12:37 PM   
  * @Version V1.0 
  */
object UserBaseFeature {

  private val log = LoggerFactory.getLogger("user-base-feature")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    System.setProperty("HADOOP_USER_NAME", "root")

    // 解析命令行参数
    val params = Config.parseConfig(UserBaseFeature, args)
    log.warn("job running please wait ... ")
    // init spark session
    val ss = SparkHelper.getSparkSession(params.env, "user-base-feature")
    // 基础数据处理
    val modelData = UserBaseFeatureModelData(ss, params.env)

    // 读取用户特征数据
    val userFeatureDF = modelData.loadSourceUserBaseFeature()

    // 为gender列值创建索引表示
    val genderIndexer = new StringIndexer()
      .setInputCol("gender").setOutputCol("gender_index")
    // 为email列创建索引表示
    val emailIndexer = new StringIndexer()
      .setInputCol("email_suffix").setOutputCol("email_index")

    // 对gender email 特征进行one-hot 编码
    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array(genderIndexer.getOutputCol, emailIndexer.getOutputCol))
      .setOutputCols(Array("gender_vec", "email_vec"))

    // 对于age特征我们划分年龄段为 [0,15,,25,35,50,60] 表示 0-15, 15-25 依次类推
    // Bucketizer 就可以帮我们做这件事
    val splits = Array(0, 15, 25, 35, 50, 60, Double.PositiveInfinity)
    val bucketizer = new Bucketizer()
      .setInputCol("age")
      .setOutputCol("age_buc")
      .setSplits(splits)

    // 对bucket 后的 age 列进行最小最大归一化
    val ageAssembler = new VectorAssembler()
      .setInputCols(Array("age_buc"))
      .setOutputCol("age_vec")

    val featuresScaler = new MinMaxScaler()
      .setInputCol("age_vec")
      .setOutputCol("age_scaler")

    // 合并归一化后的数值特征列[article_num,img_num,pub_gap]和one-hot后的type_name列合并为一个向量
    val assembler = new VectorAssembler()
      .setInputCols(Array("gender_vec", "email_vec", "age_scaler"))
      .setOutputCol("features")

    // 定义一个Pipeline,将各个特征转换操作放入到其中处理
    val pipeline = new Pipeline()
      .setStages(Array(genderIndexer, emailIndexer, encoder, bucketizer, ageAssembler, featuresScaler, assembler))

    // 将数据集itemFeatureDF，就是我们的文章内容信息数据作用到我们定义的pipeline上
    val pipelineModel = pipeline.fit(userFeatureDF)  //训练
    // 对我们的数据集执行定义的转换操作
    val featuresDF = pipelineModel.transform(userFeatureDF)  //测试
    featuresDF.show(false)
    import ss.implicits._


    //  将features列SparseVector转换为string,保留将文章ID和其对应的特征向量两列
    val baseFeatureDF = featuresDF
      .withColumn("features",  FeatureUDF.vector2Str($"features"))
      .select("uid", "features")

    // 将特征向量保存一份到到HDFS
    baseFeatureDF.write.mode(SaveMode.Overwrite).format("ORC").saveAsTable("dwb_news.user_base_vector")

    // 将特征向量存入到HBASE,先将其换换为HFile
    val hBaseUtil = HBaseUtil(ss, params.zkHost, params.zkPort)
    log.warn("start gen hfile, load user base feature result to hbase ! ")
    // 将用户特征的DataFrame转换为HFile RDD
    val featureHFileRDD = modelData.userBaseFeatureDF2HFile(baseFeatureDF)
    // HFile RDD 生成文件后直接加载到HBASE中
    hBaseUtil.loadHfileRDD2Hbase(featureHFileRDD, params.htableName, params.hfilePath)
    ss.stop()
    log.warn("job success! ")

  }

}
