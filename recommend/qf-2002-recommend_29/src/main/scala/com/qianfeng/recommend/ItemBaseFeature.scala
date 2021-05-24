package com.qianfeng.recommend

import breeze.linalg.DenseVector
import com.qianfeng.recommend.config.Config
import com.qianfeng.recommend.hbase.HBaseUtil
import com.qianfeng.recommend.transform.ItemBaseFeatureModelData
import com.qianfeng.recommend.udfs.FeatureUDF
import com.qianfeng.recommend.util.SparkHelper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{MinMaxScaler, OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.{SparseVector, Vectors}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.udf
import org.slf4j.LoggerFactory

/**
  * @Description:  生成文章基本特征向量，并存储到HBASE中
  * @Author: QF    
  * @Date: 2020/7/24 5:20 PM   
  * @Version V1.0 
  */
object ItemBaseFeature {
  private val log = LoggerFactory.getLogger("item-base-feature")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    System.setProperty("HADOOP_USER_NAME", "root")

    // 解析命令行参数
    val params = Config.parseConfig(ItemBaseFeature, args)
    log.warn("job running please wait ... ")
    // init spark session
    val ss = SparkHelper.getSparkSession(params.env, "item-base-feature")
    // 基础数据处理
    val modelData = ItemBaseFeatureModelData(ss, params.env)

    // 从dwb_news.article_base_info数据表中读取文章特征数据
    val itemFeatureDF:DataFrame = modelData.loadSourceArticleBaseInfoData()
    /**
      * 对对itemFeatureDF中的`type_name`列，做one-hot编码
      * 因为 spark OneHotEncoderEstimator 只接受数值类型列作为one-hot编码的输入，因此我们先使用 StringIndexer将type_name的中文类型类
      * 转换为对应的索引，StringIndexer 的作用举例如下
      * type_name
      * ------------
      * 娱乐
      * 情感
      * 汽车
      * 娱乐
      *
      * 执行StringIndexer后
      * type_name
      * ------------
      * 0
      * 1
      * 2
      * 0
      * 相当于每一个值有一个索引，以索引号代替数值
      *
      */
    val indexer = new StringIndexer()
      .setInputCol("type_name")  //输入列
      .setOutputCol("type_name_index")  //输出列
    // 对转换为索引表示的type_name的使用one-hot编码，这里需要注意的是，转换索引操作并不影响我们one-hot编码的结果
    // 之所以做转换，是因为spark的 OneHotEncoderEstimator 只接受数值类型作为输入
    val encoder = new OneHotEncoderEstimator()
       .setInputCols(Array(indexer.getOutputCol)) //设置输入列
       .setOutputCols(Array("type_name_vec"))  //输出列

    /** 三个数值类型特征合并为一个向量，举例如下
      *
      * article_num | img_num| pub_gap
      * -------------|--------|------------
      * 356         | 18     |4
      *
      * 对上述三列执行 VectorAssembler 后会得到如下结果
      *
      * features
      * ---------
      * [356,18,4]
      */
    val numAssembler = new VectorAssembler()
      .setInputCols(Array("article_num", "img_num", "pub_gap"))  //设置合并的输入的列
      .setOutputCol("numFeatures")  //输出列

    // 对各个列进行最小最大归一化
    val numFeaturesScaler = new MinMaxScaler()
      .setInputCol("numFeatures")
      .setOutputCol("numFeaturesScaler")

    // 合并归一化后的数值特征列[article_num,img_num,pub_gap]和one-hot后的type_name列合并为一个向量
    val assembler = new VectorAssembler()
      .setInputCols(Array("numFeaturesScaler", "type_name_vec"))  //合并的输入列
      .setOutputCol("features")  //输出列

    //定义一个Pipeline,将各个特征转换操作放入到其中处理
    val pipeline = new Pipeline()
      .setStages(Array(indexer, encoder, numAssembler, numFeaturesScaler, assembler))

    // 将数据集itemFeatureDF，就是我们的文章内容信息数据作用到我们定义的pipeline上
    val pipelineModel = pipeline.fit(itemFeatureDF)  //训练
    // 对我们的数据集执行定义的转换操作
    val featuresDF = pipelineModel.transform(itemFeatureDF)  //测试

    featuresDF.show(false)
    import ss.implicits._

    //  将features列SparseVector转换为string,保留将文章ID和其对应的特征向量两列
    val baseFeatureDF = featuresDF
      .withColumn("features", FeatureUDF.vector2Str($"features"))
      .select("article_id", "features")

    // 将特征向量保存一份到到HDFS
    baseFeatureDF.write.mode(SaveMode.Overwrite).format("ORC").saveAsTable("dwb_news.article_base_vector")

    // 将特征向量存入到HBASE,先将其转换为HFile
    val hBaseUtil = HBaseUtil(ss, params.zkHost, params.zkPort)
    log.warn("start gen hfile, load item base feature result to hbase ! ")
    // 物品特征的DataFrame转换为HFile RDD
    val featureHFileRDD = modelData.itemBaseFeatureDF2HFile(baseFeatureDF)
    // HFile RDD 生成文件后直接加载到HBASE中
    hBaseUtil.loadHfileRDD2Hbase(featureHFileRDD, params.htableName, params.hfilePath)
    ss.stop()
    log.warn("job success! ")
  }
}
