package com.qianfeng.recommend.transform

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

/**
  * @Description: 为用户基础特征转换提供数据处理方法
  * @Author: QF    
  * @Date: 2020/7/28 12:40 PM   
  * @Version V1.0 
  */
class UserBaseFeatureModelData(spark: SparkSession, env:String) extends ModelData(spark: SparkSession, env:String) {

  /**
    * UserBaseFeature 产生的结果生成HFile RDD
    * 需要添加建立好hbase表，行键将会存入uid, 列族为 f1 , 列为 base ,值为物品特征的向量
    * 创建hbase表样例如：
    *   create_namespace 'recommend'
    *   create 'recommend:user-feature','f1'
    * @param userBaseFeatureDF  生成的用户特征UserBaseFeature向量，格式[uid,SparseVector.toString]
    * @return
    */
  def userBaseFeatureDF2HFile(userBaseFeatureDF:sql.DataFrame):RDD[(ImmutableBytesWritable,KeyValue)]={

    val hfileRdd = userBaseFeatureDF.rdd.sortBy(x =>x.get(0).toString).flatMap(row=>{
      val itemId = row.getString(0)
      val features = row.getString(1)
      val listBuffer = new ListBuffer[(ImmutableBytesWritable, KeyValue)]
      val kv1: KeyValue = new KeyValue(Bytes.toBytes(itemId), Bytes.toBytes("f1"), Bytes.toBytes("base"), Bytes.toBytes(features))
      // 多个列按列名字典顺序append
      listBuffer.append((new ImmutableBytesWritable, kv1))
      listBuffer
    })
    hfileRdd
  }



}


object UserBaseFeatureModelData{
  def apply(spark: SparkSession, env: String): UserBaseFeatureModelData = new UserBaseFeatureModelData(spark, env)
}