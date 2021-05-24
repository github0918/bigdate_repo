package com.qianfeng.recommend.hbase

import java.net.URI

import com.qianfeng.recommend.util.SparkHelper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.LoggerFactory
/**
  * @Description: 通过生成HFile的方式,将数据高效的写入HBASE
  * @Author: QF    
  * @Date: 2020/7/22 2:09 PM   
  * @Version V1.0 
  */
class HBaseUtil(spark: SparkSession,hbaseZK:String,hbaseZKPort:String) {

  private val log = LoggerFactory.getLogger("HBaseUtil")

  /**
   * Bulk load   桶装载---批次插入
    * 将hfile RDD加载到HBASE中
    * @param hfileRDD  hfile  RDD[(ImmutableBytesWritable,KeyValue)
    * @param tableName 加载到的hbase中的表名
    * @param hfileTmpPath hfile 临时存储目录
    */
  def loadHfileRDD2Hbase(hfileRDD:RDD[(ImmutableBytesWritable,KeyValue)],tableName: String,hfileTmpPath:String)={

    val hfilePath = hfileTmpPath +"/"+ String.valueOf(System.currentTimeMillis())
    val hbaseConf = setHBaseConfig() //设置zk的ip地址和端口
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName) //设置输出表名为
    val conn = ConnectionFactory.createConnection(hbaseConf)  //获取hbase的连接
    log.warn("create hbase connection: "+conn.toString)
    val admin = conn.getAdmin  //获取admin操作对象
    val table = conn.getTable(TableName.valueOf(tableName))  //获取表操作对象
    val job = Job.getInstance(hbaseConf)  //获取一个job实例
    //设置job的输出格式
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])  //设置map阶段输出key的类型
    job.setMapOutputValueClass(classOf[KeyValue])   //设置map阶段输出Value的类型
    job.setOutputFormatClass(classOf[HFileOutputFormat2])  //设置输出格式类
    HFileOutputFormat2.configureIncrementalLoad(job, table, conn.getRegionLocator(TableName.valueOf(tableName)))
    // 如果路径存在就删除，因为加了毫秒作为目录，因此一般不会存在
    deleteFileExist(hfilePath)

    hfileRDD.coalesce(10).saveAsNewAPIHadoopFile(hfilePath, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration)
    val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
    // load 正常执行完毕后， 在hdfs生成的hfile文件就被mv走，不存在了
    bulkLoader.doBulkLoad(new Path(hfilePath), admin, table, conn.getRegionLocator(TableName.valueOf(tableName)))
    log.warn("load hfile to hbase success! hfile path: "+hfilePath)

  }


  /**
    * 配置HBASE参数
    * @return
    */
  def setHBaseConfig():Configuration={
    val hbaseConf:Configuration = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, hbaseZK)  //conf.set("hbase.zookeeper.quorum","hadoopp01")
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, hbaseZKPort) //hbase.zookeeper.property.clientPort
    hbaseConf
  }

  /**
    * 如果hdfs存在传入的文件路径，则删除
    * @param filePath  文件路径
    */
  def deleteFileExist(filePath: String): Unit ={
    val output = new Path(filePath)
    val hdfs = FileSystem.get(new URI(filePath), new Configuration)
    if (hdfs.exists(output)){
      hdfs.delete(output, true)  //true : 递归
    }
  }
}


object HBaseUtil {
  def apply(spark: SparkSession,hbaseZK:String,hbaseZKPort:String): HBaseUtil = new HBaseUtil(spark,hbaseZK,hbaseZKPort)

  def main(args: Array[String]): Unit = {
    HBaseUtil(SparkHelper.getSparkSession("dev","test"),"hadoop01","2181") //.loadHfileRDD2Hbase()  //.deleteFileExist("/words")
  }
}
