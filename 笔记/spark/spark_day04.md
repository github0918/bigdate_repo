

# day04 [Spark Streaming]

## 今日内容

- Spark Streaming简介
- Spark Streaming的原理和架构
- Spark Streaming之基础抽象DStream
- DStream相关操作
- Spark Streaming与flume整合
- Spark Streaming与kafka整合

## 教学目标

- 掌握Spark Streaming的原理和架构
- 掌握DStream的相关操作
- 能够完成Spark Streaming与flume整合
- 能够完成Spark Streaming与kafka整合

# 第一章 Spark Streaming概述

## 1.1 Spark Streaming简介

![](img\SparkStreaming官网介绍.png)

Spark Streaming可以很容易的构建高吞吐量和容错能力强的流式应用.
Spark Streaming类似于Apache Storm，用于流式数据的处理。
根据其官方文档介绍，Spark Streaming有高吞吐量和容错能力强等特点。
Spark Streaming支持的数据源有很多，例如：Kafka、Flume、Twitter、ZeroMQ和简单的TCP套接字等等。
数据输入后可以用Spark的高度抽象操作如：map、reduce、join、window等进行运算。
而结果也能保存在很多地方，如HDFS，数据库等。另外Spark Streaming也能和MLlib（机器学习）以及Graphx完美融合。

![](img\SparkStreaming流式处理.png)

Spark Streaming是核心Spark Core API的扩展,可以实现实时数据流的可扩展,高吞吐,容错流处理.
数据可以来自很多数据源(例如kafka,Flume或者TCP套接字)中获取,并且可以使用高级函数表示的复杂算法进行处理.
最后,处理后的数据可以推送到文件系统,数据库和实时仪表板.

## 1.2 Spark Streaming的特点

### 1.2.1 易用性

![](img\易用性.png)

可以像编写离线批处理一样去编写流式程序，支持java/scala/python语言。

### 1.2.2 容错性

![](img\容错性.png)

Spark Streaming在没有额外代码和配置的情况下可以恢复丢失的工作。

### 1.2.3 易整合

![](img\易整合.png)

流式处理与批处理和交互式查询相结合。

## 1.3 Spark Streaming与Storm的对比

|       Spark Streaming        |        Storm        |
| :--------------------------: | :-----------------: |
| ![](img\Spark Streaming.jpg) | ![](img\Storm.jpg)  |
|  开发语言:Scala/Java/Python  |  开发语言:Clojure   |
|       编程模型:DStream       | 编程模型:Spout/Bolt |

# 第二章 Spark Streaming原理

## 2.1 Spark Streaming原理

Spark Streaming是基于spark的流式批处理引擎,其基本原理是把输入数据以某一段时间间隔批量的处理,
当批处理间隔缩短到秒级时,便可以用于处理实时数据流.也就是说,在内部,它的工作原理如下图所示,Spark Streaming接收实时输入数据流并将数据分成批处理,然后由Spark引擎处理,以批量生成最终结果流

![](img\Spark Streaming流式处理图解.png)

## 2.2 Spark Streaming计算流程

Spark Streaming是将流式计算分解成一系列短小的批处理作业.这里的批处理引擎是Spark Core.也就是把Spark Streaming的输入数据按照batch size(如1秒)分成一段一段的数据(Discretized Stream),每一段数据都转换成Spark中的RDD(Resilient Distributed Dataset),然后将Spark Streaming中对DStream的transformation操作变为针对Spark中对RDD的transformation操作,将RDD经过操作变成中间结果保存在内存中.整个流式计算根据业务的需求可以对中间的结果进行缓存或者存储到外部设备.下图显示了Spark Streaming的整个流程。

![](img\Spark Streaming架构图.jpg)

## 2.3 Spark Streaming的容错性

对于流式计算来说，容错性至关重要。首先我们要明确一下Spark中RDD的容错机制。每一个RDD都是一个不可变的分布式可重算的数据集，其记录着确定性的操作继承关系（lineage），所以只要输入数据是可容错的，那么任意一个RDD的分区（Partition）出错或不可用，都是可以利用原始输入数据通过转换操作而重新算出的。对于Spark Streaming来说，其RDD的传承关系如下图所示：

![](img\Spark Streaming容错性.jpg)

图中的每一个椭圆形表示一个RDD，椭圆形中的每个圆形代表一个RDD中的一个Partition，图中的每一列的多个RDD表示一个DStream（图中有三个DStream），而每一行最后一个RDD则表示每一个Batch Size所产生的中间结果RDD。我们可以看到图中的每一个RDD都是通过lineage相连接的，由于Spark Streaming输入数据可以来自于磁盘，例如HDFS（多份拷贝）或是来自于网络的数据流（Spark Streaming会将网络输入数据的每一个数据流拷贝两份到其他的机器）都能保证容错性，所以RDD中任意的Partition出错，都可以并行地在其他机器上将缺失的Partition计算出来。这个容错恢复方式比连续计算模型（如Storm）的效率更高。

## 2.4 Spark Streaming实时性

对于实时性的讨论，会牵涉到流式处理框架的应用场景。Spark Streaming将流式计算分解成多个Spark Job，对于每一段数据的处理都会经过Spark DAG图分解以及Spark的任务集的调度过程。对于目前版本的Spark Streaming而言，其最小的Batch Size的选取在0.5~2秒钟之间（Storm目前最小的延迟是100ms左右），所以Spark Streaming能够满足除对实时性要求非常高（如高频实时交易）之外的所有流式准实时计算场景。

# 第三章 DStream

## 3.1 什么是DStream

Discretized Stream是Spark Streaming的基础抽象，代表持续性的数据流和经过各种Spark算子操作后的结果数据流。在内部实现上，DStream是一系列连续的RDD来表示。每个RDD含有一段时间间隔内的数据，如下图：

![](img\DStream_1.png)

应用于DStream的任何操作都转换为底层RDD上的操作,例如,在之前将lines DStream转换为words DStream,flatMap操作应用于lines DStream中的每一个RDD以生成words DStream的words RDD

![](img\DStream原理.png)

这些底层RDD转换是由Spark引擎计算的,DStream操作隐藏了大部分细节.

## 3.2 DStream相关操作

DStream上的操作与RDD的类似,分为Transformations(转换)和OutputOperations(输出)两种,此外转换操作中还有一些比较特殊的操作,如"updateStateByKey(),transform()以及各种Window相关的操作.

### 3.2.1 Transformations on DStreams

|         Transformation          |                           Meaning                            |
| :-----------------------------: | :----------------------------------------------------------: |
|            map(func)            | 对DStream中的各个元素进行func函数操作，然后返回一个新的DStream |
|          flatMap(func)          | 与map方法类似，只不过各个输入项可以被输出为零个或多个输出项  |
|          filter(func)           | 过滤出所有函数func返回值为true的DStream元素并返回一个新的DStream |
|   repartition(numPartitions)    |     增加或减少DStream中的分区数，从而改变DStream的并行度     |
|       union(otherStream)        | 将源DStream和输入参数为otherDStream的元素合并，并返回一个新的DStream. |
|             count()             | 通过对DStream中的各个RDD中的元素进行计数，然后返回只有一个元素的RDD构成的DStream |
|          reduce(func)           | 对源DStream中的各个RDD中的元素利用func进行聚合操作，然后返回只有一个元素的RDD构成的新的DStream. |
|         countByValue()          | 对于元素类型为K的DStream，返回一个元素为（K,Long）键值对形式的新的DStream，Long对应的值为源DStream中各个RDD的key出现的次数 |
|   reduceByKey(func[numTasks])   | 利用func函数对源DStream中的key进行聚合操作，然后返回新的（K，V）对构成的DStream |
|  join(otherStream,[numTasks])   | 输入为（K,V)、（K,W）类型的DStream，返回一个新的（K，（V，W））类型的DStream |
| cogroup(otherStream,[numTasks]) | 输入为（K,V)、（K,W）类型的DStream，返回一个新的(K,Seq[V], Seq[W]) 元组类型的DStream |
|         transform(func)         | 通过RDD-to-RDD函数作用于DStream中的各个RDD，可以是任意的RDD操作，从而返回一个新的RDD |
|     updateStateByKey(func)      | 根据key的之前状态值和key的新值，对key进行更新，返回一个新状态的DStream |

### 3.2.2 特殊的Transformations

#### 3.2.2.1 UpdateStateByKey Operation

UpdateStateByKey用于记录历史记录，保存上次的状态.如果使用UpdateStateByKey,必须执行两个步骤

- 定义状态,状态可以是任意数据类型.
- 定义状态更新函数.使用函数指定如何使用先前状态和输入流中的新值更新状态.

#### 3.2.2.2 Window Operation(开窗函数)

滑动窗口转换操作的计算过程如下图所示，我们可以事先设定一个滑动窗口的长度（也就是窗口的持续时间），并且设定滑动窗口的时间间隔（每隔多长时间执行一次计算），然后，就可以让窗口按照指定时间间隔在源DStream上滑动，每次窗口停放的位置上，都会有一部分DStream被框入窗口内，形成一个小段的DStream，这时，就可以启动对这个小段DStream的计算。

![](img\Dstream_Window.png)

- 红色的矩形就是一个窗口，窗口框住的是一段时间内的数据流。
- 这里面每一个time都是时间单元，在官方的例子中，每隔window size是3 time unit, 而且每隔2个单位时间，窗口会slide一次。
- 所以基于窗口的操作,需要指定2个参数:
  - *window length* - The duration of the window (3 in the figure).
    - 窗口长度,一段时间内数据的容器.
  - sliding interval - The interval at which the window operation is performed (2 in the figure).
    - 滑动间隔,每隔多久计算一次.

### 3.2.3 Ouput Operations on DStreams

Output Operations可以将DStream的数据输出到外部的数据库或文件系统，

当某个Output Operations被调用时（与RDD的Action相同），Spark Streaming程序才会开始真正的计算过程。

|          Output Operation          |                           Meaning                            |
| :--------------------------------: | :----------------------------------------------------------: |
|              print()               |                         打印到控制台                         |
|  saveAsTextFiles(prefix,[suffix])  | 保存流的内容为文本文件，文件名为"prefix-TIME_IN_MS[.suffix]". |
| saveAsObjectFiles(prefix,[suffix]) | 保存流的内容为SequenceFile，文件名为"prefix-TIME_IN_MS[.suffix]". |
| saveAsHadoopFiles(prefix,[suffix]) | 保存流的内容为hadoop文件，文件名为"prefix-TIME_IN_MS[.suffix]". |
|          foreachRDD(func)          |                对Dstream里面的每个RDD执行func                |

# 第四章 DStream操作实战

## 4.1 SparkStreaming接受socket数据,实现词频统计

### 4.1.1 架构图

![](img\Spark Streaming读取Socket数据.png)

### 4.1.2 代码实现

#### 4.1.2.1 安装netcat

首先在Linux服务器用YUM安装nc工具,nc命令式netcat命令的简称,它式用来设置路由器,我们可以利用它向某个端口发送数据.执行如下命令:

```shell
nc -lk 9999
```

#### 4.1.2.2 编写Spark Streaming程序

##### 导入Spark Streaming的pom配置

```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-streaming_2.11</artifactId>
    <version>2.2.0</version>
</dependency>
```

##### Java语言

```java
/**
 * @author JackieZhu
 * @date 2018-08-24 16:39:39
 * @description 我是传智JackieZhu, 我是不一样的JackieZhu
 * 使用Spark Streaming读取Socket数据并实现词频统计
 */
public class SparkStreamingFromSocketWithJava {
    public static void main(String[] args) throws InterruptedException {
        //1:创建一个SparkConf对象
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("SparkStreamingFromSocketWithJava");
        //2:根据SparkConf对象创建JavaStreamingContext对象,并指定每个批次处理的时间间隔
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.sparkContext().setLogLevel("WARN");
        //3:注册一个监听的ip地址和端口,用来收集数据
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("spark-node01.itheima.com", 9999);
        //4:切分每一行记录
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        //5:每个单词记为1
        JavaPairDStream<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        //6:对所有的单词进行聚合输出
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((a, b) -> a + b);
        //7:打印数据
        wordCounts.print();
        //8:开启流式计算
        jssc.start();
        //Wait for the computation to terminate 等待计算终止
        jssc.awaitTermination();
    }
}
```

**注意:由于使用的是本地模式"local[2]"所以可以直接在本地运行该程序要指定并行度，如在本地运行设置setMaster("local[2]")，相当于启动两个线程，一个给receiver，一个给computer。如果是在集群中运行，必须要求集群中可用core数大于1。**

##### Scala语言

```scala
object SparkStreamingFromSocketWithScala {
  def main(args: Array[String]): Unit = {
    //1:创建SparkConf对象,并指定主机名称设置AppName
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    //2:根据SparkConf对象创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.sparkContext.setLogLevel("WARN")
    //3:读取socket数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("spark-node01.itheima.com",9999)
    //4:flatMap
    val words: DStream[String] = lines.flatMap(line=>line.split(" "))
    //5:map操作
    val pairs: DStream[(String, Int)] = words.map(word=>(word,1))
    //6:对pairs进行聚合统计
    val wordCounts: DStream[(String, Int)] = pairs.reduceByKey((a, b)=>a+b)
    //打印结果
    wordCounts.print()
    //开启流式计算
    ssc.start()
    ssc.awaitTermination()
  }
}
```

## 4.2 Spark Streaming接受socket数据,实现所有批次单词计数结果累加

 	在上面的那个案例中存在这样一个问题：每个批次的单词次数都被正确的统计出来，但是结果不能累加！如果将所有批次的结果数据进行累加使用updateStateByKey(func)来更新状态.

### 4.2.1 架构图

![](img\Spark Streaming读取Socket数据.png)

### 4.2.2 代码实现

#### 4.2.2.1 Java版本

```java
public class SparkStreamingFromSocketTotalWithJava {
    public static void main(String[] args) throws InterruptedException {
        //1:创建一个SparkConf对象
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("SparkStreamingFromSocketTotalWithJava");
        //2:根据SparkConf对象创建JavaStreamingContext对象,并指定每个批次处理的时间间隔
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.sparkContext().setLogLevel("WARN");
        jssc.checkpoint("./ck");
        //3:注册一个监听的ip地址和端口,用来收集数据
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("spark-node01.itheima.com", 9999);
        //4:切分每一行记录
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        //5:每个单词记为1
        JavaPairDStream<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        //6:对所有的单词进行聚合输出
        JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey((values,state)->{
            Integer updateValue = 0;
            if(state.isPresent()){
                updateValue = state.get();
            }
            for (Integer value : values) {
                updateValue += value;
            }
            return Optional.of(updateValue);
        });
        //7:打印数据
        wordCounts.print();
        //8:开启流式计算
        jssc.start();
        //Wait for the computation to terminate 等待计算终止
        jssc.awaitTermination();
    }
}
```

#### 4.2.2.2 Scala版本

```scala
object SparkStreamingFromSocketTotalWithScala {

  def updateFunction(newValues: Seq[Int],  historyValue: Option[Int]): Option[Int] = {
    val newCount =historyValue.getOrElse(0)+newValues.sum
    Option.apply(newCount)
  }
  def main(args: Array[String]): Unit = {
    //1:创建SparkConf对象
    val conf:SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SparkStreamingFromSocketTotalWithScala")
    //2:根据SparkConf对象创建StreamingContext对象,并制定时间间隔
    val ssc:StreamingContext = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("WARN")
    ssc.checkpoint("./ckscala")
    //3:注册监听端口,指定主机和端口
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("spark-node01.itheima.com",9999)
    //4:切分每一行数据
    val words: DStream[String] = lines.flatMap(line=>line.split(" "))
    //5:单词没出现一次就记为1
    val pairs: DStream[(String, Int)] = words.map(word=>(word,1))
    //6:统计单词出现的次数
    val wordCounts: DStream[(String, Int)] = pairs.updateStateByKey(updateFunction)
    //7:打印结果
    wordCounts.print()
    //8:开启流式计算
    ssc.start()
    ssc.awaitTermination()
  }
}
```

通过函数updateStateByKey实现。根据key的当前值和key的之前批次值，对key进行更新，返回一个新状态的DStream.

## 4.3 SparkStreaming开窗函数reduceByKeyAndWindow

使用Spark Streaming的开窗函数reduceByKeyAndWindow,实现单词统计计数.

### 4.3.1 架构图

![](img\Spark Streaming读取Socket数据.png)

### 4.3.2 代码实现

#### 4.3.2.1 Java版本

```java
public class SparkStreamingWindowWithJava {
    public static void main(String[] args) throws InterruptedException {
        //1:创建一个SparkConf对象
        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("SparkStreamingWindowWithJava");
        //2:根据SparkConf对象创建JavaStreamingContext对象,并指定每个批次处理的时间间隔
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.sparkContext().setLogLevel("WARN");
        //3:注册一个监听的ip地址和端口,用来收集数据
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("spark-node01.itheima.com", 9999);
        //4:切分每一行记录
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        //5:每个单词记为1
        JavaPairDStream<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        //6:进行开窗函数的计算
        //reduceByKeyAndWindow 参数说明
        //windowDuration:表示window框住的时间长度，如本例5秒切分一次RDD，框10秒，就会保留最近2次切分的RDD
        //slideDuration:表示window滑动的时间长度，即每隔多久执行本计算
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKeyAndWindow((a, b) -> a + b, Durations.seconds(10), Durations.seconds(5));
        //7:将结果打印到控制台
        wordCounts.print();
        //8:开启流式计算
        jssc.start();
        //9:等待终止退出
        jssc.awaitTermination();
    }
}
```

#### 4.3.2.2 Scala版本

```scala
object SparkStreamingWindowWithScala {
  def main(args: Array[String]): Unit = {
    //1:创建SparkConf对象
    val conf:SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SparkStreamingWindowWithScala")
    //2:根据SparkConf对象创建StreamingContext对象,并制定时间间隔
    val ssc:StreamingContext = new StreamingContext(conf, Seconds(5))
    //3:注册监听端口,指定主机和端口
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("spark-node01.itheima.com",9999)
    //4:切分每一行
    val words: DStream[String] = lines.flatMap(line=>line.split(" "))
    //5:单词出现1次就记为1(word,1)
    val pairs: DStream[(String, Int)] = words.map(word=>(word,1))
    //6:对pairs进行开窗统计
    //参数说明
    //reduceFunc:集合函数
    //windowDuration:窗口的宽度,如本例5秒切分一次RDD，框10秒，就会保留最近2次切分的RDD
    //slideDuration:表示window滑动的时间长度，即每隔多久执行本计算
    val result: DStream[(String, Int)] = pairs.reduceByKeyAndWindow((a:Int, b:Int)=>a+b,Seconds(10),Seconds(5))
    //7:打印结果
    result.print()
    //8:开启流式计算
    ssc.start()
    //9:等待终止退出
    ssc.awaitTermination()
  }
}
```

现象：Spark  Streaming每隔5s计算一次当前在窗口大小为10s内的数据，然后将结果数据输出。窗口的宽度和滑动长度都应该是批次处理时间的整数倍.

## 4.3 实现一定时间内的热门词汇搜索

### 4.3.1 架构图

![](img\Spark Streaming读取Socket数据.png)

### 4.3.2 代码实现

#### 4.3.2.1 Java版本

```java
public class SparkStreamingWindowHotWordsWithJava {
    public static void main(String[] args) throws InterruptedException {
        //1:创建一个SparkConf对象
        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("SparkStreamingWindowWithJava");
        //2:根据SparkConf对象创建JavaStreamingContext对象,并指定每个批次处理的时间间隔
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.sparkContext().setLogLevel("WARN");
        //3:注册一个监听的ip地址和端口,用来收集数据
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("spark-node01.itheima.com", 9999);
        //4:切分每一行记录
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        //5:每个单词记为1
        JavaPairDStream<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        //6:进行开窗函数的计算
        //reduceByKeyAndWindow 参数说明
        //windowDuration:表示window框住的时间长度，如本例5秒切分一次RDD，框10秒，就会保留最近2次切分的RDD
        //slideDuration:表示window滑动的时间长度，即每隔多久执行本计算
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKeyAndWindow((a, b) -> a + b, Durations.seconds(10), Durations.seconds(5));
        //7:对wordCounts进行排序
        JavaPairDStream<String, Integer> finalDStream = wordCounts.transformToPair(wordCount->{
            //将统计的每一个单词进行位置替换
            JavaPairRDD<Integer, String> reverseWordCountRDD = wordCount.mapToPair(tuple2 -> new Tuple2<Integer, String>(tuple2._2, tuple2._1));
            //对反转之后的rdd进行排序
            JavaPairRDD<Integer, String> sortedWordCountRDD = reverseWordCountRDD.sortByKey(false);
            //对排序之后的rdd进行再次反转
            JavaPairRDD<String, Integer> reverSortedWordCount = sortedWordCountRDD.mapToPair(tuple2 -> new Tuple2<String, Integer>(tuple2._2, tuple2._1));
            //对排序之后的结果取出前三位
            List<Tuple2<String, Integer>> take = reverSortedWordCount.take(3);
            //打印前三位
            for (Tuple2<String, Integer> tuple2 : take) {
                System.out.println(tuple2._1+"............."+tuple2._2);
            }
            return reverSortedWordCount;
        });
        //7:对wordCounts进行排序
        /*JavaPairDStream<String, Integer> finalDStream = wordCounts.transformToPair(new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
            @Override
            public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> wordCount) throws Exception {
                JavaPairRDD<Integer, String> reverseWordCount = wordCount.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                        return new Tuple2<Integer, String>(tuple2._2, tuple2._1);
                    }
                });
                //对反转的rdd进行排序
                JavaPairRDD<Integer, String> sortedRdd = reverseWordCount.sortByKey(false);
                //将排序之后的RDD再次进行反转
                JavaPairRDD<String, Integer> finalResult = sortedRdd.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) throws Exception {
                        return new Tuple2<>(tuple2._2, tuple2._1);
                    }
                });
                //使用take取出前三位
                List<Tuple2<String, Integer>> hotWords = finalResult.take(3);
                for (Tuple2<String, Integer> hotWord : hotWords) {
                    System.out.println(hotWord._1 + "....." + hotWo	rd._2);
                }
                return finalResult;
            }
        });*/
        //8:打印DStream
        finalDStream.print();
        //9:开启流式计算
        jssc.start();
        //10:等待终止退出
        jssc.awaitTermination();
    }
}
```

#### 4.3.2.2 Scala版本

```scala
object SparkStreamingWindowHotWordsWithScala {
  def main(args: Array[String]): Unit = {
    //1:创建SparkConf对象
    val conf:SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SparkStreamingWindowHotWordsWithScala")
    //2:根据SparkConf对象创建StreamingContext对象,并制定时间间隔
    val ssc:StreamingContext = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("WARN")
    //3:注册监听端口,指定主机和端口
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("spark-node01.itheima.com",9999)
    //4:切分每一行
    val words: DStream[String] = lines.flatMap(line=>line.split(" "))
    //5:单词出现1次就记为1(word,1)
    val pairs: DStream[(String, Int)] = words.map(word=>(word,1))
    //6:对pairs进行开窗统计
    //参数说明
    //reduceFunc:集合函数
    //windowDuration:窗口的宽度,如本例5秒切分一次RDD，框10秒，就会保留最近2次切分的RDD
    //slideDuration:表示window滑动的时间长度，即每隔多久执行本计算
    val result: DStream[(String, Int)] = pairs.reduceByKeyAndWindow((a:Int, b:Int)=>a+b,Seconds(10),Seconds(5))
    //7:对结果进行transform转换
    val data: DStream[(String, Int)] = result.transform(rdd => {
      //对结果进行排序,降序排序
      val sortedRDD: RDD[(String, Int)] = rdd.sortBy(t => t._2, false)
      //对排序后的结果取出前三位
      val takes: Array[(String, Int)] = sortedRDD.take(3)
      println("--------------print top 3 begin--------------")
      takes.foreach(println)
      println("--------------print top 3 end--------------")
      sortedRDD
    })
    //8:打印结果
    data.print()
    //9:开启流式计算
    ssc.start()
    //10:等待退出终止
    ssc.awaitTermination()
  }
}
```

# 第五章 Spark Streaming整合Flume实战

 	Flume作为日志实时采集的框架，可以与Spark Streaming实时处理框架进行对接，Flume实时产生数据，sparkStreaming做实时处理。 Spark Streaming对接FlumeNG有两种方式，一种是FlumeNG将消息Push推给Spark Streaming，还有一种是Spark Streaming从Flume 中Poll拉取数据。

## 5.1  Poll方式

### 5.1.1 安装Flume

![](F:\Spark备课教案\day04\img\flume1.6.png)

Spark2.2.0需要的Flume版本是1.6.0以上.

安装过程略

### 5.1.2 添加依赖包

将spark-streaming-flume-sink_2.11-2.2.0.jar这个jar包放入$FLUME_HOME/lib目录下.

![](img\spark-streaming-flume-sink.png)

### 5.1.3 修改$FLUME_HOME/lib下的scala依赖包版本

从spark安装目录的jars文件夹下找到scala-library-2.11.8.jar包，替换掉flume的lib目录下自带的scala-library-2.10.5.jar。

```shell
cp /opt/modules/spark2.2.0/scala-library-2.11.8.jar /opt/modules/flume-1.8.0/lib/
```

![](img\scala-library-2.11.8.png)

### 5.1.4 编写flume-poll.conf配置文件

写flume的agent，注意既然是拉取的方式，那么flume向自己所在的机器上产数据就行

```bash
a1.sources = r1
a1.sinks = k1
a1.channels = c1
#source
a1.sources.r1.channels = c1
a1.sources.r1.type = spooldir
a1.sources.r1.spoolDir = /opt/datas/flumedata
a1.sources.r1.fileHeader = true
#channel
a1.channels.c1.type =memory
a1.channels.c1.capacity = 20000
a1.channels.c1.transactionCapacity=5000
#sinks
a1.sinks.k1.channel = c1
a1.sinks.k1.type = org.apache.spark.streaming.flume.sink.SparkSink
a1.sinks.k1.hostname=spark-node01.itheima.com
a1.sinks.k1.port = 8888
a1.sinks.k1.batchSize= 2000
```

### 5.1.5 执行日志采集命令

```shell
./bin/flume-ng agent -n a1 -c /opt/modules/flume-1.8.0/conf -f /opt/modules/flume-1.8.0/conf/flume-poll.conf -Dflume.root.logger=INFO,console
```

### 5.1.6 测试

略

### 5.1.7 代码实现

#### 5.1.7.1 添加pom依赖

```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-streaming-flume_2.11</artifactId>
    <version>2.2.0</version>
</dependency>
```

#### 5.1.7.2 编写程序

##### Java版本

```java
@SuppressWarnings("all")
public class SparkStreamingPollFlumeWithJava {
    public static void main(String[] args) throws InterruptedException {
        //1:创建一个SparkConf对象
        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("SparkStreamingPollFlume");
        //2:根据SparkConf对象创建JavaStreamingContext对象,并指定每个批次处理的时间间隔
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.checkpoint("./flume");
        jssc.sparkContext().setLogLevel("WARN");
        //3:创建一个PollStream
        JavaReceiverInputDStream<SparkFlumeEvent> pollingStream =
                FlumeUtils.createPollingStream(jssc, "spark-node01.itheima.com", 8888);
        //4:获取flume中event的body {"headers":xxxxxx,"body":xxxxx}
        JavaDStream<String> data = pollingStream.map(x -> new String(x.event().getBody().array()));
        //5:切分每一行数据
        JavaDStream<String> words = data.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        //6:每个单词出现一次记为1
        JavaPairDStream<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        //7:相同单词出现的次数进行累加
        JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey((values, state) -> {
            Integer updateValue = 0;
            if (state.isPresent()) {
                updateValue = state.get();
            }
            for (Integer value : values) {
                updateValue += value;
            }
            return Optional.of(updateValue);
        });
        //8:打印输出
        wordCounts.print();
        //9:开启流式计算
        jssc.start();
        //10:等待退出终止
        jssc.awaitTermination();

    }
}
```

##### Scala版本

```scala
object SparkStreamingPollFlumeWithScala {

  def updateFunction(newValues: Seq[Int], histroyValues: Option[Int]): Option[Int] = {
    val newCount = histroyValues.getOrElse(0)+newValues.sum
    Option.apply(newCount)
  }

  def main(args: Array[String]): Unit = {
    //1:创建SparkConf对象
    val conf:SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SparkStreamingPollFlumeWithScala")
    //2:根据SparkConf对象创建StreamingContext对象,并指定时间间隔
    val ssc:StreamingContext = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("WARN")
    ssc.checkpoint("./flumescala")
    //3:创建一个PollStream
    val pollingStream: ReceiverInputDStream[SparkFlumeEvent] =
      FlumeUtils.createPollingStream(ssc,"spark-node01.itheima.com",8888)
    //4:获取flume中event的body{"headers":xxxxxxx,"body":xxxxx}
    val lines: DStream[String] = pollingStream.map(x=>new String(x.event.getBody.array()))
    //5:切分每一行数据
    val words: DStream[String] = lines.flatMap(line=>line.split(" "))
    //6:单词没出现一次就统计为1
    val pairs: DStream[(String, Int)] = words.map(word=>(word,1))
    //7:调用状态更新函数,统计所有批次的单词
    val wordCounts: DStream[(String, Int)] = pairs.updateStateByKey(updateFunction)
    //8:打印结果
    wordCounts.print()
    //9:开启流式计算
    ssc.start()
    //10,等待终止退出
    ssc.awaitTermination()
  }
}
```

## 5.2 Push方式

### 5.2.1 编写flume-push.conf配置文件

```bash
#push mode
a1.sources = r1
a1.sinks = k1
a1.channels = c1
#source
a1.sources.r1.channels = c1
a1.sources.r1.type = spooldir
a1.sources.r1.spoolDir = /opt/datas/flumedata
a1.sources.r1.fileHeader = true
#channel
a1.channels.c1.type =memory
a1.channels.c1.capacity = 20000
a1.channels.c1.transactionCapacity=5000
#sinks
a1.sinks.k1.channel = c1
a1.sinks.k1.type = avro
#注意配置文件中指明的hostname和port是spark应用程序所在服务器的ip地址和端口。
a1.sinks.k1.hostname=172.16.0.112
a1.sinks.k1.port = 8888
a1.sinks.k1.batchSize= 2000
```

### 5.2.2 flume-ng启动命令

```shell
./bin/flume-ng agent -n a1 -c /opt/modules/flume-1.8.0/conf -f /opt/modules/flume-1.8.0/conf/flume-push.conf -Dflume.root.logger=INFO,console
```

### 5.2.3 代码实现

#### 5.2.3.1 Java实现

```java
public class SparkStreamingPushFlumeWithJava {
    public static void main(String[] args) throws InterruptedException {
        //1:创建一个SparkConf对象
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("SparkStreamingPollFlume");
        //2:根据SparkConf对象创建JavaStreamingContext对象,并指定每个批次处理的时间间隔
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.checkpoint("./flume_push");
        jssc.sparkContext().setLogLevel("WARN");
        //3:创建JavaReceiverInputDStream,根据FlumeUtils.createStream()方法
        JavaReceiverInputDStream<SparkFlumeEvent> flumeStream =
                FlumeUtils.createStream(jssc, "172.16.0.112", 8888, StorageLevel.MEMORY_AND_DISK());
        //4:获取flume中数据，数据存在event的body中，转化为String
        JavaDStream<String> lineStream = flumeStream.map(x -> new String(x.event().getBody().array()));
        //5:对每一行数据进行切分
        JavaDStream<String> words = lineStream.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        //6:单词每出现一次就记为1(word,1)
        JavaPairDStream<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        //7:对单词进行统计输出
        JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey((values, state) -> {
            Integer updateValue = 0;
            if (state.isPresent()) {
                updateValue = state.get();
            }
            for (Integer value : values) {
                updateValue += value;
            }
            return Optional.of(updateValue);
        });
        //8:将结果打印到控制台
        wordCounts.print();
        //9:开启流式计算
        jssc.start();
        //10:等待终止退出
        jssc.awaitTermination();
    }
}
```

#### 5.2.3.2 Scala实现

```scala
object SparkStreamingPushFlumeWithScala {
  def main(args: Array[String]): Unit = {
    //1:创建SparkConf对象
    val conf:SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SparkStreamingPushFlumeWithScala")
    //2:根据SparkConf对象创建StreamingContext对象,并指定时间间隔
    val ssc:StreamingContext = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("WARN")
    ssc.checkpoint("./flumescala")
    //3:创建一个RecevierDStream
    val pushStreaming: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createStream(ssc,"192.168.1.25",8888)
    //4:获取flume中event的body{"headers":xxxxxxx,"body":xxxxx}
    val lines: DStream[String] = pushStreaming.map(x=>new String(x.event.getBody.array()))
    //5:切分每一行数据
    val words: DStream[String] = lines.flatMap(line=>line.split(" "))
    //6:单词没出现一次就统计为1
    val pairs: DStream[(String, Int)] = words.map(word=>(word,1))
    //7:调用状态更新函数,统计所有批次的单词
    val wordCounts: DStream[(String, Int)] = pairs.updateStateByKey(updateFunction)
    //8:打印结果
    wordCounts.print()
    //9:开启流式计算
    ssc.start()
    //10,等待终止退出
    ssc.awaitTermination()
  }
  def updateFunction(newValues: Seq[Int], histroyValues: Option[Int]): Option[Int] = {
    val newCount = histroyValues.getOrElse(0)+newValues.sum
    Option.apply(newCount)
  }
}
```

**注意:push模式下需要先启动Spark应用程序,然后启动日志采集**

# 第六章 Spark Streaming整合kafka实战

 	kafka作为一个实时的分布式消息队列，实时的生产和消费消息，这里我们可以利用SparkStreaming实时地读取kafka中的数据，然后进行相关计算。

 	在Spark1.3版本后，KafkaUtils里面提供了两个创建dstream的方法，一种为KafkaUtils.createDstream，另一种为KafkaUtils.createDirectStream。

## 6.1 KafkaUtils.createDstream方式

### 6.1.1 简介

KafkaUtils.createDstream(ssc,[zk], [group id], [per-topic,partitions]) 使用了receivers接收器来接收数据，利用的是Kafka高层次的消费者api，对于所有的receivers接收到的数据将会保存在[Spark](http://lib.csdn.net/base/spark) executors中，然后通过Spark Streaming启动job来处理这些数据，默认会丢失，可启用WAL(预写)日志，它同步将接受到数据保存到分布式文件系统上比如HDFS。所以数据在出错的情况下可以恢复出来 。

![](img\SparkStreaming整合Kafka实战1.png)

### 6.1.2 实现步骤

- 创建一个receiver接收器来对kafka进行定时拉取数据，这里产生的dstream中rdd分区和kafka的topic分区不是一个概念,故如果增加特定主体分区数仅仅是增加一个receiver中消费topic的线程数，并没有增加spark的并行处理的数据量。
- 对于不同的group和topic可以使用多个receivers创建不同的DStream
-  如果启用了WAL(spark.streaming.receiver.writeAheadLog.enable=true)同时需要设置存储级别(默认StorageLevel.MEMORY_AND_DISK_SER_2)

### 6.1.3 准备工作

#### 6.1.3.1 添加pom依赖

```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-streaming-kafka-0-8_2.11</artifactId>
    <version>2.2.0</version>
</dependency>
```

#### 6.1.3.2 启动zookeeper集群

```shell
zkServer.sh start
```

#### 6.1.3.3 启动kafka集群

```
./bin/kafka-server-start.sh  config/server.properties
```

#### 6.1.3.4 创建topic

```shell
./bin/kafka-topics.sh --create --zookeeper spark-node01.itheima.com:2181,spark-node02.itheima.com:2181 spark-node03.itheima.com:2181 --replication-factor 1 --partitions 3 --topic kafka_spark
```

#### 6.1.3.5 向topic中生产数据

```shell
./bin/kafka-console-producer.sh --broker-list spark-node01.itheima.com:9092 --topic  kafka_spark
```

#### 6.1.3.6 代码实现

##### Java语言实现

```java
public class SparkStreamingKafkaReceiverWithJava {
    public static void main(String[] args) throws InterruptedException {
        //1:创建一个SparkConf对象
        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("SparkStreamingKafkaReceiverWithJava");
        //2:根据SparkConf对象创建JavaStreamingContext对象,并指定每个批次处理的时间间隔
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.checkpoint("./kafka_receiver");
        jssc.sparkContext().setLogLevel("WARN");
        //3:定义zk地址
        String zkQuorum = "spark-node01.itheima.com:2181,spark-node02.itheima.com:2181,spark-node03.itheima.com:2181";
        //4:定义消费者组
        String groupId = "spark_receiver1";
        //5:定义topic相关信息
        //这里的value并不是topic的分区数,它表示的topic中每一个分区被N个线程消费
        HashMap<String, Integer> topics = new HashMap<>();
        topics.put("kafka_spark",2);
        //通过kafkaUtils.createStream
        JavaPairReceiverInputDStream<String, String> stream = KafkaUtils.createStream(jssc, zkQuorum, groupId, topics);
        //获取topic中的数据
        JavaDStream<String> topicData = stream.map(x -> x._2);
        //切分每一行,每个单词记为1
        JavaPairDStream<Object, Integer> pairs = topicData.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).mapToPair(word -> new Tuple2<>(word, 1));
        //相同的单词进行累加
        JavaPairDStream<Object, Integer> wordCounts = pairs.reduceByKey((a, b) -> a + b);
        //打印输出
        wordCounts.print();
        //开启计算
        jssc.start();
        //等待终止退出
        jssc.awaitTermination();
    }
}
```

##### Scala语言实现

```scala
/**
  * @author JackieZhu
  * @note 2018-08-27 08:41:18
  * @desc 我是传智JackieZhu 我是不一样的JackieZhu
  */
object SparkStreamingKafkaReceiverWithScala {
  def main(args: Array[String]): Unit = {
    //1:创建SparkConf对象
    val conf:SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SparkStreamingKafkaReceiverWithScala")
    //2:根据SparkConf对象创建StreamingContext对象,并指定时间间隔
    val ssc:StreamingContext = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("WARN")
    ssc.checkpoint("./kafka_receiver")
    //定义zookeeper的地址
    val zkQuorum = "spark-node01.itheima.com:2181,spark-node02.itheima.com:2181,spark-node03.itheima.com:2181"
    //定义消费组id
    val groupId = "spark-receiver_scala"
    //定义topic相关信息
    //这里的value并不是topic的分区数,它表示的topic中每一个分区被N个线程消费
    val topics = Map("kafka_spark"->2)
    val receiverDstream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zkQuorum,groupId,topics)
    //获取topic中的数据
    val lines: DStream[String] = receiverDstream.map(x=>x._2)
    //切分每一行数据,单词每出现一次记为1
    val pairs: DStream[(String, Int)] = lines.flatMap(line=>line.split(" ")).map(word=>(word,1))
    //对相同的单词进行累加统计
    val wordCounts: DStream[(String, Int)] = pairs.reduceByKey((a, b)=>a+b)
    //将结果进行打印
    wordCounts.print()
    //开启流式计算
    ssc.start()
    //等待退出终止
    ssc.awaitTermination()
  }
}
```

### 6.1.4 总结

通过这种方式实现，刚开始的时候系统正常运行，没有发现问题，但是如果系统异常重新启动sparkstreaming程序后，发现程序会重复处理已经处理过的数据，这种基于receiver的方式，是使用Kafka的高级API，topic的offset偏移量在ZooKeeper中。这是消费Kafka数据的传统方式。这种方式配合着WAL机制可以保证数据零丢失的高可靠性，但是却无法保证数据只被处理一次，可能会处理两次。因为Spark和ZooKeeper之间可能是不同步的。官方现在也已经不推荐这种整合方式，我们使用官网推荐的第二种方式kafkaUtils的createDirectStream()方式。

## 6.2 KafkaUtils.createDirectStream方式

这种方式不同于Receiver接收数据，它定期地从kafka的topic下对应的partition中查询最新的偏移量，再根据偏移量范围在每个batch里面处理数据，Spark通过调用kafka简单的消费者Api（低级api）读取一定范围的数据

![](img\SparkStreaming整合Kafka实战2.png)

相比较于Receiver方式有几个优点

- 简化并行
  - 不需要创建多个kafka输入流，然后union它们，sparkStreaming将会创建和kafka分区数相同的rdd的分区数，而且会从kafka中并行读取数据，spark中RDD的分区数和kafka中的topic分区数是一一对应的关系。
- 高效
  - 第一种实现数据的零丢失是将数据预先保存在WAL中，会复制一遍数据，会导致数据被拷贝两次，第一次是接受kafka中topic的数据，另一次是写到WAL中。而没有receiver的这种方式消除了这个问题。 
- 恰好一次语义(Exactly-once-semantics)
  - Receiver读取kafka数据是通过kafka高层次api把偏移量写入zookeeper中，虽然这种方法可以通过数据保存在WAL中保证数据不丢失，但是可能会因为sparkStreaming和ZK中保存的偏移量不一致而导致数据被消费了多次。EOS通过实现kafka低层次api，偏移量仅仅被ssc保存在checkpoint中，消除了zk和ssc偏移量不一致的问题。缺点是无法使用基于zookeeper的kafka监控工具。

### 6.2.1 代码实现

#### 6.2.1.1 Java语言实现

```java
public class SparkStreamingKafkaDirectWithJava {
    public static void main(String[] args) throws InterruptedException {
        //1:创建一个SparkConf对象
        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("SparkStreamingKafkaDirectWithJava");
        //2:根据SparkConf对象创建JavaStreamingContext对象,并指定每个批次处理的时间间隔
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.checkpoint("./kafka_direct");
        jssc.sparkContext().setLogLevel("WARN");
        //3:定义kafkaParams
        Map<String,String> kafkaParams = new HashMap();
        kafkaParams.put("metadata.broker.list","spark-node01.itheima.com:9092");
        kafkaParams.put("group.id","kafka_direct");
        //4:定义topic
        HashSet<String> topics = new HashSet<>();
        topics.add("kafka_spark");
        //通过 KafkaUtils.createDirectStream接受kafka数据，这里采用是kafka低级api偏移量不受zk管理
        JavaPairInputDStream<String, String> directStream = KafkaUtils.createDirectStream(jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams, topics);
        //获取topic中的数据
        JavaDStream<String> lines = directStream.map(x -> x._2);
        //切分每一行,并将单词没出现一次记为1
        JavaPairDStream<String, Integer> pairs = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).mapToPair(word -> new Tuple2<>(word, 1));
        //对单词进行聚合输出
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((a, b) -> a + b);
        //对结果进行打印
        wordCounts.print();
        //开启流式计算
        jssc.start();
        jssc.awaitTermination();
    }
}
```

#### 6.2.1.2 Scala语言实现

```scala
object SparkStreamingKafkaDirectWithScala {
  def main(args: Array[String]): Unit = {
    //1:创建SparkConf对象
    val conf:SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SparkStreamingKafkaDirectWithScala")
    //2:根据SparkConf对象创建StreamingContext对象,并指定时间间隔
    val ssc:StreamingContext = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("WARN")
    ssc.checkpoint("./kafka_Direct")
    //6、通过 KafkaUtils.createDirectStream接受kafka数据，这里采用是kafka低级api偏移量不受zk管理
    val directDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams =
      Map("metadata.broker.list" -> "spark-node01.itheima.com:9092", "group.id" -> "kafka_direct"),
      topics = Set("kafka_spark"))
    directDStream
    //获取Kafka中的数据
    val lines: DStream[String] = directDStream.map(x=>x._2)
    //切分每一行数据
    val words: DStream[String] = lines.flatMap(line=>line.split(" "))
    //单词没出现一次记为1
    val pairs: DStream[(String, Int)] = words.map(word=>(word,1))
    //相同的单词进行聚合输出
    val wordCounts: DStream[(String, Int)] = pairs.reduceByKey((a, b)=>a+b)
    //打印
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
```

### 6.2.2 效果

略















