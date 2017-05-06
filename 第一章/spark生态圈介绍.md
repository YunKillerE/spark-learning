# spark简介

Spark，是一种"One Stack to rule them all"的大数据计算框架， 期望使用一个技术堆栈就 完美地解决大数据领域的各种计算任务。 Apache官方， 对Spark的定义就是： 通用的大数据快 速处理引擎。
Spark使用Spark RDD、 Spark SQL、 Spark Streaming、 MLlib、 GraphX成功解决了大数 据领域中， 离线批处理、 交互式查询、 实时流计算、 机器学习与图计算等最重要的任务和问题。
Spark除了一站式的特点之外， 另外一个最重要的特点， 就是基于内存进行计算， 从而让 它的速度可以达到MapReduce、 Hive的数倍甚至数十倍！
现在已经有很多大公司正在生产环境下深度地使用Spark作为大数据的计算框架， 包括 eBay、 Yahoo!、 BAT、 网易、 京东、 华为、 大众点评、 优酷土豆、 搜狗等等。
Spark同时也获得了多个世界顶级IT厂商的支持， 包括IBM、 Intel等。 

Spark使用Scala语言进行实现，它是一种面向对象、函数式编程语言，能够像操作本地集合对象一样轻松地操作分布式数据集（Scala 提供一个称为 Actor 的并行模型，其中Actor通过它的收件箱来发送和
接收非同步信息而不是共享数据，该方式被称为：Shared Nothing 模型）。在Spark官网上介绍，它具有运行速度快、易用性好、通用性强和随处运行等特点。


> 1, 运行数据快

Spark拥有DAG执行引擎，支持在内存中对数据进行迭代计算。官方提供的数据表明，如果数据由磁盘读取，速度是Hadoop MapReduce的10倍以上，如果数据从内存中读取，速度可以高达100多倍。

![](http://spark.apache.org/images/logistic-regression.png)

> 2, 易用性好

Write applications quickly in Java, Scala, Python, R.

Spark offers over 80 high-level operators that make it easy to build parallel apps. And you can use it interactively from the Scala, Python and R shells.

Word count in Spark's Python API:

    text_file = spark.textFile("hdfs://...") 
    text_file.flatMap(lambda line: line.split())
     .map(lambda word: (word, 1))
     .reduceByKey(lambda a, b: a+b)

> 3, 通用性强

Combine SQL, streaming, and complex analytics.

Spark powers a stack of libraries including SQL and DataFrames, MLlib for machine learning, GraphX, and Spark Streaming. You can combine these libraries seamlessly in the same application.

![](http://spark.apache.org/images/spark-stack.png)

> 4, Runs Everywhere

Spark runs on Hadoop, Mesos, standalone, or in the cloud. It can access diverse data sources including HDFS, Cassandra, HBase, and S3.

You can run Spark using its standalone cluster mode, on EC2, on Hadoop YARN, or on Apache Mesos. Access data in HDFS, Cassandra, HBase, Hive, Tachyon, and any Hadoop data source.

![](http://spark.apache.org/images/spark-runs-everywhere.png)


#spark生态圈

Spark生态圈以Spark Core为核心，从HDFS、Amazon S3和HBase等持久层读取数据，以MESS、YARN和自身携带的Standalone为资源管理器调度Job完成Spark应用程序的计算。 这些应用程序可以来自于不同的组件，如Spark的批处理、Spark Streaming的实时处理应用、Spark SQL的即席查询、MLlib的机器学习、GraphX的图处理等等。

BDAS乃是伯克利大学的AMPLab打造的用于大数据的分析的一套开源软件栈，这其中包括了这两年火的爆棚的Spark，也包括了冉冉升起的分布式内存系统Alluxio(Tachyon)，当然还包括著名的资源管理的开源软件Mesos。可以说Amplab最近几年引领了大数据发展的技术创新的浪潮。
























