# spark是什么

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


# spark生态圈


Spark生态圈以Spark Core为核心，从HDFS、Amazon S3和HBase等持久层读取数据，以MESS、YARN和自身携带的Standalone为资源管理器调度Job完成Spark应用程序的计算。 这些应用程序可以来自于不同的组件，如Spark的批处理、Spark Streaming的实时处理应用、Spark SQL的即席查询、MLlib的机器学习、GraphX的图处理等等。

Spark生态圈也称为BDAS（伯克利数据分析栈），是伯克利APMLab实验室打造的用于大数据的分析的一套开源软件栈，力图在算法（Algorithms）、机器（Machines）、人（People）之间通过大规模集成来展现大数据应用的一个平台。伯克利AMPLab运用大数据、云计算、通信等各种资源以及各种灵活的技术方案，对海量不透明的数据进行甄别并转化为有用的信息，以供人们更好的理解世界。该生态圈已经涉及到机器学习、数据挖掘、数据库、信息检索、自然语言处理和语音识别等多个领域。

Spark是整个BDAS的核心组件，也包括了冉冉升起的分布式内存系统Alluxio(Tachyon)，当然还包括著名的资源管理的开源软件Mesos。可以说Amplab最近几年引领了大数据发展的技术创新的浪潮。

![the Berkeley Data Analytics Stack](https://github.com/jimmy-src/spark-learning/blob/master/Public%20File/image/BDAS.jpg)

> 1,  Spark Core

* 提供了有向无环图（DAG）的分布式并行计算框架，并提供Cache机制来支持多次迭代计算或者数据共享，大大减少迭代计算之间读取数据局的开销，这对于需要进行多次迭代的数据挖掘和分析性能有很大提升
* 在Spark中引入了RDD (Resilient Distributed Dataset) 的抽象，它是分布在一组节点中的只读对象集合，这些集合是弹性的，如果数据集一部分丢失，则可以根据“血统”对它们进行重建，保证了数据的高容错性；
* 移动计算而非移动数据，RDD Partition可以就近读取分布式文件系统中的数据块到各个节点内存中进行计算
* 使用多线程池模型来减少task启动开稍
* 采用容错的、高可伸缩性的akka作为通讯框架

> 2, Spark Streaming

SparkStreaming是一个对实时数据流进行高通量、容错处理的流式处理系统,可以对多种数据源（如Kdfka、Flume、Twitter、Zero和TCP 套接字）进行类似Map、Reduce和Join等复杂操作，并将结果保存到外部文件系统、数据库或应用到实时仪表盘。

* Spark Streaming是将流式计算分解成一系列短小的批处理作业。这里的批处理引擎是Spark，也就是把Spark Streaming的输入数据按照batch size（如1秒）分成一段一段的数据（Discretized Stream），每一段数据都转换成Spark中的RDD（Resilient Distributed Dataset），然后将Spark Streaming中对DStream的Transformation操作变为针对Spark中对RDD的Transformation操作，将RDD经过操作变成中间结果保存在内存中。整个流式计算根据业务的需求可以对中间的结果进行叠加，或者存储到外部设备

![](https://github.com/jimmy-src/spark-learning/blob/master/Public%20File/image/Spark%20Streaming.png)

* 容错性：对于流式计算来说，容错性至关重要。首先我们要明确一下Spark中RDD的容错机制。每一个RDD都是一个不可变的分布式可重算的数据集，其记录着确定性的操作继承关系（lineage），所以只要输入数据是可容错的，那么任意一个RDD的分区（Partition）出错或不可用，都是可以利用原始输入数据通过转换操作而重新算出的。

![](https://github.com/jimmy-src/spark-learning/blob/master/Public%20File/image/spark%20Streaming%20fault%20tolerance.png)

* 对于Spark Streaming来说，其RDD的传承关系如图3所示，图中的每一个椭圆形表示一个RDD，椭圆形中的每个圆形代表一个RDD中的一个Partition，图中的每一列的多个RDD表示一个DStream（图中有三个DStream），而每一行最后一个RDD则表示每一个Batch Size所产生的中间结果RDD。我们可以看到图中的每一个RDD都是通过lineage相连接的，由于Spark Streaming输入数据可以来自于磁盘，例如HDFS（多份拷贝）或是来自于网络的数据流（Spark Streaming会将网络输入数据的每一个数据流拷贝两份到其他的机器）都能保证容错性。所以RDD中任意的Partition出错，都可以并行地在其他机器上将缺失的Partition计算出来。这个容错恢复方式比连续计算模型（如Storm）的效率更高。
* 实时性：对于实时性的讨论，会牵涉到流式处理框架的应用场景。Spark Streaming将流式计算分解成多个Spark Job，对于每一段数据的处理都会经过Spark DAG图分解，以及Spark的任务集的调度过程。对于目前版本的Spark Streaming而言，其最小的Batch Size的选取在0.5~2秒钟之间（Storm目前最小的延迟是100ms左右），所以Spark Streaming能够满足除对实时性要求非常高（如高频实时交易）之外的所有流式准实时计算场景。


> 3, Spark SQL

Shark是SparkSQL的前身，很早以前Hive可以说是SQL on Hadoop的唯一选择，负责将SQL编译成可扩展的MapReduce作业，鉴于Hive的性能以及与Spark的兼容，Shark项目由此而生。

Shark即Hive on Spark，本质上是通过Hive的HQL解析，把HQL翻译成Spark上的RDD操作，然后通过Hive的metadata获取数据库里的表信息，实际HDFS上的数据和文件，会由Shark获取并放到Spark上运算。Shark的最大特性就是快和与Hive的完全兼容，且可以在shell模式下使用rdd2sql()这样的API，把HQL得到的结果集，继续在scala环境下运算，支持自己编写简单的机器学习或简单分析处理函数，对HQL结果进一步分析计算。

在2014年7月1日的Spark Summit上，Databricks宣布终止对Shark的开发，将重点放到Spark SQL上。Databricks表示，Spark SQL将涵盖Shark的所有特性，用户可以从Shark 0.9进行无缝的升级。在会议上，Databricks表示，Shark更多是对Hive的改造，替换了Hive的物理执行引擎，因此会有一个很快的速度。然而，不容忽视的是，Shark继承了大量的Hive代码，因此给优化和维护带来了大量的麻烦。随着性能优化和先进分析整合的进一步加深，基于MapReduce设计的部分无疑成为了整个项目的瓶颈。因此，为了更好的发展，给用户提供一个更好的体验，Databricks宣布终止Shark项目，从而将更多的精力放到Spark SQL上。

Spark SQL允许开发人员直接处理RDD，同时也可查询例如在 Apache Hive上存在的外部数据。Spark SQL的一个重要特点是其能够统一处理关系表和RDD，使得开发人员可以轻松地使用SQL命令进行外部查询，同时进行更复杂的数据分析。除了Spark SQL外，Michael还谈到Catalyst优化框架，它允许Spark SQL自动修改查询方案，使SQL更有效地执行。

Spark SQL is Apache Spark's module for working with structured data.

* Integrated: Spark SQL lets you query structured data inside Spark programs, using either SQL or a familiar DataFrame API. Usable in Java, Scala, Python and R.

Apply functions to results of SQL queries:

    context = HiveContext(sc)
    results = context.sql(
      "SELECT * FROM people")
    names = results.map(lambda p: p.name)

* Uniform Data Access: DataFrames and SQL provide a common way to access a variety of data sources, including Hive, Avro, Parquet, ORC, JSON, and JDBC. You can even join data across these sources.

Query and join different data sources:

    context.jsonFile("s3n://...")
      .registerTempTable("json")
    results = context.sql(
      """SELECT * 
         FROM people
         JOIN json ...""")

* Hive Compatibility: Spark SQL reuses the Hive frontend and metastore, giving you full compatibility with existing Hive data, queries, and UDFs. Simply install it alongside Hive.

![](http://spark.apache.org/images/sql-hive-arch.png)

* Standard Connectivity: A server mode provides industry standard JDBC and ODBC connectivity for business intelligence tools.

![](http://spark.apache.org/images/jdbc.png)


> 4, Spark MLlib

MLlib is Apache Spark's scalable machine learning library.

* Ease of Use: MLlib fits into Spark's APIs and interoperates with NumPy in Python (as of Spark 0.9) and R libraries (as of Spark 1.5). You can use any Hadoop data source (e.g. HDFS, HBase, or local files), making it easy to plug into Hadoop workflows.

Calling MLlib in Python:
    
    data = spark.read.format("libsvm")\
      .load("hdfs://...")
    
    model = KMeans(k=10).fit(data)

* Performance: High-quality algorithms, 100x faster than MapReduce.

![](http://spark.apache.org/images/logistic-regression.png)

* Easy to Deploy: If you have a Hadoop 2 cluster, you can run Spark and MLlib without any pre-installation. Otherwise, Spark is easy to run standalone or on EC2 or Mesos. You can read from HDFS, HBase, or any Hadoop data source.

![](http://spark.apache.org/images/hadoop.jpg)

> 5, Spark GraphX

GraphX是Spark中用于图(e.g., Web-Graphs and Social Networks)和图并行计算(e.g., PageRank and Collaborative Filtering)的API,可以认为是GraphLab(C++)和Pregel(C++)在Spark(Scala)上的重写及优化，跟其他分布式图计算框架相比，GraphX最大的贡献是，在Spark之上提供一栈式数据解决方案，可以方便且高效地完成图计算的一整套流水作业。GraphX最先是伯克利AMPLAB的一个分布式图计算框架项目，后来整合到Spark中成为一个核心组件。

* Flexibility:GraphX unifies ETL, exploratory analysis, and iterative graph computation within a single system. You can view the same data as both graphs and collections, transform and join graphs with RDDs efficiently, and write custom iterative graph algorithms using the Pregel API.

Using GraphX in Scala:
    
    graph = Graph(vertices, edges)
    messages = spark.textFile("hdfs://...")
    graph2 = graph.joinVertices(messages) {
      (id, vertex, msg) => ...
    }

* Speed: GraphX competes on performance with the fastest graph systems while retaining Spark's flexibility, fault tolerance, and ease of use.

![End-to-end PageRank performance (20 iterations, 3.7B edges)](http://spark.apache.org/images/graphx-perf-comparison.png)

* Algorithms: 一个高度灵活的API，GraphX自带的各种图形算法

> 6, SparkR

SparkR是AMPLab发布的一个R开发包，为Apache Spark提供了轻量的前端。SparkR提供了Spark中弹性分布式数据集（RDD）的API，用户可以在集群上通过R shell交互性的运行job。

> 7, Alluxio

后面会用到Alluxio来进行优化，这里先简单介绍一下

由于Alluxio的设计以内存为中心，并且是数据访问的中心，所以Alluxio在大数据生态圈里占有独特地位，它居于大数据存储（如：Amazon S3，Apache HDFS和OpenStack Swift等）和大数据计算框架（如Spark，Hadoop Mapreduce）之间。对于用户应用和计算框架，无论其是否运行在相同的计算引擎之上，Alluxio都可以作为底层来支持数据的访问、快速存储，以及多任务的数据共享和本地化。因此，Alluxio可以为那些大数据应用提供一个数量级的加速，同时它还提供了通用的数据访问接口。对于底层存储系统，Alluxio连接了大数据应用和传统存储系统之间的间隔，并且重新定义了一组面向数据使用的工作负载程序。因Alluxio对应用屏蔽了底层存储系统的整合细节，所以任何底层存储系统都可以支撑运行在Alluxio之上的应用和框架。此外Alluxio可以挂载多种底层存储系统，所以它可以作为统一层为任意数量的不同数据源提供服务。

![](http://www.alluxio.org/docs/master/img/stack.png)

Alluxio的设计使用了单Master和多Worker的架构。从高层的概念理解，Alluxio可以被分为三个部分，Master，Worker和Client。Master和Worker一起组成了Alluxio的服务端，它们是系统管理员维护和管理的组件。Client通常是应用程序，如Spark或MapReduce作业，或者Alluxio的命令行用户。Alluxio用户一般只与Alluxio的Client组件进行交互。


# Spark & Hadoop

> Spark VS MapReduce 

MapReduce能够完成的各种离线批处理功能，以及常见算法（ 比如二次排序、 topn等），基于Spark RDD的核心编程， 都可以实现， 并且可以更好地、 更容易地实现。 而且基于Spark RDD编写的离线批处理程序， 运行速度是MapReduce的数倍， 速度上有非常明显的优势。

Spark相较于MapReduce速度快的最主要原因就在于， MapReduce的计算模型太死板， 必须是mapreduce模式， 有时候即使完成一些诸如过滤之类的操作， 也必须经过map-reduce过程， 这样就必须经过shuffle过程。 而MapReduce的shuffle过程是最消耗性能的， 因为shuffle中间的过程必须基于磁盘来读写。 而Spark的 shuffle虽然也要基于磁盘， 但是其大量transformation操作， 比如单纯的map或者filter等操作， 可以直接基于内 存进行pipeline操作， 速度性能自然大大提升。

但是Spark也有其劣势。 由于Spark基于内存进行计算， 虽然开发容易， 但是真正面对大数据的时候（ 比如 一次操作针对10亿以上级别） ， 在没有进行调优的情况下， 可能会出现各种各样的问题， 比如OOM内存溢出等 等。 导致Spark程序可能都无法完全运行起来， 就报错挂掉了， 而MapReduce即使是运行缓慢， 但是至少可以 慢慢运行完。 


> Spark SQL VS Hive 

Spark SQL实际上并不能完全替代Hive，因为Hive是一种基于HDFS的数据仓库， 并且提供了基于SQL模型的，针对存储了大数据的数据仓库，进行分布式交互查询的查询引擎。 

严格的来说， Spark SQL能够替代的， 是Hive的查询引擎， 而不是Hive本身， 实际上即使在生产环境下， Spark SQL 也是针对Hive数据仓库中的数据进行查询， Spark本身自己是不提供存储的，自然也不可能替代Hive作为数据仓库的这个 功能。 

Spark SQL的一个优点，相较于Hive查询引擎来说，就是速度快，同样的SQL语句，可能使用Hive的查询引擎，由于其底层基于MapReduce，必须经过shuffle过程走磁盘，因此速度是非常缓慢的。很多复杂的SQL语句， 在hive中执行都需 要一个小时以上的时间。 而Spark SQL由于其底层基于Spark自身的基于内存的特点， 因此速度达到了Hive查询引擎的数 倍以上。 

而Spark SQL相较于Hive的另外一个优点，就是支持大量不同的数据源，包括hive、json、parquet、jdbc等等。 此外，Spark SQL由于身处Spark技术堆栈内，也是基于RDD来工作，

因此可以与Spark的其他组件无缝整合使用，配合起来实现许多复杂的功能。比如Spark SQL支持可以直接针对hdfs文件执行sql语句！

> Spark Streaming VS Storm

Spark Streaming与Storm都可以用于进行实时流计算。 但是他们两者的区别是非常大的。 其中区别之一， 就是， Spark Streaming和Storm的计算模型完全不一样， Spark Streaming是基于RDD的， 因此需要将一小段时间内的， 比如1秒内的数据，收集起来， 作为一个RDD， 然后再针对这个batch的数据进行处理。 而Storm却可以做到每来一条数据， 都可以立即进行处理 和计算。 因此， Spark Streaming实际上严格意义上来说， 只能称作准实时的流计算框架； 而Storm是真正意义上的实时计算 框架。

此外， Storm支持的一项高级特性，是Spark Streaming暂时不具备的， 即Storm支持在分布式流式计算程序（ Topology） 在运行过程中，可以动态地调整并行度，从而动态提高并发处理能力。而Spark Streaming是无法动态调整并行度的。 

但是Spark Streaming也有其优点， 首先Spark Streaming由于是基于batch进行处理的， 因此相较于Storm基于单条数据 进行处理， 具有数倍甚至数十倍的吞吐量。 

此外， Spark Streaming由于也身处于Spark生态圈内， 因此Spark Streaming可以与Spark Core、 Spark SQL， 甚至是 Spark MLlib、 Spark GraphX进行无缝整合。 流式处理完的数据，可以立即进行各种map、 reduce转换操作， 可以立即使用 sql进行查询， 甚至可以立即使用machine learning或者图计算算法进行处理。 这种一站式的大数据处理功能和优势， 是Storm 无法匹敌的。 

因此， 综合上述来看， 通常在对实时性要求特别高， 而且实时数据量不稳定， 比如在白天有高峰期的情况下， 可以选择 使用Storm。 但是如果是对实时性要求一般， 允许1秒的准实时处理，而且不要求动态调整并行度的话， 选择Spark Streaming是更好的选择。 


参考资料：

    http://spark.apache.org/
    
    http://www.cnblogs.com/shishanyuan/archive/2015/08/04/4700615.html
    
    https://amplab.cs.berkeley.edu/software/
    
    http://www.alluxio.org/docs/1.4/cn/Architecture.html
    
    http://www.cnblogs.com/liuwei6/p/6587467.html