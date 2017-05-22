# 1. Spark编程模型

## 术语定义

* 应用程序（Application）： 基于Spark的用户程序，包含了一个Driver Program 和集群中多个的Executor；

* 驱动程序（Driver Program）：运行Application的main()函数并且创建SparkContext，通常用SparkContext代表Driver Program；

* 执行单元（Executor）： 是为某Application运行在Worker Node上的一个进程，该进程负责运行Task，并且负责将数据存在内存或者磁盘上，每个Application都有各自独立的Executors；

* 集群管理程序（Cluster Manager）： 在集群上获取资源的外部服务(例如：Standalone、Mesos或Yarn)；

* 操作（Operation）：作用于RDD的各种操作分为Transformation和Action；


## 模型组成

Spark应用程序可分两部分：Driver部分和Executor部分

<img src="http://spark.apache.org/docs/latest/img/cluster-overview.png" width = "600" height = "400" alt="图片名称" align=center />

* Driver部分

Driver部分主要是对SparkContext进行配置、初始化以及关闭。初始化SparkContext是为了构建Spark应用程序的运行环境，在初始化SparkContext，要先导入一些Spark的类和隐式转换；在Executor部分运行完毕后，需要将SparkContext关闭。

* Executor部分

Spark应用程序的Executor部分是对数据的处理，数据分三种：

### 1. 原生数据
    
包含原生的输入数据和输出数据

*对于输入原生数据，Spark目前提供了两种：*

1. Scala集合数据集：如Array(1,2,3,4,5)，Spark使用parallelize方法转换成RDD

2. Hadoop数据集：Spark支持存储在hadoop上的文件和hadoop支持的其他文件系统，如本地文件、HBase、SequenceFile和Hadoop的输入格式。例如Spark使用txtFile方法可以将本地文件或HDFS文件转换成RDD

*对于输出数据，Spark除了支持以上两种数据，还支持scala标量*

1. 生成Scala标量数据，如count（返回RDD中元素的个数）、reduce、fold/aggregate；返回几个标量，如take（返回前几个元素）

2. 生成Scala集合数据集，如collect（把RDD中的所有元素倒入 Scala集合类型）、lookup（查找对应key的所有值）

3. 生成hadoop数据集，如saveAsTextFile、saveAsSequenceFile

### 2. RDD

RDD具体参考spark运行架构的详细描述，主要提供了Transformations和Actions两种算子,下面会讲到具体的算子

### 3. 共享变量

在Spark运行时，一个函数传递给RDD内的patition操作时，该函数所用到的变量在每个运算节点上都复制并维护了一份，并且各个节点之间不会相互影响。但是在Spark Application中，可能需要共享一些变量，提供Task或驱动程序使用。Spark提供了两种共享变量：

广播变量（Broadcast Variables）：可以缓存到各个节点的共享变量，通常为只读

* 广播变量缓存到各个节点的内存中，而不是每个 Task

* 广播变量被创建后，能在集群中运行的任何函数调用

* 广播变量是只读的，不能在被广播后修改

* 对于大数据集的广播， Spark 尝试使用高效的广播算法来降低通信成本

使用方法：

`val broadcastVar = sc.broadcast(Array(1, 2, 3))`

累加器（accumulator）：只支持加法操作的变量，可以实现计数器和变量求和。用户可以调用SparkContext.accumulator(v)创建一个初始值为v的累加器，而运行在集群上的Task可以使用“+=”操作，但这些任务却不能读取；只有驱动程序才能获取累加器的值

使用方法：

    val accum = sc.accumulator(0) 
    sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum  + = x)
    accum.value
    val num=sc.parallelize(1 to 100)


# 2. RDD

## 术语定义

* 弹性分布式数据集（RDD）： Resillient Distributed Dataset，Spark的基本计算单元，可以通过一系列算子进行操作（主要有Transformation和Action操作）；

* 有向无环图（DAG）：Directed Acycle graph，反应RDD之间的依赖关系；

* 有向无环图调度器（DAG Scheduler）：根据Job构建基于Stage的DAG，并提交Stage给TaskScheduler；

* 任务调度器（Task Scheduler）：将Taskset提交给worker（集群）运行并回报结果；

* 窄依赖（Narrow dependency）：子RDD依赖于父RDD中固定的data partition；

* 宽依赖（Wide Dependency）：子RDD对父RDD中的所有data partition都有依赖。

## RDD概念及特点

RDD是Spark的最基本抽象,是对分布式内存的抽象使用，实现了以操作本地集合的方式来操作分布式数据集的抽象实现。RDD是Spark最核心的东西，它表示已被分区，不可变的并能够被并行操作的数据集合，不同的数据集格式对应不同的RDD实现。RDD必须是可序列化的。RDD可以cache到内存中，每次对RDD数据集的操作之后的结果，都可以存放到内存中，下一个操作可以直接从内存中输入，省去了MapReduce大量的磁盘IO操作。这对于迭代运算比较常见的机器学习算法, 交互式数据挖掘来说，效率提升非常大。

RDD 最适合那种在数据集上的所有元素都执行相同操作的批处理式应用。在这种情况下， RDD 只需记录血统中每个转换就能还原丢失的数据分区，而无需记录大量的数据操作日志。所以 RDD 不适合那些需要异步、细粒度更新状态的应用 ，比如 Web 应用的存储系统，或增量式的 Web 爬虫等。对于这些应用，使用具有事务更新日志和数据检查点的数据库系统更为高效。

主要有以下特点：

1. 来源：一种是从持久存储获取数据，另一种是从其他RDD生成

2. 只读：状态不可变，不能修改

3. 分区：支持元素根据 Key 来分区 ( Partitioning ) ，保存到多个结点上，还原时只会重新计算丢失分区的数据，而不会影响整个系统

4. 路径：在 RDD 中叫世族或血统 ( lineage ) ，即 RDD 有充足的信息关于它是如何从其他 RDD 产生而来的

5. 持久化：可以控制存储级别（内存、磁盘等）来进行持久化

6. 操作：丰富的动作 ( Action ) ，如Count、Reduce、Collect和Save 等

## Transformations　&&　Actions

对于RDD可以有两种计算方式：转换（返回值还是一个RDD）与操作（返回值不是一个RDD）

* 转换(Transformations) (如：map, filter, groupBy, join等)，Transformations操作是Lazy的，也就是说从一个RDD转换生成另一个RDD的操作不是马上执行，Spark在遇到Transformations操作时只会记录需要这样的操作，并不会去执行，需要等到有Actions操作的时候才会真正启动计算过程进行计算。

* 操作(Actions) (如：count, collect, save等)，Actions操作会返回结果或把RDD数据写到存储系统中。Actions是触发Spark启动计算的动因。

<img src="https://github.com/jimmy-src/spark-learning/blob/master/Public%20File/image/TrasformationsAndActions.jpg" width = "600" height = "400" alt="来源：http://www.jianshu.com/p/2b23a3fb479d" align=center />

*Transformations （转换*

下表列出了一些 Spark 常用的 transformations（转换）。详情请参考 RDD API 文档（Scala，Java，Python，R）和 pair RDD 函数文档（Scala，Java）。

 transformations（转换）	|	Meaning（含义）
---------------------------------------------------------------------------------------------------------	|	------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
aggregateByKey(zeroValue)(seqOp, combOp, [numTasks])	|	在一个 (K, V) pair 的 dataset 上调用时，返回一个 (K, Iterable<V>) pairs 的 dataset，它的值会针对每一个 key 使用指定的 combine 函数和一个中间的 “zero” 值来聚合，它必须为 (V,V) => V 类型。为了避免不必要的配置，可以使用一个不同与 input value 类型的 aggregated value 类型。
cartesian(otherDataset)	|	在一个 T 和 U 类型的 dataset 上调用时，返回一个 (T, U) pairs 类型的 dataset（所有元素的 pairs，即笛卡尔积）。
coalesce(numPartitions)	|	Decrease（降低）RDD 中 partitions（分区）的数量为 numPartitions。对于执行过滤后一个大的 dataset 操作是更有效的。
cogroup(otherDataset, [numTasks])	|	在一个 (K, V) 和的 dataset 上调用时，返回一个 (K, (Iterable<V>, Iterable<W>)) tuples 的 dataset。这个操作也调用了 groupWith。
distinct([numTasks]))	|	返回一个新的 dataset，它包含了 source dataset（源数据集）中去重的元素。
filter(func)	|	返回一个新的 distributed dataset（分布式数据集），它由每个 source（数据源）中应用一个函数 func 且返回值为 true 的元素来生成。
flatMap(func)	|	与 map 类似，但是每一个输入的 item 可以被映射成 0 个或多个输出的 items（所以 func 应该返回一个 Seq 而不是一个单独的 item）
groupByKey([numTasks])	|	在一个 (K, V) pair 的 dataset 上调用时，返回一个 (K, Iterable<V>) pairs 的 dataset。 \n 注意 : 如果分组是为了在每一个 key 上执行聚合操作（例如，sum 或 average)，此时使用 reduceByKey 或 aggregateByKey 来计算性能会更好。 \ 注意 : 默认情况下，并行度取决于父 RDD 的分区数。可以传递一个可选的 numTasks 参数来设置不同的任务数。
intersection(otherDataset)	|	返回一个新的 RDD，它包含了 source dataset（源数据集）和 otherDataset（其它数据集）的交集。
join(otherDataset, [numTasks])	|	在一个 (K, V) 和 (K, W) 类型的 dataset 上调用时，返回一个 (K, (V, W)) pairs 的 dataset，它拥有每个 key 中所有的元素对。Outer joins 可以通过 leftOuterJoin，rightOuterJoin 和fullOuterJoin 来实现。
map(func)	|	返回一个新的 distributed dataset（分布式数据集），它由每个 source（数据源）中的元素应用一个函数 func 来生成。
mapPartitions(func)	|	与 map 类似，但是单独的运行在在每个 RDD 的 partition（分区，block）上，所以在一个类型为 T 的 RDD 上运行时 func 必须是 Iterator<T> => Iterator<U> 类型。
mapPartitionsWithIndex(func)	|	与 mapPartitions 类似，但是也需要提供一个代表 partition 的 index（索引）的 interger value（整型值）作为参数的 func，所以在一个类型为 T 的 RDD 上运行时 func 必须是 (Int, Iterator<T>) => Iterator<U> 类型。
pipe(command, [envVars])	|	通过使用 shell 命令来将每个 RDD 的分区给 Pipe。例如，一个 Perl 或 bash 脚本。RDD 的元素会被写入进程的标准输入（stdin），并且 lines（行）输出到它的标准输出（stdout）被作为一个字符串型 RDD 的 string 返回。
reduceByKey(func, [numTasks])	|	在一个 (K, V) pair 的 dataset 上调用时，返回一个 (K, Iterable<V>) pairs 的 dataset，它的值会针对每一个 key 使用指定的 reduce 函数 func 来聚合，它必须为 (V,V) => V 类型。像 groupByKey 一样，可通过第二个可选参数来配置 reduce 任务的数量。
repartition(numPartitions)	|	Reshuffle（重新洗牌）RDD 中的数据以创建或者更多的 partitions（分区）并将每个分区中的数据尽量保持均匀。该操作总是通过网络来 shuffles 所有的数据。
repartitionAndSortWithinPartitions(partitioner)	|	根据给定的 partitioner（分区器）对 RDD 进行重新分区，并在每个结果分区中，按照 key 值对记录排序。这比每一个分区中先调用 repartition 然后再 sorting（排序）效率更高，因为它可以将排序过程推送到 shuffle 操作的机器上进行。
sample(withReplacement, fraction, seed)	|	样本数据，设置是否放回（withReplacement）、采样的百分比（fraction）、使用指定的随机数生成器的种子（seed）。
sortByKey([ascending], [numTasks])	|	在一个 (K, V) pair 的 dataset 上调用时，其中的 K 实现了 Ordered，返回一个按 keys 升序或降序的 (K, V) pairs 的 dataset。
union(otherDataset)	|	返回一个新的 dataset，它包含了 source dataset（源数据集）和 otherDataset（其它数据集）的并集。

*Actions （动作）*

下面列出了一些 Spark 常用的 actions 操作。详细请参考 RDD API 文档 (Scala, Java, Python, R) 和 pair RDD 函数文档 (Scala, Java)。

Actions （动作）	|	Meaning（含义）
---------------------------------------------------------------------------------------------------------	|	------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
first()	|	返回数据集中的第一个元素（类似于 take(1)）。
count()	|	返回数据集中元素的个数。
takeOrdered(n, [ordering])	|	返回 RDD 按自然顺序（natural order）或自定义比较器（custom comparator）排序后的前 n 个元素。
take(n)	|	将数据集中的前 n 个元素作为一个数组返回。
saveAsTextFile(path)	|	将数据集中的元素以文本文件（或文本文件集合）的形式写入本地文件系统、HDFS 或其它 Hadoop 支持的文件系统中的给定目录中。Spark 将对每个元素调用 toString 方法，将数据元素转换为文本文件中的一行记录。
saveAsSequenceFile(path) 	|	将数据集中的元素以 Hadoop SequenceFile 的形式写入到本地文件系统、HDFS 或其它 Hadoop 支持的文件系统指定的路径中。该操作可以在实现了 Hadoop 的 Writable 接口的键值对（key-value pairs）的 RDD 上使用。在 Scala 中，它还可以隐式转换为 Writable 的类型（Spark 包括了基本类型的转换，例如 Int、Double、String 等等)。
(Java and Scala)	|	
foreach(func)	|	对数据集中每个元素运行函数 func 。这通常用于副作用（side effects），例如更新一个累加器（Accumulator）或与外部存储系统（external storage systems）进行交互。注意：修改除 foreach() 之外的累加器以外的变量（variables）可能会导致未定义的行为（undefined behavior）。详细介绍请阅读 理解闭包（Understanding closures） 部分。
takeSample(withReplacement, num, [seed])	|	对一个数据集随机抽样，返回一个包含 num 个随机抽样（random sample）元素的数组，参数 withReplacement 指定是否有放回抽样，参数 seed 指定生成随机数的种子。
collect()	|	在驱动程序中，以一个数组的形式返回数据集的所有元素。这在返回足够小（sufficiently small）的数据子集的过滤器（filter）或其他操作（other operation）之后通常是有用的。
reduce(func)	|	使用函数 func 聚合数据集（dataset）中的元素，这个函数 func 输入为两个元素，返回为一个元素。这个函数应该是可交换（commutative ）和关联（associative）的，这样才能保证它可以被并行地正确计算。
saveAsObjectFile(path) 	|	使用 Java 序列化（serialization）以简单的格式（simple format）编写数据集的元素，然后使用 SparkContext.objectFile() 进行加载。
countByKey()	|	仅适用于（K,V）类型的 RDD 。返回具有每个 key 的计数的 （K , Int）对 的 hashmap。














