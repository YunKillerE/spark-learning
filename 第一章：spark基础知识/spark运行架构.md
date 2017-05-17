# spark常见术语介绍

<img src="http://images0.cnblogs.com/blog/107289/201508/111609254102564.gif" width = "300" height = "200" alt="图片名称" align=center />

* Application：Spark Application的概念和Hadoop MapReduce中的类似，指的是用户编写的Spark应用程序，包含了一个Driver 功能的代码和分布在集群中多个节点上运行的Executor代码；

* Driver：Spark中的Driver即运行上述Application的main()函数并且创建SparkContext，其中创建SparkContext的目的是为了准备Spark应用程序的运行环境。在Spark中由SparkContext负责和ClusterManager通信，进行资源的申请、任务的分配和监控等；当Executor部分运行完毕后，Driver负责将SparkContext关闭。通常用SparkContext代表Drive；

* Executor：Application运行在Worker 节点上的一个进程，该进程负责运行Task，并且负责将数据存在内存或者磁盘上，每个Application都有各自独立的一批Executor。在Spark on Yarn模式下，其进程名称为CoarseGrainedExecutorBackend，类似于Hadoop MapReduce中的YarnChild。一个CoarseGrainedExecutorBackend进程有且仅有一个executor对象，它负责将Task包装成taskRunner，并从线程池中抽取出一个空闲线程运行Task。每个CoarseGrainedExecutorBackend能并行运行Task的数量就取决于分配给它的CPU的个数了；

* Cluster Manager：指的是在集群上获取资源的外部服务，目前有：

    Standalone：Spark原生的资源管理，由Master负责资源的分配；
    
    Hadoop Yarn：由YARN中的ResourceManager负责资源的分配；
    
    Mesos：由Mesos负责资源的分配；
    
* Worker：集群中任何可以运行Application代码的节点，类似于YARN中的NodeManager节点。在Standalone模式中指的就是通过Slave文件配置的Worker节点，在Spark on Yarn模式中指的就是NodeManager节点；

* 作业（Job）：包含多个Task组成的并行计算，往往由Spark Action催生，一个JOB包含多个RDD及作用于相应RDD上的各种Operation；

* 阶段（Stage）：每个Job会被拆分很多组Task，每组任务被称为Stage，也可称TaskSet，一个作业分为多个阶段；

* 任务（Task）： 被送到某个Executor上的工作任务；

# spark运行的基本流程
    
    Spark运行流程
    DAGScheduler
    TaskScheduler

## Spark运行流程

Spark运行基本流程：

1.   构建Spark Application的运行环境（启动SparkContext），SparkContext向资源管理器（可以是Standalone、Mesos或YARN）注册并申请运行Executor资源；

2.   资源管理器分配Executor资源并启动StandaloneExecutorBackend，Executor运行情况将随着心跳发送到资源管理器上；

3.   SparkContext构建成DAG图，将DAG图分解成Stage，并把Taskset发送给Task Scheduler。Executor向SparkContext申请Task，Task Scheduler将Task发放给Executor运行同时SparkContext将应用程序代码发放给Executor。

4.   Task在Executor上运行，运行完毕释放所有资源。

<img src="http://images0.cnblogs.com/blog/107289/201508/111609373484283.jpg" width = "300" height = "200" alt="图片名称" align=center />

Spark运行架构特点：

* 每个Application获取专属的executor进程，该进程在Application期间一直驻留，并以多线程方式运行tasks。这种Application隔离机制有其优势的，无论是从调度角度看（每个Driver调度它自己的任务），还是从运行角度看（来自不同Application的Task运行在不同的JVM中）。当然，这也意味着Spark Application不能跨应用程序共享数据，除非将数据写入到外部存储系统。

* Spark与资源管理器无关，只要能够获取executor进程，并能保持相互通信就可以了。

* 提交SparkContext的Client应该靠近Worker节点（运行Executor的节点)，最好是在同一个Rack里，因为Spark Application运行过程中SparkContext和Executor之间有大量的信息交换；如果想在远程集群中运行，最好使用RPC将SparkContext提交给集群，不要远离Worker运行SparkContext。

* Task采用了数据本地性和推测执行的优化机制。

## DAGScheduler

DAGScheduler把一个Spark作业转换成Stage的DAG（Directed Acyclic Graph有向无环图），根据RDD和Stage之间的关系找出开销最小的调度方法，然后把Stage以TaskSet的形式提交给TaskScheduler，下图展示了DAGScheduler的作用：

<img src="http://images0.cnblogs.com/blog/107289/201508/111609402853253.jpg" width = "300" height = "200" alt="图片名称" align=center />

## TaskScheduler

DAGScheduler决定了运行Task的理想位置，并把这些信息传递给下层的TaskScheduler。此外，DAGScheduler还处理由于Shuffle数据丢失导致的失败，这有可能需要重新提交运行之前的Stage（非Shuffle数据丢失导致的Task失败由TaskScheduler处理）。

TaskScheduler维护所有TaskSet，当Executor向Driver发送心跳时，TaskScheduler会根据其资源剩余情况分配相应的Task。另外TaskScheduler还维护着所有Task的运行状态，重试失败的Task。下图展示了TaskScheduler的作用：

<img src="http://images0.cnblogs.com/blog/107289/201508/111609424426393.jpg" width = "300" height = "200" alt="图片名称" align=center />

在不同运行模式中任务调度器具体为：

     Spark on Standalone模式为TaskScheduler；

     YARN-Client模式为YarnClientClusterScheduler

     YARN-Cluster模式为YarnClusterScheduler

# RDD的运行原理

RDD在Spark架构中是如何运行的呢？总体来看，主要分为三步：

1. 创建 RDD 对象
2. DAGScheduler模块介入运算，计算RDD之间的依赖关系。RDD之间的依赖关系就形成了DAG
3. 每一个JOB被分为多个Stage，划分Stage的一个主要依据是当前计算因子的输入是否是确定的，如果是则将其分在同一个Stage，避免多个Stage之间的消息传递开销。

<img src="http://images0.cnblogs.com/blog/107289/201508/111610021459507.jpg" width = "300" height = "200" alt="图片名称" align=center />

以下面一个按 A-Z 首字母分类，查找相同首字母下不同姓名总个数的例子来看一下 RDD 是如何运行起来的。

<img src="http://images0.cnblogs.com/blog/107289/201508/111610138321471.jpg" width = "300" height = "200" alt="图片名称" align=center />

步骤 1 ：创建 RDD  上面的例子除去最后一个 collect 是个动作，不会创建 RDD 之外，前面四个转换都会创建出新的 RDD 。因此第一步就是创建好所有 RDD( 内部的五项信息 ) 。

步骤 2 ：创建执行计划 Spark 会尽可能地管道化，并基于是否要重新组织数据来划分 阶段 (stage) ，例如本例中的 groupBy() 转换就会将整个执行计划划分成两阶段执行。最终会产生一个 DAG(directed acyclic graph ，有向无环图 ) 作为逻辑执行计划。

<img src="http://images0.cnblogs.com/blog/107289/201508/111610176609067.jpg" width = "300" height = "200" alt="图片名称" align=center />

步骤 3 ：调度任务  将各阶段划分成不同的 任务 (task) ，每个任务都是数据和计算的合体。在进行下一阶段前，当前阶段的所有任务都要执行完成。因为下一阶段的第一个转换一定是重新组织数据的，所以必须等当前阶段所有结果数据都计算出来了才能继续。

假设本例中的 hdfs://names 下有四个文件块，那么 HadoopRDD 中 partitions 就会有四个分区对应这四个块数据，同时 preferedLocations 会指明这四个块的最佳位置。现在，就可以创建出四个任务，并调度到合适的集群结点上。

<img src="http://images0.cnblogs.com/blog/107289/201508/111610241453720.jpg" width = "300" height = "200" alt="图片名称" align=center />


# spark Cluster Mode

     Spark on Standalone
     Spark on YARN-Client
     Spark on YARN-Cluster
     YARN-Client vs YARN-Cluster
     
     <img src="" width = "300" height = "200" alt="图片名称" align=center />