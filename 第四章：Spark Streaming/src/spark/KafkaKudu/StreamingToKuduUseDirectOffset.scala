package spark.KafkaKudu

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spark.utils.dataProcessing

/**
  * Created by yunchen on 2017/8/17.
  *
  * 1，这种情况下如果程序挂了，会从最新的offset进行读取，这并不是我们想要的
  *
  * 可以采用两种方式：
  *
  *   >1，checkpoint方式，实现简单
  *   >2，将offset存储到zookeeper，也方便基于zookeeper的kafka监控工具进行监控，但实现相对麻烦
  *
  * 提交命令：
  *   >1,spark2-submit --jars /root/spark-streaming-kafka-0-10_2.11-2.1.0.jar,/root/kudu-spark2_2.11-1.3.0.jar --master yarn --deploy-mode cluster --class KafkaKudu.StreamingToKuduUseDirect spark-learning.jar zjdw-pre0065:9092,zjdw-pre0066:9092,zjdw-pre0067:9092,zjdw-pre0068:9092,zjdw-pre0069:9092 syslognifi 192.168.3.79:7051 impala::default.tsgz_syslog StreamingToKuduUseDirect default common
  *   >2,spark2-submit --jars /root/spark-streaming-kafka-0-10_2.11-2.1.0.jar,/root/kudu-spark2_2.11-1.3.0.jar --master yarn --deploy-mode cluster --class KafkaKudu.StreamingToKuduUseDirect spark-learning.jar zjdw-pre0065:9092,zjdw-pre0066:9092,zjdw-pre0067:9092,zjdw-pre0068:9092,zjdw-pre0069:9092 syslognifi 192.168.3.79:7051 impala::default.tsgz_syslog_new StreamingToKuduUseDirectNew newcommon newcommon
  *   >3,spark2-submit --jars /root/spark-streaming-kafka-0-10_2.11-2.1.0.jar,/root/kudu-spark2_2.11-1.3.0.jar --master yarn --deploy-mode client --class KafkaKudu.StreamingToKuduUseDirect spark-learning.jar zjdw-pre0065:9092,zjdw-pre0066:9092,zjdw-pre0067:9092,zjdw-pre0068:9092,zjdw-pre0069:9092 syslognifi 192.168.3.79:7051 impala::default.tsgz_test StreamingToKuduUseDirectCheckpoint default dd_test
  */
object StreamingToKuduUseDirectOffset extends Serializable {

  def main(args: Array[String]): Unit = {
    if (args.length < 7) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |  <masterList> is a list of kudu
                            |  <kuduTableName> is a name of kudu
                            |  <appName>  is a name of spark processing
                            |  <dataProcessingMode> the function of dataProcessing logical processing mode
                            |         default,common,newcommon,stds,tcdns
                            |  <groupid> is the name of kafka groupname
                            |  <checkpoint_path> is a address of checkpoint  hdfs:///tmp/checkpoint_
        """.stripMargin)
      System.exit(1)
    }

    val log = Logger.getLogger(getClass.getName)

    //1.获取输入参数与定义全局变量,主要两部分，一个brokers地址，一个topics列表，至于其他优化参数后续再加
    //TODO: 注意：后面需要改为properties的方式来获取参数
    val Array(brokers,topic,masterList,kuduTableName,appName,dataProcessingMode,groupid,checkpoint_path) = args
    //val appName = "DirectKafkaWordCount"
    //val masterList = "zjdw-pre0069:7051"
    //val kuduTableName = "tsgz_syslog"
    val sctime = 10

    //2.配置spark环境以及kudu环境
    val sparkConf = new SparkConf().setAppName(appName)
    val ssc = new StreamingContext(sparkConf, Seconds(sctime))
    //ssc.checkpoint("hdfs:///tmp/checkpoint_"+ Random.nextInt())

    log.info("ssc初始化成功..........")

    //ssc.checkpoint(checkpoint_path)

    val spark = SparkSession.builder.appName(appName).enableHiveSupport().getOrCreate()

    val kuduContext = new KuduContext(masterList)

    //3.配置创建kafka输入流的所需要的参数，注意这里可以加入一些优化参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(topic)

    //4.创建kafka输入流0.10
    val stream  = KafkaUtils.createDirectStream[String, String](ssc,PreferConsistent,Subscribe[String, String](topics, kafkaParams))

    //5.对每一个window操作执行foreach，写入数据处理逻辑
    // TODO：这里要写一个数据逻辑处理函数，不同的数据对应不同的函数,函数返回的就是DF，然后直接操作DF写入目的地

    stream.foreachRDD { rdd =>

      log.info("记录offset值。。")
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      val messageRDD = rdd.map(record => (record.key(), record.value())).values

      val kuduDF = dataProcessing.Porcess(dataProcessingMode,messageRDD,spark)

      //messageDF.createOrReplaceTempView("syslogTableTemp")
      if(kuduDF == null){
        System.err.println("dataProcessingMode选择错误： " + dataProcessingMode + "\n" +
                            "dataProcessingMode可以为default,common,newcomer")
        System.exit(1)
      }
      kuduContext.upsertRows(kuduDF,kuduTableName)

      log.info("将offset值写入zookeeper。。")
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

    }

    //6.Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
