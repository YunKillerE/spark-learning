package KafkaKudu

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by yunchen on 2017/8/24.
  */
class StreamingKafkaWordCount {

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |  <masterList> is a list of kudu
                            |  <kuduTableName> is a name of kudu
        """.stripMargin)
      System.exit(1)
    }

    //1.获取输入参数与定义全局变量,主要两部分，一个brokers地址，一个topics列表，至于其他优化参数后续再加
    //注意：后面需要改为properties的方式来获取参数
    val Array(brokers,topic,masterList,kuduTableName) = args
    val appName = "DirectKafkaWordCount"
    //val masterList = "zjdw-pre0069:7051"
    //val kuduTableName = "tsgz_syslog"

    //2.配置spark环境以及kudu环境
    val sparkConf = new SparkConf().setAppName(appName)
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    //ssc.checkpoint("hdfs:///tmp/checkpoint")
    val spark = SparkSession.builder.appName(appName).enableHiveSupport().getOrCreate()

    val kuduContext = new KuduContext(masterList)

    //3.配置创建kafka输入流的所需要的参数，注意这里可以加入一些优化参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array(topic)

    //4.创建kafka输入流
    val stream  = KafkaUtils.createDirectStream[String, String](ssc,PreferConsistent,Subscribe[String, String](topics, kafkaParams))

    // Get the lines, split them into words, count the words and print
    val lines = stream.map(record => (record.key, record.value))

    //val words = lines.flatMap(_.split(" "))
    val wordCounts = lines.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    //6.Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
