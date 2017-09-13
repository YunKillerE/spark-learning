package Sparkflume

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume._
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import scala.util.Random

/**
  * 如果采用这种push的方式，提交的时候不能提交yarn集群，executor会随机运行在集群中的一台机器上，无法侦听我们指定的ip地址和端口，暂时未找到解决办法
  *
  * 所以提交的时候运行在local[2]模式
  *
  * spark2-submit --jars .ivy2/jars/org.apache.spark_spark-streaming-flume_2.11-2.2.0.jar,kudu-spark2_2.11-1.3.0.jar --master local[3]
  * --class Sparkflume.FlumeEventToKudu spark-learning.jar 192.168.3.74 34343 192.168.3.79 impala::default.ratings ratings
  *
  *
  * flume配置：
  *
  * fk.sources  = source1
fk.channels = channel1
fk.sinks    = sink1

fk.sources.source1.type = spooldir
fk.sources.source1.spoolDir = /home/demo/ratings/
fk.sources.source1.fileHeader = false
fk.sources.source1.channels = channel1

fk.channels.channel1.type                = memory
fk.channels.channel1.capacity            = 10000
fk.channels.channel1.transactionCapacity = 1000

fk.sinks.sink1.type = avro
fk.sinks.sink1.hostname = 192.168.3.74
fk.sinks.sink1.port = 34343
fk.sinks.sink1.channel = channel1
  *
  */
object FlumeEventToKudu {

  case class ratings(sys_id: String, sys_hostname: String, sys_time: String, sys_message: String)

  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <host> is a list of one or more Kafka brokers
                            |  <port> is a list of one or more kafka topics to consume from
                            |  <masterList> is a list of kudu
                            |  <kuduTableName> is a name of kudu
                            |  <appName>  is a name of spark processing
        """.stripMargin)
      System.exit(1)
    }

    //val Array(brokers,topic,masterList,kuduTableName,appName,dataProcessingMode,groupid,checkpoint_path) = args
    val Array(host, port, masterList, kuduTableName, appName) = args
    val batchInterval = Milliseconds(5000)

    // Create the context and set the batch size
    val kuduContext = new KuduContext(masterList)
    val sparkConf = new SparkConf().setAppName(appName)
    val ssc = new StreamingContext(sparkConf, batchInterval)
    val spark = SparkSession.builder.appName(appName).enableHiveSupport().getOrCreate()

    // Create a flume stream that polls the Spark Sink running in a Flume agent
    val stream = FlumeUtils.createStream(ssc, host, port.toInt, StorageLevel.MEMORY_ONLY_SER_2)

    // Print out the count of events received from this server in each batch
    stream.foreachRDD{rdd =>
      val eventRDD = rdd.map(flumeEvent =>new String(flumeEvent.event.getBody.array()).split("\t"))

      import spark.implicits._
      val eventDF = eventRDD.map(row => ratings(BigInt.probablePrime(50,Random).toString(36)+"-"+row(0),row(1),row(2),row(3))).toDF

      kuduContext.upsertRows(eventDF,kuduTableName)
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
