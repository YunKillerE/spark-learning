package Sparkflume

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.flume._

import scala.util.Random

/**
  *  flume的配置：
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

fk.sinks.sink1.type = org.apache.spark.streaming.flume.sink.SparkSink
fk.sinks.sink1.hostname = 192.168.3.74
fk.sinks.sink1.port = 34343
fk.sinks.sink1.channel = channel1

  *
  * 以及将三个commons-lang3-3.5.jar  scala-library-2.11.8.jar  spark-streaming-flume-sink_2.11-2.2.0.jar包加入到flume的lib目录中
  *
  * 这里尤其要注意spark-streaming-flume-sink_2.11-2.2.0.jar这个包的版本，flume 1.7不能用2.11的版本，要用2.10的版本，才能测试通过
  *
  * 我在讲flume的lib目录scala-library-2.10.5.jar删掉换成2.11.8还是报错，感觉flume1.7默认就是以scala2.10.5进行编译，一改变就把偶吃哦
  *
  * 所以我这里放进去的包是：commons-lang3-3.5.jar  scala-library-2.10.5.jar  spark-streaming-flume-sink_2.10-2.2.0.jar
  *
  * 数据源：
  *
  */
object FlumePollingEventToKudu {

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
    val batchInterval = Milliseconds(10000)

    // Create the context and set the batch size
    val kuduContext = new KuduContext(masterList)
    val sparkConf = new SparkConf().setAppName(appName)
    val ssc = new StreamingContext(sparkConf, batchInterval)
    val spark = SparkSession.builder.appName(appName).enableHiveSupport().getOrCreate()

    // Create a flume stream that polls the Spark Sink running in a Flume agent
    val stream = FlumeUtils.createPollingStream(ssc, host, port.toInt)

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
