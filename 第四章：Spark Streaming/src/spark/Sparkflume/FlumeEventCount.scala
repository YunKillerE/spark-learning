package Sparkflume

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
  * Created by yunchen on 2017/9/4.
  *
  * Approach 1: Flume-style Push-based Approach
  *
  */
class FlumeEventCount {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(
        "Usage: FlumeEventCount <host> <port>")
      System.exit(1)
    }

    val Array(host, port) = args

    val batchInterval = Milliseconds(2000)

    // Create the context and set the batch size
    val sparkConf = new SparkConf().setAppName("FlumeEventCount")
    val ssc = new StreamingContext(sparkConf, batchInterval)

    // Create a flume stream
    val stream = FlumeUtils.createStream(ssc, host, port.toInt, StorageLevel.MEMORY_ONLY_SER_2)

    // Print out the count of events received from this server in each batch
    stream.count().map(cnt => "Received " + cnt + " flume events." ).print()

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    *
    * val batchInterval = Milliseconds(2000)
    * val ssc = new StreamingContext(sc, batchInterval)
    * val stream = FlumeUtils.createStream(ssc, "192.168.3.74", 34343, StorageLevel.MEMORY_ONLY_SER_2)
    * stream.count().map(cnt => "Received " + cnt + " flume events." ).print()
    * ssc.start()
    * ssc.awaitTermination()
    */

}
