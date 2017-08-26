package Test


import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

/**
  * Created by yunchen on 2017/8/3.
  *
  * 注意需要下载对应的版本spark-streaming-kafka-0-10_2.11-2.2.0.jar
  *
  * https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10_2.11/2.2.0
  *
  */
object StreamingToKudu {

  def main(args: Array[String]): Unit = {

    val kuduTableName = "impala::default.stk"
    val kafkaBrokers  = args(0)
    val kuduMasters   = args(1)

    /*val conf = new SparkConf().setAppName("Spark2StreamingToKudu")
    val ssc = new StreamingContext(conf, Seconds(10))*/
    val spark = SparkSession
      .builder
      .appName("StructuredKafkaWordCount_yunchen")
      .getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext,Seconds(10))


    val kuduContext = new KuduContext(kuduMasters)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaBrokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "yunchen",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("html")

    val stream = KafkaUtils.createDirectStream[String, String](ssc,PreferConsistent,Subscribe[String,String](topics,kafkaParams))

/*    stream.foreachRDD { rdd =>
      import spark.implicits._
      val dataFrame = rdd.map(rec => ((new Random).nextInt(300000000),"xxxx",rec.toString()))
        .toDF("id","name","time")

      //dataFrame.registerTempTable("traffic")

      /* NOTE: All 3 methods provided  are equivalent UPSERT operations on the Kudu table and
         are idempotent, so we can run all 3 in this example (although only 1 is necessary) */

      // Method 1: All kudu operations can be used with KuduContext (INSERT, INSERT IGNORE,
      //           UPSERT, UPDATE, DELETE)

      kuduContext.upsertRows(dataFrame, kuduTableName)

      /*      // Method 2: The DataFrames API provides the 'write' function (results in a Kudu UPSERT)
            val kuduOptions: Map[String, String] = Map("kudu.table"  -> kuduTableName,
            "kudu.master" -> kuduMasters)
            resultsDataFrame.write.options(kuduOptions).mode("append").kudu

            // Method 3: A SQL INSERT through SQLContext also results in a Kudu UPSERT
            resultsDataFrame.registerTempTable("traffic_results")
            sqlContext.read.options(kuduOptions).kudu.registerTempTable(kuduTableName)
            sqlContext.sql(s"INSERT INTO TABLE `$kuduTableName` SELECT * FROM traffic_results")*/
    }*/

    case class LogStashV1(message:String, path:String, host:String, linenoouble:String,timestamp:String)
/*    stream.map(line => {
      //val json = parse(line)
      val js = parse(line,true)
      js.extract[LogStashV1]
    }).print()*/

    ssc.start()
    ssc.awaitTermination()

    }

  }
