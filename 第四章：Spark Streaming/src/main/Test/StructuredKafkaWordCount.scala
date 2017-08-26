package Test

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession

/**
  * Created by yunchen on 2017/8/1.
  *
  * 这里注意版本的问题:
  *
  * 1，kafka版本：CDH5.11 kafka的版本默认是0.10版本
  * 2，spark版本，spark2.2.0
  * 3，注意Structured Streaming只支持kafka 0.10高的版本
  *
  * 这里需要依赖的包是spark-sql-kafka-0-10_2.11，注意0-10是kafka版本，2.11是scala版本，下面的2.2.0是spark版本
  *
  * 下载地址：https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.11/2.2.0/
  *
  */
object StructuredKafkaWordCount {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: StructuredKafkaWordCount <bootstrap-servers> " + " <topics>")
      System.exit(1)
    }

    val Array(bootstrapServers, topics) = args

    val masterList = "zjdw-pre0069:7051"
    val kuduContext = new KuduContext(masterList)

    val spark = SparkSession
      .builder
      .appName("StructuredKafkaWordCount_yunchen")
      .getOrCreate()

    import spark.implicits._

    // Create DataSet representing the stream of input lines from kafka
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topics)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    // Generate running word count
    //val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()
    val kuduTableName = "impala::default.xxx"
    val  kuduInsert = lines.flatMap(_.split(" ")).map(rec => (1,1,rec.toLowerCase))
      //.toDF("id","name","json")
    kuduInsert.foreach(ki =>
      kuduContext.upsertRows(Seq(
        (ki)
      ).toDF("id","name","json"), kuduTableName)
    )

    // .options(Map("kudu.master"-> "kudu.master:7051", "kudu.table"-> "test_table")).mode("append").kudu
    // Start running the query that prints the running counts to the console
    val query = kuduInsert.writeStream
      .outputMode("complete")
      .start()

    query.awaitTermination()
  }


}
