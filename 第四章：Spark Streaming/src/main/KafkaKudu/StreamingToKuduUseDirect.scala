package KafkaKudu

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by yunchen on 2017/8/17.
  *
  * spark2-submit --jars /root/spark-streaming-kafka-0-10_2.11-2.1.0.jar,/root/kudu-spark2_2.11-1.3.0.jar --master yarn --deploy-mode cluster --class KafkaKudu.StreamingToKuduUseDirect spark-learning.jar zjdw-pre0065:9092,zjdw-pre0066:9092,zjdw-pre0067:9092,zjdw-pre0068:9092,zjdw-pre0069:9092 syslognifi 192.168.3.79:7051 impala::default.tsgz_syslog
  *
  *
  */
object StreamingToKuduUseDirect {

  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |  <masterList> is a list of kudu
                            |  <kuduTableName> is a name of kudu
                            |  <appName>  is a name of spark processing
                            |  <dataProcessingMode> the function of dataProcessing logical processing mode
                            |         default,common,newcomer,_
                            |  <groupid> is the name of kafka groupname
        """.stripMargin)
      System.exit(1)
    }

    //1.获取输入参数与定义全局变量,主要两部分，一个brokers地址，一个topics列表，至于其他优化参数后续再加
    //TODO: 注意：后面需要改为properties的方式来获取参数
    val Array(brokers,topic,masterList,kuduTableName,appName,dataProcessingMode,groupid) = args
    //val appName = "DirectKafkaWordCount"
    //val masterList = "zjdw-pre0069:7051"
    //val kuduTableName = "tsgz_syslog"
    val sctime = 2

    //2.配置spark环境以及kudu环境
    val sparkConf = new SparkConf().setAppName(appName)
    val ssc = new StreamingContext(sparkConf, Seconds(sctime))
    //ssc.checkpoint("hdfs:///tmp/checkpoint")
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

    //4.创建kafka输入流
    val stream  = KafkaUtils.createDirectStream[String, String](ssc,PreferConsistent,Subscribe[String, String](topics, kafkaParams))

    //5.对每一个window操作执行foreach，写入数据处理逻辑
    // TODO：这里要写一个数据逻辑处理函数，不同的数据对应不同的函数

    stream.foreachRDD { rdd =>

      //获取所有的value，不知道这里key为什么都是null？
      val messageRDD = rdd.map(record => (record.key, record.value)).values
      //messageRDD.foreach(println(_))

      //将value映射成多个字段,也就是将rdd转换成dataframe
/*      //方法1
      val schemaString = "sys_id,sys_hostname,sys_time,sys_message"
      val schema = StructType(schemaString.split(",").map(fieldName=>StructField(fieldName,StringType,true)))
      val kuduRDD = messageDF.map(_.split(",")).map(p=>Row(p(0),p(1),p(2),p(3)))
      val kuduDF = spark.createDataFrame(kuduRDD,schema)
      kuduDF.show(false)

      //方法2
      case class syslog(sys_id: String, sys_hostname: String, sys_time: String, sys_message: String)
      import spark.implicits._
      val kuduDF1 = messageDF.map(_.split(",")).map(p=>syslog(p(0),p(1),p(2),p(3))).toDF()
      kuduDF.show(false)*/
      val kuduDF = KafkaKudu.utils.dataProcessing.syslogPorcess(dataProcessingMode,messageRDD,spark)

      //messageDF.createOrReplaceTempView("syslogTableTemp")
      if(kuduDF == null){
        System.err.println("dataProcessingMode选择错误： " + dataProcessingMode )
        System.exit(1)
      }
      kuduContext.upsertRows(kuduDF,kuduTableName )
    }

    //6.Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
