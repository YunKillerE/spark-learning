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

/**
  * Created by yunchen on 2017/8/17.
  *
  */
object StreamingToKuduUseDirectUpdateStateByKey extends Serializable {

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

    ssc.checkpoint(checkpoint_path)

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

    val addFunc = (currValuess:Seq[Int],prevValues:Option[Int]) => {
      //当前批次单词总数
      val currCount = currValuess.sum
      //已累加的值，如果是初次，则为0
      val prevCount = prevValues.getOrElse(0)
      //返回累加后的结果
      Some(currCount + prevCount)
    }

    stream.map(record => ((record.value().split(",").toList)(1),1)).updateStateByKey[Int](addFunc)
    .foreachRDD{ rdd =>
/*
      log.info("记录offset值。。")
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
*/

      import spark.implicits._
      val df = rdd.map(x=>(x._2,x._1)).sortByKey().map(x=>(x._2,x._1)).toDF("host","count")
/*      log.info("当前写入的内容为：\n"+df.show())
      log.info("写入kudu..........")*/
      kuduContext.upsertRows(df,kuduTableName)
/*
      log.info("将offset值写入zookeeper。。")
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)*/
    }

    //6.Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
