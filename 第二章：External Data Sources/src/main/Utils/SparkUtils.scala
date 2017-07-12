package Utils

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.io.compress.{BZip2Codec, GzipCodec, SnappyCodec}
import org.apache.hadoop.mapred.{KeyValueTextInputFormat, TextInputFormat}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrameReader, Row, SparkSession}

/**
  * Created by yunchen on 2017/5/8.
  */

object SparkUtils {

  /**
    * spark 1.6
    * @param appName
    * @return
    */
  def SaprkSessionSP(appName:String):SparkSession = {
    val spark = SparkSession
      .builder
      .appName(appName)
      .enableHiveSupport()
      .getOrCreate()

    return  spark
  }

  /**
    * spark 2.x
    * @param appName
    * @return
    */
  def SaprkSessionSC(appName:String):SparkContext = {
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)

    return sc
  }

  /**
    *
    * 此函数可以实现从文件系统或者hdfs上通过textFile读取数据进行单词计数
    *
    * @param sc sparkcontext，这里不能传入sparksession
    * @param InputFilePath  输入路径
    * @param OutPutFilePath 结果输入路径
    * @param compress 压缩格式，后续可以加入更多压缩格式的支持，比如lzo
    */
  def ScTextFileWordCount(sc:SparkContext, InputFilePath:String, OutPutFilePath:String , compress:String):Unit = {
    val count = sc.textFile(InputFilePath).flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey(_+_)
    if(compress == "bzip"){
      count.saveAsTextFile(OutPutFilePath,classOf[BZip2Codec])
    } else if(compress == "snappy"){
      count.saveAsTextFile(OutPutFilePath,classOf[SnappyCodec])
    } else if(compress == "gzip"){
      count.saveAsTextFile(OutPutFilePath,classOf[GzipCodec])
    }else{
      System.out.println("输入的压缩格式不支持或者还没有实现！！")
      System.exit(1)
    }
  }


  /**
    * 此函数可以实现从文件系统或者hdfs上通过dataset来进行单词计数
    *
    * 其实还可以加入压缩格式和存储格式
    *
    * @param sc sparksession
    * @param InputFilePath
    * @param OutPutFilePath
    * @param compress
    */
  def SpTextFileWordCount(sc:SparkSession, InputFilePath:String, OutPutFilePath:String , compress:String):Unit = {

    //结果是DF（key,value）
    import sc.implicits._
    val count = sc.read.textFile(InputFilePath).as[String].flatMap(_.split(" ")).groupByKey(_.toLowerCase).count()

    //case class Trans(name:String ,count:Int)
    //def toTrans = (trans: Seq[String]) => Trans(trans(0), trans(1).trim.toInt)

    count.withColumnRenamed("count(1)","count").write.save(OutPutFilePath)

  }

  def SpTextFileWordCount(sc:SparkSession, InputFilePath:String, OutPutFilePath:String):Unit = {

    //结果是DF（key,value）
    import sc.implicits._
    val count = sc.read.textFile(InputFilePath).as[String].flatMap(_.split(" ")).groupByKey(_.toLowerCase).count()

    case class Trans(name:String ,count:Int)
    def toTrans = (trans: Seq[String]) => Trans(trans(0), trans(1).trim.toInt)

    count.withColumnRenamed("count(1)","count").write.save(OutPutFilePath)

  }

  /**
    * 一直没测试成功，跳过，后续再测试
    * @param sc
    * @param InputFilePath
    * @param OutPutFilePath
    */
  def customInputFormat(sc:SparkContext, InputFilePath:String, OutPutFilePath:String):Unit = {

    val currencyFile = sc.newAPIHadoopFile[LongWritable, Text, TextInputFormat](InputFilePath)



  }


  def readDataFromHDFS(sc:SparkSession, dataFormat:String, InputFilePath:String, OutPutFilePath:String) = {

    val currFile = sc.read.format("dataFormat").load()

    currFile.write.format("text").save(OutPutFilePath)
    }


}
