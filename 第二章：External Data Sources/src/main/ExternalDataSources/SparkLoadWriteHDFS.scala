package ExternalDataSources

import Utils.SparkUtils
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.io.compress.{BZip2Codec, GzipCodec, SnappyCodec}
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext


/**
  * Created by yunchen on 2017/5/17.
  */
object SparkLoadWriteHDFS {

  def main(args: Array[String]): Unit = {

    if(args.length < 2){
      System.err.println("Usage: <统计目录> <结果写入目录>")
      System.exit(1)
    }

    val sc = SparkUtils.SaprkSessionSC("yunchen")
    val InputFilePath = args(0)
    val OutPutFilePath = args(1)

    //WordCount(sc,InputFilePath,OutPutFilePath,"snappy")

    val job = new Job()
    job.setOutputFormatClass(class[TextOutputFormat[LongWritable,Text]])

    job.getConfiguration().set("mapred.output.compress", "true")
    job.getConfiguration().set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec")

    val textFile = sc.newAPIHadoopFile(InputFilePath, classOf[LzoTextInputFormat],classOf[LongWritable], classOf[Text],job.getConfiguration())

    textFile.saveAsNewAPIHadoopFile(args(1), classOf[LongWritable], classOf[Text],classOf[TextOutputFormat[LongWritable,Text]],job.getConfiguration())

  }

  /**
    *
    * 此函数可以实现从文件系统或者hdfs上通过textFile读取数据
    *
    * @param sc sparkcontext，这里不能传入sparksession
    * @param InputFilePath  输入路径
    * @param OutPutFilePath 结果输入路径
    * @param compress 压缩格式，后续可以加入更多压缩格式的支持，比如lzo
    */
  def WordCount(sc:SparkContext, InputFilePath:String, OutPutFilePath:String , compress:String):Unit = {
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

  def customInputFormat():Unit = {



  }


}
