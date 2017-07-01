package test

import Utils.SparkUtils

/**
  * Created by yunchen on 2017/6/29.
  */
object ScLocalAndHDFSTest {

  /**
    * 测试从本地文件系统/hdfs获取文件进行wordcount
    * 本地文件系统：file:///root/file
    * hdfs文件系统：hdfs:///root/file
    * @param args 第一个参数是input 第二个是ouput
    *
    * (不知道为什么明明所有节点都有文件，就是执行不成功，提示找不到文件？？)
    * spark-submit --master yarn-client --class ExternalDataSources.SparkLoadWriteHDFS spark-learning.jar file:///root/sshd_config file:///root/abc
    *
    * spark-submit --master yarn-client --class ExternalDataSources.SparkLoadWriteHDFS spark-learning.jar hdfs:///user/root/sshd_config hdfs:///user/root/abc
    *
    */
  def main(args: Array[String]): Unit = {

    if(args.length < 2){
      System.err.println("Usage: <统计目录> <结果写入目录>")
      System.exit(1)
    }

    val sc = SparkUtils.SaprkSessionSC("yunchen")
    val InputFilePath = args(0)
    val OutPutFilePath = args(1)

    SparkUtils.ScTextFileWordCount(sc,InputFilePath,OutPutFilePath,"snappy")

  }

}
