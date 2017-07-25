package ExternalDataSources

import Utils.SparkUtils


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
    //val sc = SparkUtils.SaprkSessionSP("yunchen")
    val InputFilePath = args(0)
    val OutPutFilePath = args(1)

    //SparkUtils.ScTextFileWordCount(sc,InputFilePath,OutPutFilePath,"snappy")
    //SparkUtils.SpTextFileWordCount(sc,InputFilePath,OutPutFilePath,"snappy")

  }


}
