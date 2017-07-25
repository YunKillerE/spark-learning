package test

import Utils.SparkUtils

/**
  * Created by yunchen on 2017/6/29.
  *
  * 注意：编译的时候package上一级的目录要为source，否则编译出来后找不到主函数，比较奇怪，不知道为什么
  *
  * 1，输入text 输出orc/par/csv/json，只需要更改下面的orc及输出目录就行
  * /opt/spark2/bin/spark-submit --master yarn --deploy-mode client --class test.sparkReadAndWrite spark-learning.jar hdfs://zjdw-pre0065:8020/user/root/sample
  *   hdfs://zjdw-pre0065:8020/user/root/samorc text orc false
  * 2，输入csv 输出parquet/json/orc/text 只需要更改下面的json及输出目录就行
  * /opt/spark2/bin/spark-submit --master yarn --deploy-mode client --class test.sparkReadAndWrite spark-learning.jar hdfs://zjdw-pre0065:8020/user/root/index_data.csv
  * hdfs://zjdw-pre0065:8020/user/root/indexjson csv json true
  *
  *
  *
  */
object sparkReadAndWrite {

  /**
    *
    *
    */
  def main(args: Array[String]): Unit = {

    if(args.length < 5){
      System.err.println("Usage: <InputFilePath> <OutPutFilePath> <srcDataFormat> <dstDataFormat> <isHeader>")
      System.exit(1)
    }

    val spark = SparkUtils.SaprkSessionSP("yunchen")
    val InputFilePath = args(0)
    val OutPutFilePath = args(1)
    val srcDataFormat = args(2)
    val dstDataFormat = args(3)
    val isHeader = args(4)

    SparkUtils.readAndWriteDataFromHDFS(spark,srcDataFormat,dstDataFormat,InputFilePath,OutPutFilePath,isHeader)

  }

}
