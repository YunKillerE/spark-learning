package test

import Utils.SparkUtils
import Utils.SparkReadFromRDBMS
import org.apache.spark.sql.types._

/**
  *
  * Created by yunchen on 2017/7/24.
  * 提交：
  * /opt/spark2/bin/spark-submit --driver-class-path mysql-connector-java-5.1.38-bin.jar --jars mysql-connector-java-5.1.38-bin.jar --master yarn --deploy-mode client --class test.SparkReadWriteRDBMS spark-learning.jar
  *
  * 注意提交时一定要加--driver-class-path
  *
  */
object SparkReadWriteRDBMS {

  def main(args: Array[String]): Unit = {

    //spark:SparkSession, jdbcUrl:String, user:String, password:String

    if(args.length < 0){
      System.err.println("Usage: <InputFilePath> <OutPutFilePath> <srcDataFormat> <dstDataFormat> <isHeader>")
      System.exit(1)
    }

    val spark = SparkUtils.SaprkSessionSP("spark read from rdbms")

    val jdbcUrl = "jdbc:mysql://zjdw-pre0064:3306/hive"
    val user = "root"
    val password = "hadoop"

    val srfr = new SparkReadFromRDBMS(spark,jdbcUrl,user,password)

    println("读取表SDS成DF,然后存储在/tmp/sds...........")
    srfr.readJdbcDF("SDS","hdfs://zjdw-pre0065:8020/tmp/sds","500")
    println("分区读取表SDS成DF,然后存储在/tmp/sds2...........")
    srfr.readJdbcDF("SDS","hdfs://zjdw-pre0065:8020/tmp/sds2","SD_ID","10","1","2000","1000")

    /*
    *     创建dataframe的集中方式:
    *   1,通过Seq里面放个数组然后toDF加上结构，就可以创建一个测试DF，注意这里要import spark.implicits._，否则会报错
    *   2，通过structtype定义结构，然后读取数据加载这个结构
    *   3，直接读取有结构的源数据，比如jdbc/json/csv/parquet/orc等
    */


    //1
    import spark.implicits._ //解决toDF报错
    val acTransDF1 = Seq(
      (10001, "ppppppp", 1111, 13999999999D, "fdsa", "fdsa"), (10101, "ppppppp", 222, 13999989999D, "fdsa", "fdsa")
    ).toDF("s_suppkey", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment")

    //2
    val schema = StructType(
      StructField("id", LongType, nullable = false) ::
        StructField("name", StringType, nullable = false) ::
        StructField("score", DoubleType, nullable = false) :: Nil)

    //3
    val acTransDF3 = SparkUtils.readAndDataFromHDFSToDF(spark,"csv","hdfs://zjdw-pre0065:8020/user/root/index_data.csv","true")


    println("将测试DF写入到mysql..........")
    srfr.writeJdbcDF(acTransDF3,"indexData","append")

  }

}
