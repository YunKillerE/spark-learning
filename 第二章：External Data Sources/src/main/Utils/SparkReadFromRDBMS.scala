package Utils

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by yunchen on 2017/7/18.
  *
  * /opt/spark2/bin/spark-shell --driver-class-path mysql-connector-java-5.1.38-bin.jar --jars mysql-connector-java-5.1.38-bin.jar
  *
  */
class SparkReadFromRDBMS(spark:SparkSession, jdbcUrl:String, user:String, password:String) {

  /**
    *
    * 读取一个表创建DF然后写入到hfds，对于数据量大的话可以加入分区增加性能那个，也可以加大fetchsize的值增加读取性能
    *
    * @param dbTable
    * @param fetchsize
    * @return
    */
  //def readJdbcDF(spark:SparkSession, jdbcUrl:String, dbTable:String, user:String, password:String, hdfsWritePath:String, fetchsize:String) = {
  def readJdbcDF(dbTable:String, hdfsWritePath:String, fetchsize:String) = {

    //spark.read.format("jdbc").options(Map("url"->"jdbc:mysql://zjdw-pre0064:3306/hive","dbtable"->"hive.SDS","user"->"root","password"->"hadoop")).load()
    val jdbcDF = spark.read.format("jdbc").options(Map("url"->jdbcUrl,"dbtable"->dbTable,"user"->user,"password"->password,"fetchsize"->fetchsize)).load()

    jdbcDF.count()

    jdbcDF.write.save(hdfsWritePath)

  }

  /**
    *
    * @param dbTable
    * @param hdfsWritePath
    * @param partitionColumn  用于分区的列
    * @param numPartitions  partitionColumn must be a numeric column from the table in question
    * @param lowerBound
    * @param upperBound 这几个参数不怎么理解，可以参考一下JdbcRDD
    *      Given a lowerBound of 1, an upperBound of 20, and a numPartitions of 2,
    *      the query would be executed twice, once with (1, 10) and once with (11, 20)
    *      从源码注释里面看貌似upperBound是最大值，只会读取到这个值
    * @param fetchsize  默认比较小，可以在增大到10000
    */
  def readJdbcDF(dbTable:String, hdfsWritePath:String, partitionColumn:String, numPartitions:String,lowerBound:String, upperBound:String, fetchsize:String) = {

    //spark.read.format("jdbc").options(Map("url"->"jdbc:mysql://zjdw-pre0064:3306/hive","dbtable"->"hive.SDS","user"->"root","password"->"hadoop",
    // "partitionColumn"->"SD_ID","numPartitions"->"10","lowerBound"->"1","upperBound"->"2000","fetchsize"->"1000")).load()
    val jdbcDF = spark.read.format("jdbc").options(Map("url"->jdbcUrl,"dbtable"->dbTable,"user"->user,"password"->password,"partitionColumn"->partitionColumn,
      "numPartitions"->numPartitions,"lowerBound"->lowerBound,"upperBound"->upperBound,"fetchsize"->fetchsize)).load()

    jdbcDF.count()

    jdbcDF.write.save(hdfsWritePath)

  }

  /**
    *
    * 这里只最好先创建好表，否则会按照spark sql字段类型来创建表
    *
    * @param ds
    * @param dbTable
    * @param mode   写入模式append,overwrite,ignore,error
    *
    */
  def writeJdbcDF(ds:DataFrame, dbTable:String,mode:String): Unit = {

    ds.write.format("jdbc").options(Map("url"->jdbcUrl,"dbtable"->dbTable,"user"->user,"password"->password)).save()

  }

}
