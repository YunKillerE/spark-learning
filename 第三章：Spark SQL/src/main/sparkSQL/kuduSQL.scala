package sparkSQL

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.kudu.spark.kudu.{KuduContext, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

/**
  * Created by yunchen on 2017/9/28.
  */
object kuduSQL {

  def main(args: Array[String]): Unit = {

    val masterList = "cmname1:7051"
    val kuduTable = "impala::default.tsgz_stds"

    val spark = SparkSession.builder.appName("kuduSQL").enableHiveSupport().getOrCreate()

    val kuduContext = new KuduContext(masterList)

    val kuduDF = spark.read.options(Map("kudu.master" -> masterList,"kudu.table" -> kuduTable)).kudu

    val tempkuduDF = kuduDF.withColumn("newday",lit(getDat(1))).withColumn("newtim",lit(getTime(1)))

    //test
    tempkuduDF.select(tempkuduDF("newday").as("stds_day"),tempkuduDF("stds_id"),tempkuduDF("stds_srcip")
      ,tempkuduDF("stds_dstip"),tempkuduDF("stds_rulename"),tempkuduDF("newtim").as("stds_timedate"))
      .limit(10).show(false)

    /**
      *
      * CREATE TABLE default.stds10wan (
   stds_id STRING ,
   stds_timedate STRING,
   stds_srcip STRING,
   stds_dstip STRING,
   stds_rulename STRING,
   PRIMARY KEY (stds_id)
 )
 PARTITION BY HASH (stds_id) PARTITIONS 2
 STORED AS KUDU

 CREATE TABLE default.stds100wan (
   stds_id STRING ,
   stds_timedate STRING,
   stds_srcip STRING,
   stds_dstip STRING,
   stds_rulename STRING,
   PRIMARY KEY (stds_id)
 )
 PARTITION BY HASH (stds_id) PARTITIONS 2
 STORED AS KUDU


CREATE TABLE default.stds_range_hash_100 (
   stds_id STRING ,
   stds_day STRING,
   stds_timedate STRING,
   stds_srcip STRING,
   stds_dstip STRING,
   stds_rulename STRING,
   PRIMARY KEY (stds_id,stds_day)
 )
 PARTITION BY HASH (stds_id) PARTITIONS 20,
 RANGE(stds_day)
(
  PARTITION VALUE = ("2017-09-01"),
  PARTITION VALUE = ("2017-09-02"),
  PARTITION VALUE = ("2017-09-03")
)
 STORED AS KUDU;


 ALTER TABLE stds_range_hash_100 ADD RANGE PARTITION VALUE = "2017-09-04";
 ALTER TABLE stds_range_hash_100 ADD RANGE PARTITION VALUE = "2017-09-05";
 ALTER TABLE stds_range_hash_100 ADD RANGE PARTITION VALUE = "2017-09-06";
 ALTER TABLE stds_range_hash_100 ADD RANGE PARTITION VALUE = "2017-09-07";
 ALTER TABLE stds_range_hash_100 ADD RANGE PARTITION VALUE = "2017-09-08";
 ALTER TABLE stds_range_hash_100 ADD RANGE PARTITION VALUE = "2017-09-09";
 ALTER TABLE stds_range_hash_100 ADD RANGE PARTITION VALUE = "2017-09-10";
      */


    /**
      * for (i <- Range(1,11)){
println("now is :" + i)
var tempdf = df1.withColumn("newday",lit(getDat(i))).withColumn("newtim",lit(getTime(i))).select('newday as 'stds_day,'stds_id,'stds_srcip,'stds_dstip,'stds_rulename,'newtim as 'stds_timedate).limit(1000000)
tempdf.write.options(Map("kudu.master"-> "cmname1:7051", "kudu.table"-> "impala::default.stds_range_hash_100")).mode("append").kudu
}
      */

    //插入stds_range_hash_100表中
    for (i <- Range(1,11)){
      println("now is :" + i)
      val tempdf = kuduDF.withColumn("newday", lit(getDat(i))).withColumn("newtim", lit(getTime(i)))
        .select(tempkuduDF("newday").as("stds_day"), tempkuduDF("stds_id"), tempkuduDF("stds_srcip")
          , tempkuduDF("stds_dstip"), tempkuduDF("stds_rulename"), tempkuduDF("newtim").as("stds_timedate")).limit(1000000)
      tempdf.write.options(Map("kudu.master"-> "cmname1:7051", "kudu.table"-> "impala::default.stds_range_hash_100")).mode("append").kudu
    }


    /** 不知道为什么在代码中不能采用这种方式进行写，在repl中却可以
      * var temp = df
for (i <- Range(1,11)){
println("now is :" + i)
val t = temp.withColumn("newtim",lit(getTime(i))).select('stds_id,'stds_srcip,'stds_dstip,'stds_rulename,'newtim as 'stds_timedate).limit(1000000)
t.write.options(Map("kudu.master"-> "cmname1:7051", "kudu.table"-> "impala::default.stds100wan")).mode("append").kudu
temp = df.except(t)
println(temp.count())
}
      */

    //插入stds100wan表中
    var temp = kuduDF

    for (i <- Range(1,11)){
      println("now is :" + i)
      val t = temp.withColumn("newtim",lit(getTime(i)))
      val t1 = t.select(t("stds_id"),t("stds_srcip"),t("stds_dstip"),t("stds_rulename"),t("newtim").as("stds_timedate")).limit(1000000)
      t1.write.options(Map("kudu.master"-> "cmname1:7051", "kudu.table"-> "impala::default.stds100wan")).mode("append").kudu
      temp = temp.except(t1)
      println(temp.count())
    }


    /**
      *  CREATE TABLE default.netflow (
   netflow_id STRING ,
   netflow_srcip STRING,
   netflow_dstip STRING,
   netflow_timedate STRING,
   PRIMARY KEY (netflow_id)
 )
 PARTITION BY HASH (netflow_id) PARTITIONS 2
 STORED AS KUDU


       val colArray=Array("stds_dstip", "stds_srcip")


 df.dropDuplicates(colArray).withColumn("newtim",lit(getTime(1))).select('stds_id as 'netflow_id,'stds_srcip as 'netflow_srcip,'stds_dstip as 'netflow_dstip,'newtim as 'netflow_timedate).write.options(Map("kudu.master"-> "192.168.3.79:7051", "kudu.table"-> "impala::default.netflow")).mode("append").kudu

      */


    val colArray=Array("stds_dstip", "stds_srcip")


    val tmp = kuduDF.dropDuplicates(colArray).withColumn("newtim",lit(getTime(1)))
      //.select('stds_id as 'netflow_id,'stds_srcip as 'netflow_srcip,'stds_dstip as 'netflow_dstip,'newtim as 'netflow_timedate)
    tmp.select(tmp("stds_id").as("netflow_id"),tmp("stds_srcip").as("netflow_srcip"),tmp("stds_dstip").as("netflow_dstip"),tmp("newtim").as("netflow_timedate"))
      .write.options(Map("kudu.master"-> "192.168.3.79:7051", "kudu.table"-> "impala::default.netflow")).mode("append").kudu

  }

  def getTime(day:Int):String={
    var period:String=""
    val cal: Calendar = Calendar.getInstance()
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    cal.set(Calendar.DATE, day)
    period=df.format(cal.getTime())//本月第一天
    period
  }


  def getDat(day:Int):String={
    var period:String=""
    val cal:Calendar =Calendar.getInstance()
    val df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    cal.set(Calendar.DATE, day)
    period=df.format(cal.getTime())//本月第一天
    period
  }

}
