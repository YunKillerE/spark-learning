package Utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by yunchen on 2017/5/8.
  */

object SparkUtils {

  def SaprkSessionSP(appName:String):SparkSession = {
    val spark = SparkSession
      .builder
      .appName(appName)
      .getOrCreate()

    return  spark
  }

  def SaprkSessionSC(appName:String):SparkContext = {
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)

    return sc
  }

}
