package Base

import org.apache.spark.sql.SparkSession

/**
  * Created by yunchen on 2017/5/8.
  */
class SparkUtils {

  def SaprkSessionSC(appName:String):SparkSession = {

    val spark = SparkSession
      .builder
      .appName(appName)
      .getOrCreate()

    return spark
  }

}
