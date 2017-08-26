package KafkaKudu

import scala.io.Source
import scala.util.parsing.json._

/**
  * Created by yunchen on 2017/8/25.
  */
object main {

  def main(args: Array[String]): Unit = {

    val str2 = "{\"et\":\"kanqiu_client_join\",\"vtm\":1435898329434,\"body\":{\"client\":\"866963024862254\",\"client_type\":\"android\",\"room\":\"NBA_HOME\",\"gid\":\"\",\"type\":\"\",\"roomid\":\"\"},\"time\":1435898329}"

    val file = Source.fromFile("C:\\Users\\yunchen\\Desktop\\yunchen\\spark-learning\\第四章：Spark Streaming\\stds_attack.json").mkString

    val b = JSON.parseFull(file)

    //第一级
    val first = regJson(b)
    println(first.get("_shards").toString.replace("Some(","").replace(")",""))

/*
    //第二级
    val two = first.get("hits")
    println(two)
    val sec = regJson(two)
    println(sec.get("client").toString.replace("Some(","").replace(")",""))
*/

    }

  def regJson(json:Option[Any]) = json match {
        case Some(map: Map[String, Any]) => map
/*        case None => "erro"
        case other => "Unknow data structure : " + other*/
   }


}
