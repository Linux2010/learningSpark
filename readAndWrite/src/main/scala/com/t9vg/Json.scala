package com.t9vg
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.json4s.jackson.Serialization
import org.json4s.ShortTypeHints
import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats
/**
  * @author tianfusheng
  * @e-mail linuxmorebetter@gmail.com
  * @date 2019/7/31
  */
object Json {
  def main(args: Array[String]): Unit = {
    // 第二种方法解析json文件
    val conf = new SparkConf().setAppName("Json").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")  // 设置日志显示级别
    implicit val formats = Serialization.formats(ShortTypeHints(List()))
    val input = sc.textFile("readAndWrite/src/main/resources/pandainfo.json")
    input.collect().foreach(x=>{
      var c = parse(x).extract[Panda]
      println(c.name+","+c.lovesPandas)
    })
    case class Panda(name:String,lovesPandas:Boolean)

    // 保存json
    val datasave = input.map{  myrecord =>
      implicit val formats = DefaultFormats
      val jsonObj = parse(myrecord)
      jsonObj.extract[Panda]
    }
    datasave.saveAsTextFile("readAndWrite/src/main/resources/savejson")
  }
}
