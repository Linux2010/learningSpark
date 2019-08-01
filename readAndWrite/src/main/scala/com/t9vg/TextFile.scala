package com.t9vg

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author tianfusheng
  * @e-mail linuxmorebetter@gmail.com
  * @date 2019/7/31
  */
object TextFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("textFile")
    val sc = new SparkContext(conf)
    //read
    val text = sc.textFile("readAndWrite/src/main/resources/1.txt")
    //遍历输出到控制台
    text.foreach(x=>println(x))
    //write
    text.saveAsTextFile("readAndWrite/src/main/resources/2")
  }
}
