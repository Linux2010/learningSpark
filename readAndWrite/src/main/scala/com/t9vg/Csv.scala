package com.t9vg
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import au.com.bytecode.opencsv.CSVReader
import java.io.StringReader

/**
  * @author tianfusheng
  * @e-mail linuxmorebetter@gmail.com
  * @date 2019/8/1
  */
object Csv {
  def main(args: Array[String]): Unit = {

    // 在Scala中使用textFile()读取CSV
    val conf = new SparkConf().setAppName("Csv").setMaster("local")
    val sc = new SparkContext(conf)
    val inputFile = "readAndWrite/src/main/resources/favourite_animals.csv"//读取csv文件
    val input = sc.textFile(inputFile)
    val result = input.map{
      line => val reader = new CSVReader(new StringReader(line))
        reader.readNext()
    }
    // result.foreach(println)
    for(res <- result)
      for(r <- res)
        println(r)
  }
}
