package work7

import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
object top {
  def main(args:Array[String]){
    val conf = new SparkConf().setAppName("top").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("/Users/mac/workspace/learningSpark/quickStart/src/main/scala/work7/access.log", 1)


    //计算top10
    val url = lines.filter(_.split(" ").size > 10).map(x => (x.split(" ")(10),1))
    val t1 = url.filter(_._1.length()>10)
    val result = t1.reduceByKey(_+_)
    val sortURL = result.sortBy(_._2,false)
    val res = sortURL.take(10)
    res.foreach(println(_))


    //计算pv
    val pv = lines.count()
    println("pv: "+ pv)
  }
}