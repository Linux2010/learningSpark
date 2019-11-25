package com.t9vg

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)
    val text = sc.textFile("E:\\IdeaProjects\\learningSpark\\quickStart\\src\\main\\resources\\1.txt")
    val rdd =text.flatMap(line =>line.split(",")).map(word =>(word,1))

    //以单词分组，然后进行map取每组中tuple ("a",1)，然后取tuple中的第二字段累加
    val rdd2 =rdd.groupBy(_._1).map(temp=>(temp._1,temp._2.map(word=>word._2).reduceLeft(_+_)))
    rdd2.foreach(s=>println(s))

    //行动算子 ——countByKey
    val rdd3 = rdd.countByKey()
    rdd3.foreach(s=>println(s))

    //行动算子 ——countByValue ，以tuple为单位 ("a",1)
    val rdd4 = rdd.countByValue()
    rdd4.foreach(s=>println(s))


    //mapReduce算法
    val rdd5 = rdd.reduceByKey(_+_)
    rdd5.foreach(s=>println(s))


/*    foldByKey其实就是aggregateByKey简化版，
    当aggregateByKey中分区内和分区间的计算规则一样时，使用foldByKey就可以了
    rdd.aggregateByKey(0)(+,+) ——> rdd.foldByKey(0)(+)*/
    val rdd6= rdd.foldByKey(0)(_+_)
    rdd6.collect().foreach(println)


    /*    转换算子 —— aggregateByKey()()使用了函数柯里化
      存在两个参数列表 :
      第一个参数列表表示分区内计算时的初始值（零值）——在初始值的基础上做比较运算
    第二参数列表中需要传递两个参数
    第一个参数表示分区内计算规则
    第二个参数表示分区间计算规则*/
    val rdd7 = rdd.aggregateByKey(0)((x,y)=>{Math.max(x,y)}, (x,y)=>{x+y})
    rdd7.collect().foreach(println)


    //group后使用map和case后求tuple._2 的和，即为wordcount
    val rdd8 = rdd.groupByKey().map { case (c, datas) => {(c, datas.sum)}}
    rdd8.collect().foreach(println)
  }
}
