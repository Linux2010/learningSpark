package streaming

import org.apache.spark.{SparkConf, SparkContext}

object demo2 {
  val conf = new
      SparkConf().setAppName("streaming demo")
  val sc = new SparkContext(conf)
  var exitFlag = false //声明接收exit指令的标志符
  var appFlag = true //声明全局标志符
  import org.apache.spark.streaming._
  val ssc = new StreamingContext(sc, Seconds(5))
  val lines = ssc.socketTextStream("localhost", 11324)
  val tag = lines.filter(line => line.contains("exit")) //过滤出包含‘exit’指令的line
  tag.foreachRDD(rdd => {if (!rdd.isEmpty()) {exitFlag = true}}) //当存在exit时，将exitFlag修改为true
  tag.print()
  ssc.start()
  while(appFlag){
    if(exitFlag){
      println("接收到了exit指令, 关闭streaming...")
      ssc.stop()
      appFlag = false
    }
  }
}
