package streaming
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._

object demo {
  val conf = new
      SparkConf().setAppName("streaming demo")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(30))
  ssc.checkpoint("file:///Users/t1mon/tmp/wc/wc_sc_ckpt")
  val lines = ssc.socketTextStream("localhost", 11324)
  val result = lines.flatMap(_.split(" ")).map(w => (w, 1)).updateStateByKey((values: Seq[Int], state: Option[Long]) => {
    // 从value中获取累加值
    val sum = values.sum
    // 获取以前的累加值
    val oldStateSum = state.getOrElse(0L)
    // 更新状态值并返回
    Some(oldStateSum + sum)
  })

  val topN = result.transform(rdd => {
    val list = rdd.sortBy(_._2, false).take(10)
    rdd.filter(list.contains(_))
  })
  topN.print
  topN.repartition(1).saveAsTextFiles("file:///Users/t1mon/tmp/wc_sc_res") //将分区设置为1，方便输出的文件中查看结果
  ssc.start()
}
