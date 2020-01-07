package work7;


import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 数据清洗工作
  */
object Format {

  val SOURCE_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
  val TARGET_TIME_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  def parse(time: String) = {
    TARGET_TIME_FORMAT.format(new Date(getTime(time)))
  }
  def getTime(time: String) = {
    try {
      SOURCE_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]"))).getTime
    } catch {
      case e: Exception => {
        0l
      }
    }
  }

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    val spark=SparkSession.builder().appName("FormatAccess").master("local[*]").getOrCreate()
    val text=spark.sparkContext.textFile("quickStart/src/main/scala/work7/access.log")
    text.map(line=>{
      val splits=line.split(" ")
      if(splits.size>11){
        val ip=splits(0)
        val time=splits(3)+" "+splits(4)
        val url=splits(11).replaceAll("\"","")
        val traffic=splits(9)
        parse(time)+"\t"+url+"\t"+traffic+"\t"+ip
      }
    }).saveAsTextFile("quickStart/src/main/scala/work7/result1")
    spark.stop()
  }



}
