package work4

import org.apache.spark.sql.SparkSession

object ShotTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("TicketTest").master("local[*]").getOrCreate()
    val df =spark.read.format("csv").option("header","true").csv("E:\\IdeaProjects\\learningSpark\\quickStart\\src\\main\\scala\\work4\\shot_logs.csv")

    df.createOrReplaceTempView("shot")
    df.printSchema()
  }
}
