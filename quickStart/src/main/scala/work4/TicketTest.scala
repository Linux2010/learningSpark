package work4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TicketTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("TicketTest").master("local[*]").getOrCreate()
    val df =spark.read.format("csv").option("header","true").csv("E:\\IdeaProjects\\learningSpark\\quickStart\\src\\main\\scala\\work4\\ticket.csv")

    df.createOrReplaceTempView("ticket")
    df.printSchema()

    //1,When are tickets most likely to be issued?
    //dataFrame查询需要的列，转rdd，map方法转为k-v对，reduceByKey方法将v累加，sortBy方法按v倒叙
    val rdd = df.select("Violation Time").rdd.map(word =>(word,1)).reduceByKey(_+_).sortBy(_._2,false)
    println(" tickets most likely to be issued time :"+rdd.first())
    //([0836A],9140)


    //2,What are the most common years and types of cars to be ticketed?
    // dataFrame查询需要的列，别名为year ，where函数过滤0，转rdd，map方法转为k-v对，reduceByKey方法将v累加，sortBy方法按v倒叙
    val years =df.select(col("Vehicle Year").as("year")).where("year != '0'").rdd.map(word=>(word,1)).reduceByKey(_+_).sortBy(_._2,false)
      println("the most common years: "+years.first())
    //the most common years: ([2018],337572)

    //dataFrame查询需要的列，转rdd，map方法转为k-v对，reduceByKey方法将v累加，sortBy方法按v倒叙
    val types = df.select("Vehicle Body Type").rdd.map(word =>(word,1)).reduceByKey(_+_).sortBy(_._2,false)
    println("Vehicle Body Type most : "+types.first())
    //Vehicle Body Type most : ([SUBN],1342211)


    //3,Where are tickets most commonly issued?
    //dataFrame查询需要的列，转rdd，map方法转为k-v对，reduceByKey方法将v累加，sortBy方法按v倒叙
    val ave = df.select("Street Name").rdd.map(word=>(word,1)).reduceByKey(_+_).sortBy(_._2,false)
    println("the most commonly issued :" +ave.first())
    //the most commonly issued :([Broadway],55829)

    //4,Which color of the vehicle is most likely to get a ticket?
    //dataFrame查询需要的列，转rdd，map方法转为k-v对，reduceByKey方法将v累加，sortBy方法按v倒叙
    val color = df.select("Vehicle Color").rdd.map(word=>(word,1)).reduceByKey(_+_).sortBy(_._2,false)
    println("the most likely to get a ticket vehicle color : "+color.first())
    //the most likely to get a ticket vehicle color : ([WH],689716)


  }

}
