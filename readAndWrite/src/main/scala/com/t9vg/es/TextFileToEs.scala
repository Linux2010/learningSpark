package com.t9vg.es
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * @author tianfusheng
  * @e-mail linuxmorebetter@gmail.com
  * @date 2019/8/1
  */
object TextFileToEs {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TextFileToEs").setMaster("local[2]")
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes", "172.16.60.42")
    sparkConf.set("es.port", "9200")
    sparkConf.set("es.index.read.missing.as.empty", "true")
    sparkConf.set("es.nodes.wan.only", "true")
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder().getOrCreate().sqlContext


    val peopleRDD = spark.sparkContext.textFile("readAndWrite/src/main/resources/1.txt")
    val schemaString = "id name age"

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0).trim, attributes(1),attributes(2).trim))
    val peopleDF = spark.createDataFrame(rowRDD, schema)

    peopleDF.printSchema()
    peopleDF.show
    // Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")

    // SQL can be run over a temporary view created using DataFrames
    val results = spark.sql("SELECT name FROM people")

    results.show()
    EsSparkSQL.saveToEs(peopleDF, "text_file_to_es/people")

  }


}
