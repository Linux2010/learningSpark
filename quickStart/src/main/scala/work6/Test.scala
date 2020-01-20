package work6

import org.apache.spark.sql.SparkSession

/**
  * @author tianfusheng
  * @e-mail linuxmorebetter@gmail.com
  * @date 2019/12/27
  */
object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameApp").master("local").getOrCreate()
//    val employeeDF = spark.read.format("json").load("usr/local/spark/examples/employee.json")
//    employeeDF.createOrReplaceTempView("employee")
//    //1
//    employeeDF.show(10)
//    val employeeDFdist= employeeDF.distinct()
//    employeeDFdist.show(10)
//    //2
//    employeeDF.select("name","age").show(10)
//    //3
//    employeeDF.where("age>30").show(10)
//
//    //4
//    spark.sql("select name as username from employee")
//
//
//    //5
//
//
//
    val name =Array(Tuple2(1,"spark"),Tuple2(2,"tachyon"),Tuple2(3,"hadoop"))
    val score = Array(Tuple2(1,100),Tuple2(2,90),Tuple2(3,80))
    val sc = spark.sparkContext

    val namerdd = sc.parallelize(name)
    val scorerdd = sc.parallelize(score)
    val result = namerdd.join(scorerdd)
    result.foreach(s=>println(s))
    result.collect.foreach(println)
    println(    result.count()
    )
    println(    result.take(3)
    )



    val groupTopNRdd=sc.parallelize(Array("aa 11","bb 11","aa 22","bb 33"))
    val tupleRdd = groupTopNRdd.map(s=>{
      val arr = s.split(" ")
      Tuple2(arr(0),arr(1))
    })


  }





}
