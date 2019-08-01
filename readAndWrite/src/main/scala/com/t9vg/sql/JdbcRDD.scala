package com.t9vg.sql

import java.sql.{Connection, DriverManager, ResultSet}
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author tianfusheng
  * @e-mail linuxmorebetter@gmail.com
  * @date 2019/8/1
  */
object JdbcRDD {
  def main(args: Array[String]) {
    //val conf = new SparkConf().setAppName("spark_mysql").setMaster("local")
    val sc = new SparkContext("local","JdbcRDD")

    def createConnection() = {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root")
    }

    def extractValues(r: ResultSet) = {
      (r.getString(1), r.getString(2))
    }

    val data = new JdbcRDD(sc, createConnection, "SELECT id,name FROM person where ? <= ID AND ID <= ?", lowerBound = 0, upperBound =5, numPartitions = 1, mapRow = extractValues)

    println(data.collect().toList)

    sc.stop()
  }

}
