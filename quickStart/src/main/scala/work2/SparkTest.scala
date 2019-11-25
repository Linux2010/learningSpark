package work2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @author tianfusheng
  * @e-mail linuxmorebetter@gmail.com
  * @date 2019/11/24
  */
object SparkTest {

  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder.appName("SparkTest").master("local[*]").getOrCreate()
    val df =spark.read.format("csv").option("header","true").csv("/Users/mac/workspace/learningSpark/quickStart/src/main/scala/work2/Zip_Zhvi_Summary_AllHomes.csv")
    df.printSchema()

    //1，计算并打印，City为 new york的数据数量，以及 new york里zhvi指数排名前10条数据
    //过滤df中的数据,选择city为new york的数据集，并以zhvi进行倒序。
    val df2 = df.filter("City='New York'").orderBy(desc("Zhvi"))
    //打印df2的记录数。
    println("New York count: "+ df2.count())
    //打印前10条
    df2.show(10)

    //计算并打印，10年内，增长率最高的10个城市
    //df.orderBy(df("10Year").desc).show(100)







    //2，以城市为单位计算各区的房价指数均值，并找出排名前十的城市。
    //将df的数据集， 注册为内存表，表名为test
    df.createOrReplaceTempView("test")
    //使用spark-sql 查询 City Zhvi转为Integer类型
    val cityTop10DF = spark.sql("select City, CAST(Zhvi AS INTEGER) from test")
    //打印Schema信息
    cityTop10DF.printSchema()
    //打印 cityTop10DF前十条数据
    cityTop10DF.show(10)
    //将cityTop10DF进行以City分组，并用mean方法求出每组的平均值。这样计算将会把city下所有区的房地产指数进行求平均值，以反应某个城市的 房地产指数
    val cityMean = cityTop10DF.groupBy("City").mean()
    //打印cityMean的schema信息
    cityMean.printSchema()
    //打印cityMean的前十条信息
    cityMean.show(10)
    //将 cityMean数据集，进行按 avg(Zhvi) 进行倒序，并打印前十，以计算出房地产指数均值排名前十的城市
    cityMean.orderBy(desc("avg(Zhvi)")).show(10)


    //3, 求出进10年来年房产指数增长率均值最低的10个城市
    val growth10DF = null;


    //4, 计算房价指数均值峰值落差最大的前10个城市

    //5, 计算最有投资

  }
}
