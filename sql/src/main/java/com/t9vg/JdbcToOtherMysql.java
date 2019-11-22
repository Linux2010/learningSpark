package com.t9vg;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.*;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author tianfusheng
 * @e-mail linuxmorebetter@gmail.com
 * @date 2019/8/26
 */
public class JdbcToOtherMysql {

    public static void main(String[] args) {

        String master = "local";
        String appName = "SparkJdbcDemo";
        SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName(appName)
                .config("spark.some.config.option", "some-value")
                .config("spark.driver.cores","1")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        SQLContext sqlContext = new SQLContext(sc);

        //创建配置文件
        Properties connectionProperties = new Properties();
        //connectionProperties.setProperty("dbtable", "test01");
        connectionProperties.setProperty("user", "root");
        connectionProperties.setProperty("password","root");
        connectionProperties.setProperty("driver","com.mysql.jdbc.Driver");


        JavaRDD<String> personData = sc.parallelize(Arrays.asList("1 tom 5 ss", "2 jim 6 ss", "3 hh 9 ss", "4 zz 10 ss", "5 yy 1 ss"));
        JavaRDD<Row> personsRDD = personData.map(new Function<String,Row>(){
            public Row call(String line) throws Exception {
                String[] splited = line.split(" ");
                return RowFactory.create(Integer.valueOf(Integer.valueOf(splited[0])),splited[1],Integer.valueOf(splited[2]),splited[3]);
            }
        });
        List structFields = new ArrayList();
        structFields.add(DataTypes.createStructField("id",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType    ,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("address",DataTypes.StringType,true));


        StructType structType = DataTypes.createStructType(structFields);
        Dataset<Row> persistDF = sqlContext.createDataFrame(personsRDD,structType);
        persistDF.write().mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/demo","person",connectionProperties);



        Dataset<Row> drdsDF = sqlContext.read().jdbc("jdbc:mysql://localhost:3306/demo", "person", connectionProperties);
        drdsDF.show();
        drdsDF.createOrReplaceTempView("person");
        Dataset<Row> result = spark.sql("SELECT * FROM person");
        result.show();

        sc.close();
    }

}