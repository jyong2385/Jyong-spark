package com.jyong.spark.basic;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CreateDataSetFromHive {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaSparkHive")
                .master("local")
                .config("hive.metastore.uris","thrift://c7node1:9083")
                .enableHiveSupport()
                .getOrCreate();

//        com.jyong.scala.com.jyong.scala.spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive");
//        com.jyong.scala.com.jyong.scala.spark.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src");
        
        Dataset<Row> sql = spark.sql("SELECT regexp_extract('{a:1,b:3,app_name:lisi,age:11}', 'app_name:([^,]+)', 1) AS app_name ");
                sql.show();
    }
}
