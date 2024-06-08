package com.jyong.spark.basic;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by jyong on 2021/1/23 9:41
 */
public class DFtoJson {


    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\appinstall\\winutils-master\\hadoop-2.6.3");
        SparkSession session = SparkSession.builder().appName("job").master("local[*]").getOrCreate();

        Dataset<Row> rowDataset = session.read().json("D:\\data\\people.json");
        //直接写入，造成null值的key被过滤掉
        rowDataset.write().format("JSON")
                .option("spark.sql.jsonGenerator.ignoreNullFields", "true").json("D:\\data\\people3.json");


    }
}
