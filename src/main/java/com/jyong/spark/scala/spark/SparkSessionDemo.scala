package com.jyong.spark.scala.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 *
 * Created by jyong on 2021/1/23 13:12
 *
 */
object SparkSessionDemo {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\appinstall\\hadoop_home")
    val conf = new SparkConf()
    conf.set("hive.metastore.uris", "thrift://192.168.92.103:9083")

    val sparkSession = SparkSession.builder().appName("demo").config(conf).master("local[*]").enableHiveSupport().getOrCreate()

    sparkSession.sparkContext.setLogLevel("warn")


    val frame = sparkSession.sql("select * from student")


    frame.show()


  }
}
