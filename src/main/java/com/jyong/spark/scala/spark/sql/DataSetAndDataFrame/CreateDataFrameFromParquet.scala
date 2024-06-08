package com.jyong.spark.scala.spark.sql.DataSetAndDataFrame

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object CreateDataFrameFromParquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("createdataframefromparquet").getOrCreate()
//    val df1: DataFrame = com.jyong.scala.com.jyong.scala.spark.read.com.jyong.scala.json("./data/com.jyong.scala.json")
    val df1: DataFrame = spark.read.format("json").load("./data/com.jyong.scala.json")
    df1.show()

    /**
      * 保存成parquet文件
      *  Append: 如果文件存在就追加
      *  Overwrite:覆盖写
      *  Ignore:忽略
      *  ErrorIfExists：报错
      */
    df1.write.mode(SaveMode.Append).format("parquet").save("./data/parquet")

    /**
      * 读取parquet文件
      *
      */
//    val df2: DataFrame = com.jyong.scala.com.jyong.scala.spark.read.parquet("./data/parquet")
    val df2 = spark.read.format("parquet").load("./data/parquet")
    val count: Long = df2.count()
    println(count)
    df2.show(100)
  }
}
