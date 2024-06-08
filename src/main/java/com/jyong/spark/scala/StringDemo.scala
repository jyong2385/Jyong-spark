package com.jyong.spark.scala

/**
 * @author jyong
 * @date 2022年09月03日 15:42
 * @desc:
 */
object StringDemo {

  def main(args: Array[String]): Unit = {
    val list = List("a","b","c")
    println(list)
  }
  def quoteIdentifier(colName: String): String = {
    println(colName)
    s""""$colName""""
  }

}