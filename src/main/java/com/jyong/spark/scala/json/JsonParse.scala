package com.jyong.spark.scala.json

import cn.hutool.json.JSONUtil

object JsonParse {

  def main(args: Array[String]): Unit = {

    val str="{\"age\":19,\"name\":\"lisi\",\"class\":\"EL\",\"math\":100}"
    val nObject = JSONUtil.parseObj(str)

    val age = nObject.getInt("age",0)
    val classs = nObject.getStr("class","wangwu")
    println(s"$classs ============== $age")
    println("移除元素"+nObject.remove("name"))
    nObject.remove("class")
    import scala.collection.JavaConversions._
    val sum = nObject.values().map(r=>r.asInstanceOf[Int]).sum
    println(sum)
    println(Int.MaxValue)

  }
}
