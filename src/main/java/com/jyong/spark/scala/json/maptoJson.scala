package com.jyong.spark.scala.json

import scala.util.parsing.json.{JSON, JSONObject}

object maptoJson {

  def main(args: Array[String]): Unit = {


  }

  def mapToJson(map: Map[String,String])={
    JSONObject(map).toString()
  }


  def jsonToMap(json:String)={
    JSON.parseFull(json).get.asInstanceOf[Map[String,String]]
  }
}
