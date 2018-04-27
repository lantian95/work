package com.bigdata.camus


case class EtlRequest(topic: String, partition: Int, startOffset: Long, endOffset: Long) {
}

object EtlRequest {
  val empty = null
  def convert(str: String, white_topics:Array[String]):EtlRequest={
    val index1: Int = str.indexOf("{")
    val index2: Int = str.lastIndexOf("}")
    val str1: String = str.substring(index1 + 1, index2)
    val str2: Array[String] = str1.split(",")
    val topic: String = str2(0).split("=")(1)
    if (! white_topics.contains(topic)){
      return empty
    }
    val partition: Int = str2(1).split("=")(1).toInt
    val startOffset: Long = (str2(2).split("=")(1)).toLong
    val endOffset: Long = (str2(3).split("=")(1)).toLong
    if (startOffset >= endOffset){
      return empty
    }
    println(topic, partition, startOffset, endOffset)
    EtlRequest(topic, partition, startOffset, endOffset)
  }
  def apply(line:String, white_topics:Array[String]):EtlRequest={
    convert(line, white_topics)
  }
}
