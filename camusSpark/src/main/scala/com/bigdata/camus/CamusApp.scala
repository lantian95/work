package com.bigdata.camus


import java.text.SimpleDateFormat
import java.util
import java.util.Locale
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import scala.collection.mutable.ArrayBuffer
import com.bigdata.kafka.KafkaIterator
import org.apache.spark.SparkContext._
import kafka.message.{MessageAndMetadata, MessageAndOffset}
import kafka.serializer.{StringDecoder, Decoder}
import org.apache.hadoop.io.compress.GzipCodec

import org.apache.spark.{SparkConf, SparkContext}


class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
     key.asInstanceOf[String]+ "_" +name
}

object CamusApp {

  def getLogHour(log :String):String ={
    val sdf: SimpleDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
    val sdf2: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd/HH")
    val index1: Int = log.indexOf("[")
    if (index1 < 0) {
      return null
    }
    val index2: Int = log.indexOf("]")
    if (index2 < index1) {
      return null
    }
    val time: String = log.substring(index1 + 1, index2)
    val timeL: Long = sdf.parse(time).getTime
    val hour: String = sdf2.format(timeL)
    hour
  }
  def main(args: Array[String]){ 
    val input_path = "/user/offset/offset.info"
    val conf = new SparkConf().setAppName("readKafka")
    conf.set("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      .set("spark.kryoserializer.buffer.mb", "512")
      .set("spark.storage.memoryFraction", "0.4")
      .set("spark.default.parallelism", "72")
      .set("spark.shuffle.compress", "true")
      .set("spark.shuffle.manager", "SORT")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.akka.frameSize", "512")
      .set("spark.rdd.compress", "true")
      .set("spark.ui.port", "4055")
    val sc = new SparkContext(conf)
    sc.addSparkListener(new JobListener(sc))
    val kafka_brokers ="192.168.100.60:9092,192.168.100.61:9092,192.168.100.62:9092"
    val kafkaParams = new util.HashMap[String, String]
    kafkaParams.put("metadata.broker.list", kafka_brokers)
    val topics ="all_topic,topic1,topic2"
    val white_topics = topics.split(",")
    val output_path = "/user/result"
    val rdd = sc.textFile(path_prefix+input_path)
      .map(line => EtlRequest(line, white_topics)).filter(etlRequest => etlRequest!=null).repartition(360)
      .flatMap{
          etlRequest=>
            val topic = etlRequest.topic
            val partitionId = etlRequest.partition
            val startOffset = etlRequest.startOffset
            val endOffset = etlRequest.endOffset
            val iter = KafkaIterator.apply(kafkaParams, topic, partitionId, startOffset, endOffset)
            val messages = new ArrayBuffer[(String, String)]()
            while(iter.hasNext) {
              val msg: MessageAndOffset = iter.next()
              val currentOffset = msg.offset
              val keyDecoder = new StringDecoder(iter.kc.config.props)
              val valueDecoder = new StringDecoder(iter.kc.config.props)
              val mm = new MessageAndMetadata[String, String](topic, partitionId, msg.message, msg.offset, keyDecoder, valueDecoder)
              val value = mm.message.toString
              val hour = getLogHour(value)
              hour match {
               case hour:String =>
                 messages.append((hour+"_"+hour.replace("/", "-"), value))
               case _ => println("hour not String")
              }

            }
           messages
      }
       //println(rdd.count())
    //rdd.saveAsTextFile(path_prefix+output_path, classOf[GzipCodec])
    rdd.saveAsHadoopFile(output_path, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat], classOf[GzipCodec])
    sc.stop()
  }


}
