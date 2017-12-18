package com.fastweb.panda_test

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Time
import org.elasticsearch.spark.sparkRDDFunctions
import kafka.serializer.StringDecoder
import kafka.common.TopicAndPartition
import scala.util.parsing.json.JSON
import scala.collection.mutable.ArrayBuffer
import java.util.Properties
import kafka.producer.KeyedMessage
import kafka.producer.Producer
import kafka.producer.ProducerConfig
import scala.util.Random
import org.apache.spark.streaming.kafka.{DirectKafkaUtils, KafkaCluster}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}


object FastmediaCsApp {
  def main(args: Array[String]) {

    if (args.length != 7) {
      System.err.println("Usage: PandatvApp <param1:topics> <param2:elasticsearch_nodes> <param3:zookeeper_nodes> <param4:consumer_group> <param5:streaming_window> <param6:spark_streaming_receiver_maxRate>")
      System.exit(1)
    }

    val Array(topics, esNode, kafka, consumer_group, window, maxrate, port) = args

    val win = Integer.valueOf(window)

    val sparkConf = new SparkConf().setAppName("FastmediaCsAppTest1")
    sparkConf.set("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      .set("spark.streaming.kafka.maxRatePerPartition", maxrate)
      .set("spark.streaming.blockInterval", "30000")
      .set("spark.kryoserializer.buffer", "512m")
      .set("spark.storage.memoryFraction", "0.4")
      .set("spark.streaming.unpersist", "true")
      .set("spark.default.parallelism", "120")
      .set("spark.shuffle.compress", "true")
      .set("spark.shuffle.manager", "SORT")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.rpc.message.maxSize", "512")
      .set("es.index.auto.create", "true")
      .set("spark.rdd.compress", "true")
      .set("spark.cleaner.ttl", String.valueOf(win * 5 * 60))
      .set("spark.ui.port", port)
      .set("es.nodes", esNode)

    val kafkaConfig = Map[String, String](
      "bootstrap.servers" -> kafka,
      "group.id" -> consumer_group,
      "auto.offset.reset" -> "largest")

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(win * 60))

    val topic_set = topics.split(",").toSet
    val logs = DirectKafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConfig, topic_set) /*.filter(log => log._2.contains("PUBLISH"))*/
                                .map(x => FastmediaCs(x._2, Channel.update())).filter(x => x != null)

    val formate = new SimpleDateFormat("yyyyMMdd")

    def foreachCs = (rdd: RDD[((String, String, String), Long)], time: Time) => {
      val indexPart = formate.format(new Date(time.milliseconds))
      rdd.map(x => Map(
        "domain" -> x._1._1,
        "userid" -> x._1._2,
        "timestamp" -> x._1._3,
        "cs" -> x._2)).saveToEs("fastmedia_cs_test1/" + indexPart)
    }

    var logsWindow = logs.window(Minutes(win.toLong), Minutes(win.toLong))

    var cs = logsWindow.filter { x => "publish".equals(x.flag) }.map { x => ((x.domain, x.userid, x.timestamp), x.cs) }.reduceByKey(_ + _).foreachRDD(foreachCs)

    /*
   val ss = logs.foreachRDD { messages =>
     messages.foreachPartition { lines =>

       val random = new Random();
       val props = new Properties()
       props.put("metadata.broker.list", "slave224:9092,slave226:9092,slave227:9092,slave228:9092,slave229:9092,slave230:9092,slave231:9092,slave232:9092,slave233:9092,slave234:9092,slave235:9092,slave236:9092")
       props.put("serializer.class", "kafka.serializer.StringEncoder")
       props.put("key.serializer.class", "kafka.serializer.StringEncoder")
       props.put("request.required.acks", "-1");
       props.put("compression.codec", "snappy")
       props.put("producer.type", "async")
       props.put("topic.metadata.refresh.interval.ms", "30000")
       props.put("batch.num.messages", "1000")
       props.put("retry.backoff.ms", "500")

       val config = new ProducerConfig(props)
       val producer = new Producer[String, String](config)

       try {
         lines.foreach(line => {
           val kmessage = new KeyedMessage[String, String]("slave224_topic", String.valueOf(random.nextInt()), line.line)
           producer.send(kmessage)
         })
       } catch {
         case t: Throwable => t.printStackTrace()
       } finally {
         producer.close()
       }
     }
   }
   */

    ssc.start()
    ssc.awaitTermination()
  }

}