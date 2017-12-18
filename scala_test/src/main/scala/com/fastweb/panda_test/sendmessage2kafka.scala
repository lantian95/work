package com.fastweb.panda_test

import java.util.Properties
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.producer.KeyedMessage
import kafka.producer.Producer
import kafka.producer.ProducerConfig
import kafka.serializer.StringDecoder
import scala.util.Random
/**
  * Created by Administrator on 2017/7/27.
  */
object sendmessage2kafka {
  def main(args: Array[String]): Unit = {

    if (args.length != 5) {
      System.err.println("Usage: KsswsAnalysisApp <p1:read_topic, p2:write_topic, p3:zk, p4:consumer_group, p5:max_rate>")
      System.exit(1)
    }

    val Array(topics_in, topics_out, zookeeper, consumer_group, max_rate) = args

    val sparkConf = new SparkConf().setAppName("kscloud_slave3kafka_slave224kafka")

    sparkConf.set("spark.akka.frameSize", "512")
      .set("spark.kryoserializer.buffer.mb", "512")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.streaming.unpersist", "true")
      .set("spark.cleaner.ttl", "400")
      .set("spark.ui.port", "4090")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.storage.memoryFraction", "0.3")
      .set("spark.rdd.compress", "true")
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      .set("spark.streaming.blockInterval", "5000")
      .set("spark.streaming.receiver.maxRate", max_rate)
      .set("spark.shuffle.manager", "SORT")

    val streamingContext = new StreamingContext(sparkConf, Seconds(60))

    val sparkContext = streamingContext.sparkContext

    val kafka_config = Map(
      //192.168.100.26,192.168.100.27,192.168.100.28,192.168.100.29,192.168.100.30
      "zookeeper.connect" -> zookeeper,
      "group.id" -> consumer_group,
      "auto.offset.reset" -> "largest")

    val topic_array = topics_in.split(",")

    val topic_size = topic_array.size * 12

    val kafkaRDD = (0 to topic_size - 1).map(i => {
      val topic_map = Map(topic_array(i / 12) -> 1)
      val kafka_stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](streamingContext, kafka_config, topic_map, StorageLevel.DISK_ONLY)
        .filter(x => x._2.contains("KS_FWLOG_TAG")).map { x =>
        val line = x._2.replaceAll("\\\\\"", "\"")
        val hostname = line.substring(line.indexOf("\"hostname\":\"", 0) + "\"hostname\":\"".length(), line.indexOf("\",", line.indexOf("\"hostname\":\"", 0) + "\"hostname\":\"".length()))
        val si = line.substring(line.indexOf("\"si\":\"") + 6, line.indexOf("\",", line.indexOf("\"si\":\"") + 6))

        val index = line.indexOf("\",\"", line.indexOf("KS_FWLOG_TAG", 0))

        "CDNLOG_cache " + hostname + " " + line.substring(line.indexOf("\"ori\":\"", 0) + 7, line.lastIndexOf(" ", index) + 1) + si + " " + si
      }
      kafka_stream
    })

    val datas = streamingContext.union(kafkaRDD);

    val ss = datas.foreachRDD { messages =>

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
            val kmessage = new KeyedMessage[String, String](topics_out, String.valueOf(random.nextInt()), line)
            producer.send(kmessage)
          })
        } catch {
          case t: Throwable => t.printStackTrace()
        } finally {
          producer.close()
        }
      }
      /*
      try {
        messages.foreach(line => {
          val kmessage = new KeyedMessage[String, String](topics_out, String.valueOf(random.nextInt()), line)
          producer.send(kmessage)
        })
      } catch {
        case t: Throwable => t.printStackTrace()
      } finally {
        producer.close()
      }*/
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
