package com.fastweb.panda_test

/**
  * Created by hry on 2017/7/5.
  */

import java.text.SimpleDateFormat
import java.util.Date
import kafka.util.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.elasticsearch.spark.sparkRDDFunctions
import kafka.serializer.StringDecoder
import kafka.serializer.Decoder
import kafka.message.MessageAndMetadata
import org.apache.spark.Logging
import kafka.common.TopicAndPartition
import java.util.Properties
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.{DirectKafkaUtils, KafkaCluster, ZooKeeperOffsetsStore}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}

object StoreOffset2ZkApp {
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
      .set("spark.default.parallelism", "240")
      .set("spark.shuffle.compress", "true")
      .set("spark.shuffle.manager", "SORT")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.rpc.message.maxSize", "512")
      .set("es.index.auto.create", "true")
      .set("spark.rdd.compress", "true")
      .set("spark.cleaner.ttl", String.valueOf(win * 5 * 60))
      .set("spark.ui.port", port)
      .set("es.nodes", esNode)
    val zkHosts = "192.168.100.204:2181,192.168.100.205:2181,192.168.100.206:2181,192.168.100.207:2181,192.168.100.208:2181"
    //val zkHosts = "192.168.100.26:2181,192.168.100.27:2181,192.168.100.28:2181,192.168.100.29:2181,192.168.100.30:2181"
    val kafkaConfig = Map[String, String](
      "bootstrap.servers" -> kafka,
      "group.id" -> consumer_group,
      //"zookeeper.connect" -> zkHosts,
      "auto.offset.reset" -> "largest")

    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(win * 60))

    val topic_set = topics.split(",").toSet


    val kafka_logs = DirectKafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConfig, topic_set) //如果未保存，根据 kafkaParam 的配置使用最新或者最旧的 offset

    val logs = kafka_logs.map(x => StoreOffset2Zk(x._2, Channel.update())).filter(x => x != null)
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
    // save the offsets
    val topicDirs = new _root_.kafka.util.ZKGroupTopicDirs(consumer_group, topics) //创建一个 ZKGroupTopicDirs 对象，对保存
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"
    println(zkTopicPath)

    val zkClient = new ZkClient(zkHosts, 30000, 30000, ZKStringSerializer)
    var offsetRanges = Array[OffsetRange]()
    kafka_logs.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //得到该 rdd 对应 kafka 的消息的 offset
      rdd
    }.map(msg => msg._2).foreachRDD { rdd =>
      for (o <- offsetRanges) {
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        println(zkPath)
        println(o.fromOffset.toString)
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString) //将该 partition 的 offset 保存到 zookeeper
        println(s"@@@@@@ topic  ${o.topic}  partition ${o.partition}  fromoffset ${o.fromOffset}  untiloffset ${o.untilOffset} #######")
      }
    }


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