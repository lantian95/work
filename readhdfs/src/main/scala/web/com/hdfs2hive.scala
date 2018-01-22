package web.com

import java.text.SimpleDateFormat
import java.util.{ArrayList, Date}
import kafka.serializer.StringDecoder
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.{KafkaUtils}
import org.apache.spark.streaming.{Minutes, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}


object hdfs2hive  {
  def main(args: Array[String]) {
    if (args.length != 5) {
      System.err.println("Usage: hdfs2hive <zkQuorum> <group> <topics>  <uiPort>")
      System.exit(1)
    }

    val Array(kafka, group, topics, maxrate, uiPort) = args
    println(kafka, group, maxrate, topics)

    val sparkConf = new SparkConf()
      .setAppName("hdfs2hive")
      .set("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.streaming.unpersist", "true")
      .set("spark.cleaner.ttl", "36000")
      .set("spark.storage.memoryFraction", "0.4")
      .set("spark.rdd.compress", "true")
      .set("spark.shuffle.compress", "true")
      .set("spark.ui.port", uiPort)
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      .set("spark.shuffle.manager", "SORT")
      .set("spark.streaming.kafka.maxRatePerPartition", maxrate) //每个partition 消费的最大数据量
      .set("spark.streaming.backpressure.initialRate", maxrate) //任务初始化消费最大数据量
      .set("spark.streaming.backpressure.enabled", "true") //根据系统负载选择最优消费速率
      .set("spark.streaming.stopGracefullyOnShutdown", "true") //确保任务kill时，能够处理完最后一批数据


    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Minutes(1))
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val kafka_config = Map(
      "bootstrap.servers" -> kafka,
      "group.id" -> group,
      "auto.commit.enable" -> "true",
      "fetch.message.max.bytes" -> ("" + 1024 * 1024 * 10 * 3),
      "auto.offset.reset" -> "largest",
      "rebalance.backoff.ms" -> "10000",
      "rebalance.max.retries" -> "10",
      "zookeeper.connection.timeout.ms" -> "1000000",
      "zookeeper.session.timeout.ms" -> "20000",
      "zookeeper.sync.time.ms" -> "10000")

    val topicList = topics.split(",").toSet

    val kafka_stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafka_config, topicList)


    val logInfos = kafka_stream.map(x => {
      readhdfs2es(x._2.trim())
    }).filter { x => x != null }

    val format = new SimpleDateFormat("yyyyMMddHHmm")
    def foreeachRdd = (rdd: RDD[readhdfs2es], time: Time) => {

      var timestmp = format.format(new Date(time.milliseconds))
      var month = timestmp.substring(0, 6)
      var day = timestmp.substring(6, 8)
      var hour = timestmp.substring(8, 10)
      //var minute = timestmp.substring(10, 12) + "2"

      //记录消费条数到本地
      //            val consumelog = new FileWriter(new File(s"/hadoop/cdnlog/cdnlog_parquet/c01i05/consumelog/consume-${month+day}"),true)
      //            consumelog.write("consume count"+" "+timestmp+" "+rdd.count()+"\n")
      //            consumelog.close()

      try {
        //分区追加
        rdd.toDF().write.mode("append").parquet("/user/hive/warehouse/test/month_=" + month + "/day_=" + day + "/hour_=" + hour + "/")
        //新建分区
        //  rdd.toDF().write.parquet("/user/hive/warehouse/test/month_=" + month + "/day_=" + day + "/hour_=" + hour + "/")
      } catch {
        case e: Throwable => println(s"save parquet failed:/user/hive/warehouse/test/month_=${month}/day_=${day}/hour_=${hour}")

      }
      val hiveConf = new HiveConf()
      //hiveConf.set("hive.metastore.uris", "thrift://115.238.146.2:9083")
      val hiveClient = new HiveMetaStoreClient(hiveConf)
      val partitionValues = new ArrayList[String]()
      partitionValues.add(month)
      partitionValues.add(day)
      partitionValues.add(hour)
      try {
        val newPartition = hiveClient.appendPartition("default", "test", partitionValues)

      } catch {
        case e: Throwable => e.printStackTrace()
      } finally {
        hiveClient.close()
      }
    }
    logInfos.window(Minutes(1), Minutes(1)).foreachRDD(foreeachRdd)

    ssc.start()
    ssc.awaitTermination()
  }


}
