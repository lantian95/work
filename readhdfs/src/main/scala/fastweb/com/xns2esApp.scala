package fastweb.com

/**
  * Created by Administrator on 2017/6/26.
  */
import java.io.{FileWriter, File, FileInputStream}
import java.text.SimpleDateFormat
import java.util.{ArrayList, Collections, Properties}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.spark.SparkContext._
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.kafka.KafkaUtils
import org.elasticsearch.spark.sparkRDDFunctions



object xns2esApp {
  def main(args: Array[String]) {

    if (args.length != 7) {
      System.err.println("Usage: PandatvApp <param1:topics> <param2:elasticsearch_nodes> <param3:zookeeper_nodes> <param4:consumer_group> <param5:streaming_window> <param6:spark_streaming_receiver_maxRate>")
      System.exit(1)
    }

    val Array(path, esNode, index, month, window, maxrate, port) = args

    val win = Integer.valueOf(window)

    val sparkConf = new SparkConf().setAppName("xns2esApp")
    sparkConf.set("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
      .set("spark.streaming.receiver.maxRate", maxrate)
      .set("spark.streaming.blockInterval", "30000")
      .set("spark.kryoserializer.buffer.mb", "512")
      .set("spark.storage.memoryFraction", "0.4")
      .set("spark.streaming.unpersist", "true")
      .set("spark.default.parallelism", "30")
      .set("spark.shuffle.compress", "true")
      .set("spark.shuffle.manager", "SORT")
      .set("spark.scheduler.mode", "FAIR")
      .set("spark.akka.frameSize", "512")
      .set("es.index.auto.create", "true")
      .set("spark.rdd.compress", "true")
      .set("spark.cleaner.ttl", String.valueOf(win * 5 * 60))
      .set("spark.ui.port", port)
      .set("es.nodes", esNode)


    val sc = new SparkContext(sparkConf)
    val last_path = "hdfs://192.168.100.2:8020"+path

    /*def foreachFunc = (rdd: RDD[((String, String, String, String), Long)], time: Time) => {
      val formate = new SimpleDateFormat("yyyyMMddHH")
      var indexPart = formate.format(new Date(time.milliseconds))
      rdd.map(x => Map("domain" -> x._1._1, "dir1" -> x._1._2, "dir2" -> x._1._3, "timestamp" -> x._1._4, "cs" -> x._2)).saveToEs("dir_cs_06/" + indexPart)
    }*/
    val xns = sc.textFile(last_path).map(x => xns2es(x)).filter(x => x != null)
      .map(x => (x.host, x.isp, x.region, x.timestamp, x.count, x.userid, x.zone, x.viewid))
      .map(x => Map("host" -> x._1, "isp" -> x._2, "region" -> x._3, "timestamp" -> x._4, "count" -> x._5, "userid" -> x._6, "zone" -> x._7, "viewid" -> x._8))
      .saveToEs(index + "/" + month)


    sc.stop()
  }

}
