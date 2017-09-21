package fastweb.com

/**
  * Created by Administrator on 2017/6/23.
  */
import java.text.SimpleDateFormat
import java.util.Date
import java.io.{FileWriter, File, FileInputStream}
import java.text.SimpleDateFormat
import java.util.{ArrayList, Collections, Properties}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{SparkConf, SparkContext}

import org.elasticsearch.spark.sparkRDDFunctions



object readhdfs2esApp {
  def main(args: Array[String]) {

    if (args.length != 3) {
      System.err.println("Usage: PandatvApp <param1:topics> <param2:elasticsearch_nodes> <param3:zookeeper_nodes> <param4:consumer_group> <param5:streaming_window> <param6:spark_streaming_receiver_maxRate>")
      System.exit(1)
    }

    val Array(esNode, time, port) = args


    val sparkConf = new SparkConf().setAppName("readhdfsApp")
    sparkConf.set("spark.serialize", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
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
      .set("spark.ui.port", port)
      .set("es.nodes", esNode)


    val sc = new SparkContext(sparkConf)
    val path1 = "hdfs://192.168.100.2:8020/*"


    /*val getEsType = () => {
      val date_now = new Date(time.toLong * 1000)
      val df = new SimpleDateFormat("yyyyMMdd")
      val esType = df.format(date_now)
      esType
    }*/

    val domain = sc.textFile(path1).map(x => readhdfs2es(x)).filter(x => x != null)
                    .map(x => ((x.domain, x.dir1, x.dir2, x.timestamp), x.cs))
                    .reduceByKey((x1: Long, x2: Long) => x1 + x2)
                    .map(x => Map("domain" -> x._1._1, "dir1" -> x._1._2, "dir2" -> x._1._3, "timestamp" -> x._1._4, "cs" -> x._2))
                    .saveToEs("dir_cs/" + time)



    sc.stop()
  }

}
