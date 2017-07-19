

package org.apache.spark.streaming.kafka

import java.io.OutputStream
import java.lang.{Integer => JInt, Long => JLong, Number => JNumber}
import java.nio.charset.StandardCharsets
import java.util.{List => JList, Map => JMap, Set => JSet}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{Decoder, DefaultDecoder, StringDecoder}
import net.razorvine.pickle.{IObjectPickler, Opcodes, Pickler}

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}


object DirectKafkaUtils {

  /** get leaders for the given offset ranges, or throw an exception */
  private def leadersForRanges(
      kc: KafkaCluster,
      offsetRanges: Array[OffsetRange]): Map[TopicAndPartition, (String, Int)] = {
    val topics = offsetRanges.map(o => TopicAndPartition(o.topic, o.partition)).toSet
    val leaders = kc.findLeaders(topics)
    KafkaCluster.checkErrors(leaders)
  }

  /** Make sure offsets are available in kafka, or throw an exception */
  private def checkOffsets(
      kc: KafkaCluster,
      offsetRanges: Array[OffsetRange]): Unit = {
    val topics = offsetRanges.map(_.topicAndPartition).toSet
    val result = for {
      low <- kc.getEarliestLeaderOffsets(topics).right
      high <- kc.getLatestLeaderOffsets(topics).right
    } yield {
      offsetRanges.filterNot { o =>
        low(o.topicAndPartition).offset <= o.fromOffset &&
        o.untilOffset <= high(o.topicAndPartition).offset
      }
    }
    val badRanges = KafkaCluster.checkErrors(result)
    if (!badRanges.isEmpty) {
      throw new SparkException("Offsets not available on leader: " + badRanges.mkString(","))
    }
  }

  private[kafka] def getFromOffsets(
      kc: KafkaCluster,
      kafkaParams: Map[String, String],
      topics: Set[String]
    ): Map[TopicAndPartition, Long] = {
    val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
    val result = for {
      topicPartitions <- kc.getPartitions(topics).right
      leaderOffsets <- (if (reset == Some("smallest")) {
        kc.getEarliestLeaderOffsets(topicPartitions)
      } else {
        kc.getLatestLeaderOffsets(topicPartitions)
      }).right
    } yield {
      leaderOffsets.map { case (tp, lo) =>
          (tp, lo.offset)
      }
    }
    KafkaCluster.checkErrors(result)
  }


  /**
   * @param ssc StreamingContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *    configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *    to be set with Kafka broker(s) (NOT zookeeper servers) specified in
   *    host1:port1,host2:port2 form.
   * @param fromOffsets Per-topic/partition Kafka offsets defining the (inclusive)
   *    starting point of the stream
   * @param messageHandler Function for translating each message and metadata into the desired type
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @tparam KD type of Kafka message key decoder
   * @tparam VD type of Kafka message value decoder
   * @tparam R type returned by messageHandler
   * @return DStream of R
   */
  def createDirectStream[
    K: ClassTag,
    V: ClassTag,
    KD <: Decoder[K]: ClassTag,
    VD <: Decoder[V]: ClassTag,
    R: ClassTag] (
      ssc: StreamingContext,
      kafkaParams: Map[String, String],
      fromOffsets: Map[TopicAndPartition, Long],
      messageHandler: MessageAndMetadata[K, V] => R
  ): InputDStream[R] = {
    val cleanedHandler = ssc.sc.clean(messageHandler)
    new DirectKafkaInputDStream[K, V, KD, VD, R](
      ssc, kafkaParams, fromOffsets, cleanedHandler)
  }

  /**
   * @param ssc StreamingContext object
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *   configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *   to be set with Kafka broker(s) (NOT zookeeper servers), specified in
   *   host1:port1,host2:port2 form.
   *   If not starting from a checkpoint, "auto.offset.reset" may be set to "largest" or "smallest"
   *   to determine where the stream starts (defaults to "largest")
   * @param topics Names of the topics to consume
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @tparam KD type of Kafka message key decoder
   * @tparam VD type of Kafka message value decoder
   * @return DStream of (Kafka message key, Kafka message value)
   */
  def createDirectStream[
    K: ClassTag,
    V: ClassTag,
    KD <: Decoder[K]: ClassTag,
    VD <: Decoder[V]: ClassTag] (
      ssc: StreamingContext,
      kafkaParams: Map[String, String],
      topics: Set[String]
  ): InputDStream[(K, V)] = {
    val messageHandler = (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)
    val kc = new KafkaCluster(kafkaParams)
    val fromOffsets = getFromOffsets(kc, kafkaParams, topics)
    new DirectKafkaInputDStream[K, V, KD, VD, (K, V)](
      ssc, kafkaParams, fromOffsets, messageHandler)
  }

  /**
   * @param jssc JavaStreamingContext object
   * @param keyClass Class of the keys in the Kafka records
   * @param valueClass Class of the values in the Kafka records
   * @param keyDecoderClass Class of the key decoder
   * @param valueDecoderClass Class of the value decoder
   * @param recordClass Class of the records in DStream
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *   configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *   to be set with Kafka broker(s) (NOT zookeeper servers), specified in
   *   host1:port1,host2:port2 form.
   * @param fromOffsets Per-topic/partition Kafka offsets defining the (inclusive)
   *    starting point of the stream
   * @param messageHandler Function for translating each message and metadata into the desired type
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @tparam KD type of Kafka message key decoder
   * @tparam VD type of Kafka message value decoder
   * @tparam R type returned by messageHandler
   * @return DStream of R
   */
  def createDirectStream[K, V, KD <: Decoder[K], VD <: Decoder[V], R](
      jssc: JavaStreamingContext,
      keyClass: Class[K],
      valueClass: Class[V],
      keyDecoderClass: Class[KD],
      valueDecoderClass: Class[VD],
      recordClass: Class[R],
      kafkaParams: JMap[String, String],
      fromOffsets: JMap[TopicAndPartition, JLong],
      messageHandler: JFunction[MessageAndMetadata[K, V], R]
    ): JavaInputDStream[R] = {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
    implicit val keyDecoderCmt: ClassTag[KD] = ClassTag(keyDecoderClass)
    implicit val valueDecoderCmt: ClassTag[VD] = ClassTag(valueDecoderClass)
    implicit val recordCmt: ClassTag[R] = ClassTag(recordClass)
    val cleanedHandler = jssc.sparkContext.clean(messageHandler.call _)
    createDirectStream[K, V, KD, VD, R](
      jssc.ssc,
      Map(kafkaParams.asScala.toSeq: _*),
      Map(fromOffsets.asScala.mapValues(_.longValue()).toSeq: _*),
      cleanedHandler
    )
  }

  /**
   * @param jssc JavaStreamingContext object
   * @param keyClass Class of the keys in the Kafka records
   * @param valueClass Class of the values in the Kafka records
   * @param keyDecoderClass Class of the key decoder
   * @param valueDecoderClass Class type of the value decoder
   * @param kafkaParams Kafka <a href="http://kafka.apache.org/documentation.html#configuration">
   *   configuration parameters</a>. Requires "metadata.broker.list" or "bootstrap.servers"
   *   to be set with Kafka broker(s) (NOT zookeeper servers), specified in
   *   host1:port1,host2:port2 form.
   *   If not starting from a checkpoint, "auto.offset.reset" may be set to "largest" or "smallest"
   *   to determine where the stream starts (defaults to "largest")
   * @param topics Names of the topics to consume
   * @tparam K type of Kafka message key
   * @tparam V type of Kafka message value
   * @tparam KD type of Kafka message key decoder
   * @tparam VD type of Kafka message value decoder
   * @return DStream of (Kafka message key, Kafka message value)
   */
  def createDirectStream[K, V, KD <: Decoder[K], VD <: Decoder[V]](
      jssc: JavaStreamingContext,
      keyClass: Class[K],
      valueClass: Class[V],
      keyDecoderClass: Class[KD],
      valueDecoderClass: Class[VD],
      kafkaParams: JMap[String, String],
      topics: JSet[String]
    ): JavaPairInputDStream[K, V] = {
    implicit val keyCmt: ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt: ClassTag[V] = ClassTag(valueClass)
    implicit val keyDecoderCmt: ClassTag[KD] = ClassTag(keyDecoderClass)
    implicit val valueDecoderCmt: ClassTag[VD] = ClassTag(valueDecoderClass)
    createDirectStream[K, V, KD, VD](
      jssc.ssc,
      Map(kafkaParams.asScala.toSeq: _*),
      Set(topics.asScala.toSeq: _*)
    )
  }
}

