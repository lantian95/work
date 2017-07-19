package org.apache.spark.streaming.kafka

/**
  * Created by hry on 2017/7/7.
  */
import kafka.common.TopicAndPartition
import kafka.util.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.HasOffsetRanges

class ZooKeeperOffsetsStore(zkHosts: String, zkPath: String) {

   val zkClient = new ZkClient(zkHosts, 30000, 30000, ZKStringSerializer)

  // Read the previously saved offsets from Zookeeper
   def readOffsets(topic: String): Option[Map[TopicAndPartition, Long]] = {

    println("Reading offsets from ZooKeeper")
    val (offsetsRangesStrOpt, _) = ZkUtils.readDataMaybeNull(zkClient, zkPath)

    offsetsRangesStrOpt match {
      case Some(offsetsRangesStr) =>
        println(s"Read offset ranges: ${offsetsRangesStr}")

        val offsets = offsetsRangesStr.split(",")
          .map(s => s.split(":"))
          .map { case Array(partitionStr, offsetStr) => (TopicAndPartition(topic, partitionStr.toInt) -> offsetStr.toLong) }
          .toMap

        println("Done reading offsets from ZooKeeper. Took " )

        Some(offsets)
      case None =>
        println("No offsets found in ZooKeeper. Took ")
        None
    }

  }

  //将该 partition 的 offset 保存到 zookeeper
   def saveOffsets(topic: String, rdd: RDD[_]): Unit = {

    println("Saving offsets to ZooKeeper")

    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    offsetsRanges.foreach(offsetRange => println(s"Using ${offsetRange}"))

    val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}")
      .mkString(",")
    println(s"Writing offsets to ZooKeeper: ${offsetsRangesStr}")
    ZkUtils.updatePersistentPath(zkClient, zkPath, offsetsRangesStr)

    println("Done updating offsets in ZooKeeper. Took ")
  }

}


