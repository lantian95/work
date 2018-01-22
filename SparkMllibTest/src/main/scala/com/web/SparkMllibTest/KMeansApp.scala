package com.web.SparkMllibTest

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{ SparkConf, SparkContext }


//KMeans聚类--Demo
object KMeansApp {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("KMeansApp").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("src/main/resource/kmeans.log")
    val parsedData = data.map(s => Vectors.dense(s.split(" ").map(_.trim.toDouble))).cache()

    //设置簇的个数为3
    val numClusters = 4
    //迭代20次
    val numIterations = 20
    //运行10次,选出最优解
    val runs = 10
    val clusters = KMeans.train(parsedData, numClusters, numIterations, runs)
    // Evaluateclustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("WithinSet Sum of Squared Errors = " + WSSSE)

    val a21 = clusters.predict(Vectors.dense(57.0, 30.0))
    val a22 = clusters.predict(Vectors.dense(0.0, 0.0))

    //打印出中心点
    println("Clustercenters:");
    for (center <- clusters.clusterCenters) {
      println(" " + center)
    }

    //打印出测试数据属于哪个簇
    println(parsedData.map(v => v.toString() + " belong to cluster :" + clusters.predict(v)).collect().mkString("\n"))
    println("预测第21个用户的归类为-->" + a21)
    println("预测第22个用户的归类为-->" + a22)
  }

}