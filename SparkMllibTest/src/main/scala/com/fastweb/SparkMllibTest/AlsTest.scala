package com.fastweb.SparkMllibTest

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import org.jblas.DoubleMatrix
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.Map
import scala.collection.mutable.Set

/**
 * rank 是模型中隐语义因子的个数。
 * numBlocks 是用于并行化计算的分块个数 (设置为-1，为自动配置)。
 * iterations 是迭代的次数。
 * lambda 是ALS的正则化参数。
 * implicitPrefs 决定了是用显性反馈ALS的版本还是用适用隐性反馈数据集的版本。
 * alpha 是一个针对于隐性反馈 ALS 版本的参数，这个参数决定了偏好行为强度的基准
 * 可以调整这些参数，不断优化结果，使均方差变小。比如：iterations越多，lambda较小，均方差会较小，推荐结果较优
 */
//ALS协同过滤--商品推荐--Demo
object AlsTest {

  
  def aaa() = {
    
    val map = Map[Int,Int]()
    map += (3 -> 3)
    
    
    val set = Set[Int]()
    set += 3
    
  }
  
  
  def main(args: Array[String]): Unit = {
    
    val time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
    
    val conf = new SparkConf().setAppName("AlsTest").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val data = sc.textFile("src/main/resource/als.log")
    val ratings = data.map(_.split("::") match {
      case Array(user, item, rate, name) => Rating(user.toInt, item.toInt, rate.toDouble)
    })

    val user = sc.parallelize(List("1,105", "1,106", "2,105", "2,107", "3,102","4,2012")).map(
      _.split(",") match {
        case (Array(user, product)) => (user.toInt, product.toInt)
      })

    val rank = 10
    val lambda = 0.01
    val numIterations = 20
    //使用ALS训练数据建立推荐模型
    val model = ALS.train(ratings, rank, numIterations, lambda)
    
    val predictions = model.predict(user).map{case Rating(user, product, rate) => ((user,product),rate)}
    predictions.saveAsTextFile("user/hive/warehouse/" + time + "/")
  }
}