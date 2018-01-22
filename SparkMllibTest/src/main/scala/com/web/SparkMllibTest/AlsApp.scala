package com.web.SparkMllibTest

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import org.jblas.DoubleMatrix

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
object AlsApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AlsApp").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val data = sc.textFile("src/main/resource/als.log")
    val ratings = data.map(_.split("::") match {
      case Array(user, item, rate, name) => Rating(user.toInt, item.toInt, rate.toDouble)
    })

    val users = ratings.map(_.user).distinct()
    val products = ratings.map(_.product).distinct()

    /*
    println("-----------------user" + ratings.map { _.user }.distinct().count())
    println("-----------------product" + ratings.map(_.product).distinct().count())
*/
    val rank = 1
    val lambda = 0.01
    val numIterations = 20
    //使用ALS训练数据建立推荐模型
    val model = ALS.train(ratings, rank, numIterations, lambda)
    /*
    println("======================================" + model.userFeatures.count())
    println("======================================" + model.productFeatures.count())
*/
    val usersProducts = ratings.map { case Rating(user, product, rate) => (user, product) }
    //使用推荐模型对用户商品进行预测评分，得到预测评分的数据集
    val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) => ((user, product), rate) }
    //将真实评分数据集与预测评分数据集进行合并。注--join() : (RDD[(K, V)]; RDD[(K, W)]) => RDD[(K, (V, W))]
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) => ((user, product), rate) }.join(predictions)
    //然后计算均方差
    val MSE = ratesAndPreds.map {
      case ((user, product), (r1, r2)) => math.pow((r1 - r2), 2)
    }.reduce(_ + _) / ratesAndPreds.count

    ratesAndPreds.sortByKey().repartition(1).sortBy(_._1).map({ case ((user, product), (rate, pred)) => (user + "," + product + "," + rate + "," + pred) }).saveAsTextFile("user/hive/warehouse/" + Math.random() + "/")

    users.take(5).foreach { x =>
      val pro = model.recommendProducts(x, 3)
      println(pro.mkString("........"))
    }

    def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
      vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
    }
    /*
    val itemId = 2012
    val itemFactor = model.productFeatures.lookup(itemId).head
    val itemVector = new DoubleMatrix(itemFactor)

    val sims = model.productFeatures.map {
      case (id, factor) =>
        val factorVector = new DoubleMatrix(factor)
        val sim = cosineSimilarity(factorVector, itemVector)
        (id, sim)
    }
    val sortedSims = sims.top(10)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
    println(sortedSims.mkString("--"))
*/

    products.take(10).foreach(x => {
      val itemFactor = model.productFeatures.lookup(x).head
      val itemVector = new DoubleMatrix(itemFactor)

      val sims = model.productFeatures.map {
        case (id, factor) =>
          val factorVector = new DoubleMatrix(factor)
          val sim = cosineSimilarity(factorVector, itemVector)
          (id, sim)
      }
      val sortedSims = sims.top(10)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
      println(sortedSims.mkString("--"))
    })

    val itemFactors = model.productFeatures.map { case (id, factor) => factor }.collect()
    val itemMatrix = new DoubleMatrix(itemFactors)
    //println(itemMatrix)
    println(itemMatrix.rows, itemMatrix.columns)

    val imBroadcast = sc.broadcast(itemMatrix)
    var idxProducts=model.productFeatures.map { case (prodcut, factor) => prodcut }.zipWithIndex().map{case (prodcut, idx) => (idx,prodcut)}.collectAsMap()
    val idxProductsBroadcast = sc.broadcast(idxProducts)
    val allRecs = model.userFeatures.map {
      case (user, array) =>
        val userVector = new DoubleMatrix(array)
        val scores = imBroadcast.value.mmul(userVector)
        val sortedWithId = scores.data.zipWithIndex.sortBy(-_._1)
        //根据索引取对应的商品id
        val recommendedProducts = sortedWithId.map(_._2).map { idx => idxProductsBroadcast.value.get(idx).get }
        (user, recommendedProducts)
    }

    println("Mean Squared Error = " + MSE)
  }
}