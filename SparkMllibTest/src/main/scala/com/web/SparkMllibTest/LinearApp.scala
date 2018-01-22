package com.web.SparkMllibTest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

//Linear线性回归--Demo
object LinearApp {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LinearApp").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("src/main/resource/linear.log")
    val parsedData = data.map { line =>
      val parts = line.split(" ")
      //new  and old different
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(x => x.toDouble)))
    }


    // Building the model
    val numIterations = 20
    val model = LinearRegressionWithSGD.train(parsedData, numIterations)

    // Evaluate model on training examples and compute training error
    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val MSE = valuesAndPreds.map { case (v, p) => math.pow((v - p), 2) }.reduce(_ + _) / valuesAndPreds.count
    println("training Mean Squared Error = " + MSE)
  }

}