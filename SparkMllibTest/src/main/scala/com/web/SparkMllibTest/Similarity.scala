package com.web.SparkMllibTest

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.collection.Map
import org.apache.spark.mllib.linalg.{Vector, Vectors}


/**
  * Created by Administrator on 2017/9/4.
  */
class Similarity extends Serializable {
  /**
    * The TfIdf should just return term-id, doc-id, termId-DocId vectors.
    * Here exist A simple approach to tf-idf, that is, using history data
    * to calculation idf, the tf is builded on the fly.
    */
  val defaultPartitions = 128
  def TfIdf(docs: RDD[(String, Seq[String])]):
  (Map[String, Long], Map[Long, String], Map[Long, Double], RDD[Vector]) = {
    val docTermFreqs= docs.mapValues(terms => {
      val termFreqInDoc = terms.foldLeft(new mutable.HashMap[String, Int]()) {
        (mp, term) => mp += term -> (mp.getOrElse(term, 0) + 1)
      }
      termFreqInDoc
    })

    docTermFreqs.cache()

    val doc2docId = docTermFreqs.map(_._1).zipWithUniqueId().collectAsMap()
    val docId2doc = doc2docId.map(_.swap)
    val docFreq = docTermFreqs.map(_._2)
      .flatMap(_.keySet)
      .map((_, 1))
      .reduceByKey(_ + _, numPartitions=defaultPartitions)
      .cache()

    val term2termId = docFreq.map(_._1).zipWithIndex().collectAsMap()
    // val termId2term = term2termId.map(_.swap)

    val docSize = doc2docId.size
    val termSize = term2termId.size
    val idf = docFreq.map {
      case (term, cnt) => (term2termId(term), 1 + math.log(docSize/cnt))
    }.collectAsMap()

    val tf = docTermFreqs.map(_._2).map(termFreq => {
      val totalTermsInDoc = termFreq.values.sum
      val termFreqScore = termFreq.map {
        case (term, cnt) => (term2termId(term).toInt, math.sqrt(cnt/totalTermsInDoc))
      }.toSeq
      Vectors.sparse(termSize.toInt, termFreqScore)
    })
    println(term2termId, docId2doc, idf, tf.collect)
    // should return term2termId map, docId2doc map, idf map, tf vector
    (term2termId, docId2doc, idf, tf)
  }

  /***
    * The default similarity calaculation function is just using tf-idf vector
    * and the lucene practice score function. But We need to make this function
    * work with both global tf-idf score and the local tf-idf score(filtered doc)
    * So the here we just implement the score function.
    *
    *   score(query, doc) = coord-factor(query, doc) *
    *                 queryNorm(query) *
    *                 Sum[term in query](tf(term in doc) * idf(term) ** 2 * norm(term, doc))
    *   tf(term in doc) = sqrt(freq)
    *   idf(term) = 1 + log(numDocs/(docFreq+1))
    *   queryNorm(query) = 1/sqrt(Sum[term in query](idf(term) ** 2))
    *   norm(term, doc) = lengthNorm
    *
    */
  def defaultScoreFunction(termData: Seq[(Double, Double)], totalTermSize: Int) = {
    val queryNorm = 1.0 / math.sqrt(termData.map {
      case (tf, idf) =>
        idf * idf
    }.sum)
    val coordFactor = termData.count(_._1 > 0) / totalTermSize.toDouble
    termData.map {
      case (tf, idf) =>
        tf * idf * idf
    }.sum * queryNorm * coordFactor
  }
}

object Similarity extends Similarity {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SimilarityApp").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val data = sc.textFile("src/main/resource/simi.log")
    val docs = data.map(line =>(line.split(",")(0), line.split(",")(1).split(" ").toSeq) )
    val tf_idf = TfIdf(docs)
  }
}
