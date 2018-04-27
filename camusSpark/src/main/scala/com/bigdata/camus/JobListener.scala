package com.bigdata.camus

import com.bigdata.util.SendMailUtil

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, JobSucceeded, SparkListenerJobEnd, SparkListener}


class JobListener(sc:SparkContext) extends SparkListener{
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val appName = sc.getConf.get("spark.app.name")
    jobEnd.jobResult match {
      case JobSucceeded =>  {
        SendMailUtil.sendMail("Spark ", appName+" Job succeed")
      }
      case _ => {
        SendMailUtil.sendMail("Spark ", appName+" Job failed")
      }
    }
    println("******************job finished, delete offset dir************************")
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {

  }
}
