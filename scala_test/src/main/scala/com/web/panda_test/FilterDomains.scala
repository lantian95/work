package com.web.panda_test

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.URL
import java.util.zip.GZIPInputStream



import scala.collection.JavaConversions._

object FilterDomains {

    // 时间间隔为一小时,一天更新一次直播域名
    val timeinterval = 1 * 60 * 60 * 1000L

    var lastUpdateTime = System.currentTimeMillis()
    val domains = collection.mutable.Set[String]() ++ update()

    def apply(inStream: InputStream): Set[String] = {
        val result = collection.mutable.Set[String]()
        var reader = new BufferedReader(new InputStreamReader(inStream))
        var tmp = ""
        while ( {
            tmp = reader.readLine()
            tmp
        } != null) {
            val ops = tmp.split(",")
             if (ops(10) == "2") {
              result.add(ops(0))

              }
        }
        println(result.toSet.size)
        result.toSet
    }

    def apply(): java.util.Set[String] = {
        val currentTime = System.currentTimeMillis()
        if ((currentTime - lastUpdateTime) > timeinterval) {
            lastUpdateTime = currentTime
            domains.clear()
            domains ++= update()
        }
        domains
    }

    def update(): Set[String] = {
        val result = collection.mutable.Set[String]()
        val url = new URL(Channel.CDN_CHANNEL_URL)
        val gz = new GZIPInputStream(url.openStream())
        try {
            println("update")
            result ++= this (gz)
        } finally {
            gz.close()
        }
        result.toSet
    }

    def main(args: Array[String]) {
        println(FilterDomains.apply())
        Thread.sleep(10000)
        println(FilterDomains.apply())
    }
}