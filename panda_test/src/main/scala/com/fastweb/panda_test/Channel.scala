package com.fastweb.panda_test

import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URL
import java.util.zip.ZipInputStream
import java.util.zip.ZipEntry
import java.util.zip.GZIPInputStream

class Channel {
  val domains = new HashMap[String, String]
  val leftDomains = new HashMap[String, String]

  val EXT_DOMAIN = "ext."

  def transform(domain: String): String = {
    var i = 0
    if ({ i = domain.indexOf('.'); i } == -1) {
      return null
    }
    val sub = domain.substring(i + 1)
    if (leftDomains.contains(domain)) {
      //正常域名
      return domain
    } else if (domains.contains(sub)) {
      //泛域名 加上泛域名标识："ext"
      return EXT_DOMAIN + sub
    } else {
      transform(sub)
    }
  }

  def getUserID(domain: String): String = {
    if (leftDomains.contains(domain)) {
      //* 正常域名
      return leftDomains.get(domain).get;
    } else if (domain.startsWith(EXT_DOMAIN)) {
      //* 查找泛域名ext
      return domains.get(domain.substring(EXT_DOMAIN.length())).get;
    } else {
      "0"
    }
  }

  override def toString(): String = {
    super.toString() + ":" + (domains.size + leftDomains.size)
  }

}
object Channel {
  val CDN_CHANNEL_URL = "http://udap.fwlog.cachecn.net:8088/cdn_channel.conf.gz"
  // 时间间隔为一天,一天更新一次cdn channel 
  val timeinterval = 1 * 60 * 60 * 1000L

  var lastUpdate = System.currentTimeMillis()

  var current = apply(CDN_CHANNEL_URL)

  def apply(inStream: InputStream): Channel = {
    val channel = new Channel()
    var reader = new BufferedReader(new InputStreamReader(inStream))
    var tmp = ""
    while ({ tmp = reader.readLine(); tmp } != null) {
      val ops = tmp.split(",")
      if (tmp.startsWith("ext.") && ops(2) == "2") {
        channel.domains.put(ops(0).substring(4), ops(6))
      } else {
        channel.leftDomains.put(ops(0), ops(6))
      }
    }
    channel
  }

  def apply(path: String): Channel = {
    var url = new URL(path)
    try {
      val gz = new GZIPInputStream(url.openStream())
      try {
        current = this(gz)
        return current
      } catch {
        case e: Throwable => {
          println(e)
          return current
        }
      } finally {
        gz.close()
      }
    } catch {
      case e: Throwable => {
        println(e)
        return current
      }
    }
  }

  def apply(): Channel = {
    var inStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("cdn_channel.conf")
    try {
      this(inStream)
    } finally {
      inStream.close()
    }
  }
  def update(): Channel = {
    val currentTime = System.currentTimeMillis()
    if ((currentTime - lastUpdate) > timeinterval) {
      lastUpdate = currentTime
      apply(CDN_CHANNEL_URL)
    } else {
      current
    }
  }
  def main(args: Array[String]) {
    lastUpdate = 0
    println(Channel.current)
    println(Channel.update())
  }
}