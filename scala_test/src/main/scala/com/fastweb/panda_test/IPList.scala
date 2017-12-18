package com.fastweb.panda_test

import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import java.net.URL
import java.util.zip.GZIPInputStream

import scala.collection.mutable.HashMap

class IPList {
  private val areas = new HashMap[String, (String, String, String)]

  def getIspAndPrvAndMachine(ip: String): (String, String, String) = {
    var area = areas.get(ip)
    if (area.isEmpty) {
      ("0", "0", "0")
    } else {
      area.get
    }
  }
  def hasCityCode(city: String): Boolean = {
    areas.contains(city)
  }
  override def toString(): String = {
    super.toString() + ":" + (areas.size)
  }
}

object IPList {
  val CDN_IP_LIST_URL = "http://udap.fwlog.cachecn.net:8088/ip_list.gz"
  // 时间间隔为一天,一天更新一次cdn channel
  val timeinterval = 1 * 60 * 60 * 1000L
  var lastUpdate = System.currentTimeMillis()
  /**
   * 第一次开启app读取路径下数据
   */
  var current = apply(CDN_IP_LIST_URL)

  def apply(inStream: InputStream): IPList = {
    val ipArea = new IPList()
    var reader = new BufferedReader(new InputStreamReader(inStream))
    var tmp = ""
    while ({ tmp = reader.readLine(); tmp } != null) {
      val ops = tmp.split(",")
      if (ops.length == 9) {
        ipArea.areas.put(ops(6), (ops(2), ops(4), ops(7)))
      }
    }
    ipArea
  }

  def apply(path: String): IPList = {
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

  def apply(): IPList = {
    var inStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("ip_list")
    try {
      this(inStream)
    } finally {
      inStream.close()
    }
  }
  def update(): IPList = {
    val currentTime = System.currentTimeMillis()
    if ((currentTime - lastUpdate) > timeinterval) {
      lastUpdate = currentTime
      apply(CDN_IP_LIST_URL)
    } else {
      current
    }
  }
  def main(args: Array[String]) {
    lastUpdate = 0
    println(IPList.update().getIspAndPrvAndMachine("210.22.63.120"))
  }
}