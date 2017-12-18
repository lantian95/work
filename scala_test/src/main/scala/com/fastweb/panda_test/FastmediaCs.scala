package com.fastweb.panda_test

import scala.annotation.tailrec
import scala.io.Source
import org.apache.commons.lang3.StringUtils
import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSON
import java.util.Locale
import java.text.SimpleDateFormat

case class FastmediaCs(domain: String, userid: String, cs: Long, timestamp: String, flag: String) {}

object FastmediaCs {

  val empty = null

  def parseLine(line: String, channel: Channel): FastmediaCs = {

    //CDNLOG_MEDIA_cache ctl-gd-121-010-121-077 fastmedia rtmp [22/Aug/2016:11:35:24 +0800] 23294_79666_1471836919915 fastweb.upstream.yy.com live 70384468_70384468 115.238.138.48 121.10.121.77 push

    val index_time = line.indexOf("]")

    val index_id = line.indexOf(" ", index_time + 2)

    val index_domain = line.indexOf(" ", index_id + 1)

    val domain = line.substring(index_id + 1, index_domain)

    val userid = channel.getUserID(domain);

    val index_app = line.indexOf(" ", line.indexOf(" ", index_domain + 1))

    val index_stream = line.indexOf(" ", line.indexOf(" ", index_app + 1))

    val index_ip_f = line.indexOf(" ", line.indexOf(" ", index_stream + 1))

    val index_ip_s = line.indexOf(" ", line.indexOf(" ", index_ip_f + 1))

    val index_command = line.indexOf(" ", line.indexOf(" ", index_ip_s + 1))

    val flag = line.substring(index_ip_s + 1, index_command).toLowerCase()
    //    var flag = line.substring(line.indexOf("] \"", 0) + 3, line.indexOf(" ", line.indexOf("] \"", 0) + 3)).toLowerCase()
    /*   if (flag.endsWith("ing")) {
         flag = flag.replace("ing", "")
       }*/

    /*
    if (!"publish".equals(flag)) {
      return empty;
    }
    */

    val index_tcUrl = line.indexOf(" ", line.indexOf(" ", index_command + 1))

    val index_args = line.indexOf(" ", line.indexOf(" ", index_tcUrl + 1))

    val index_bytes_sent = line.indexOf(" ", line.indexOf(" ", index_args + 1))

    val index_bytes_received = line.indexOf(" ", line.indexOf(" ", index_bytes_sent + 1))
    val cs = line.substring(line.lastIndexOf("\" ") + 2, line.indexOf(" -", 0)).toLong
    println(cs)
    val SDF = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
    //val cs = line.substring(index_bytes_received + 1, line.indexOf(" ", index_bytes_received + 1)).toLong

    val timestamp = SDF.parse(line.substring(line.indexOf("[", 0) + 1, line.indexOf("]", 0))).getTime / 1000 / 60 * 60

    //    val re_time = line.substring(line.lastIndexOf("(") + 1, line.lastIndexOf(")")).replaceAll(" ", "").toLowerCase()
    /*
    var dd = 6L

    var index_s = 0

    if (re_time.contains("s")) {
      if (re_time.length() < 4) {
        dd = if (re_time.substring(0, re_time.length() - 1).toLong % 6 == 0) 6 else (re_time.substring(0, re_time.length() - 1).toLong % 6)
      } else {
        val o = re_time.substring(re_time.length() - 2, re_time.length() - 1).toLong
        val t = if (StringUtils.isNumeric(re_time.substring(re_time.length() - 3, re_time.length() - 2))) re_time.substring(re_time.length() - 3, re_time.length() - 2).toLong else 0
        dd = if ((t * 10 + o) % 6 == 0) 6 else (t * 10 + o) % 6
      }
    }
    */
    /*
    val userip = line.substring(index_stream + 1, index_ip_f)

    val line_rtmp = line.substring(0, line.indexOf(domain)) + "rtmp://" + domain  + "/" + line.substring(line.indexOf(domain, 0) + domain.length(), line.length())

    val line_tmp = line_rtmp.substring(0, line_rtmp.indexOf(" ", line_rtmp.indexOf(" ", 0) + 1))

    val line_fcd = line_tmp + " " + line_rtmp.substring(line_tmp.length() + 1, line_rtmp.length())
    */
    // Time granularity is 6s
    FastmediaCs(domain, userid, cs, String.valueOf(timestamp), flag)
  }

  def apply(line: String, channel: Channel): FastmediaCs = {
    try {
      parseLine(line, channel)
    } catch {
      case _: Throwable => empty
    }
  }

  def main(args: Array[String]) {

    val arr = Source.fromInputStream(Thread.currentThread().getContextClassLoader.getResourceAsStream("fastmediacs")).getLines().toArray
    for (log <- arr) {
      val fc = FastmediaCs(log, Channel.update())
      println(fc.flag + "\t" + fc.domain + "\t" + fc.userid + "\t" + fc.timestamp + "\t" + fc.cs)

    }
  }
}