package fastweb.com

/**
  * Created by Administrator on 2017/6/26.
  */
import java.text.SimpleDateFormat

import scala.io.Source
import java.util.Locale

case class xns2es(host: String, isp: Long, region: Long, timestamp: Long, count: Long, userid: Long, zone: Long, viewid: Long){}

object xns2es {

  def empty() = { null }

  def apply(line: String): xns2es = {
    try {
      val lines = line.split(",")
      val host = lines(1)
      val timestamp = lines(2).toLong
      val userid = lines(3).toLong
      val zone = lines(4).toLong
      val viewid = lines(7).toLong
      val count = lines(8).toLong
      val isp = lines(10).toLong
      val region = lines(11).toLong

      println(host, isp, region, timestamp, count, userid, zone, viewid)
      xns2es(host, isp, region, timestamp, count, userid, zone, viewid)
    }
    catch {
      case e: Throwable => {
        null
      }
    }
  }


  def main(args: Array[String]) {

    def printLine(line: Any){
      val clazz = line.getClass
      val arr = clazz.getDeclaredFields()
      for(i <- 0 to arr.length-1){
        val f = arr(i)
        f.setAccessible(true)
        print(f.getName + "：" + f.get(line).toString() + "\t")
      }
      println()
    }

    val line1 = "mongdb,#other#,1494342000,30125,111365,0,0,54,10,mongdb,2,3"
    val a = xns2es(line1)
    printLine(a)

  }

}
