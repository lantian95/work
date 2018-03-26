package web.com

/**
  * Created by Administrator on 2017/6/23.
  */
import scala.annotation.tailrec
import java.text.SimpleDateFormat

import scala.io.Source
import java.util.Locale

case class readhdfs2es(domain: String, dir1: String, dir2: String, timestamp: Long, cs: Long){}

object readhdfs2es {

  def empty() = { null }

  def apply(line: String): readhdfs2es = {
    try {
    val index_time = line.indexOf("]")

    val index_get = line.indexOf(" ", index_time + 1)
    val index_url = line.indexOf(" ", index_get + 1)

    val index_status = line.indexOf(" ", index_url + 1)
    var url_string = line.substring(index_url + 1, index_status)
    if (!url_string.startsWith("http://") ) {
      return empty();
    }
    var url_list = url_string.substring(7, url_string.length)

    val domain = url_list.split("/")(0)
    val dir1 = url_list.split("/")(1)
    val dir2 = url_list.split("/")(2)


    val index_code = line.indexOf(" ", index_status + 1)
    val index_cs = line.indexOf(" ", index_code + 1)
    val index_refer = line.indexOf(" ", index_cs + 1)
    val cs = line.substring(index_cs + 1,index_refer).toLong

    println(cs)

    val SDF = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
    val url = line.substring(line.indexOf(" "),line.indexOf("GET ")+4)
    //val cs = line.substring(index_bytes_received + 1, line.indexOf(" ", index_bytes_received + 1)).toLong

    val timestamp = SDF.parse(line.substring(line.indexOf("[", 0) + 1, line.indexOf("]", 0))).getTime / 1000 / 60 * 60


    readhdfs2es(domain, dir1, dir2, timestamp, cs)
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

    val line1 = " "
    val a = readhdfs2es(line1)
    printLine(a)

  }

}
