package fastweb.com

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

    val line1 = "112.12.123.223 131.365 - [23/Jun/2017:00:00:08 +0800] \"GET http://kw36.videocc.net/pts/05714ecace_j48bsrqd/5/05714ecace62bd8b26be8ea95c2fdde5_3_255_312.pts?pid=1498144236668X1414972 HTTP/1.1\" 200 1512272 \"http:\n//www.iguxuan.com/index.php/tnew/spshow/id/13752.html\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.104 Safari/537.36 Core/1.53.3051.400 QQBrowser/9.6.11301.400\" \nFCACHE_HIT_DISK 1.012 0.000 - - - - 0.000 131.365 131.365 1 - - 112.12.123.223 \"DISK HIT from 117.131.204.203, Configured MISS from 117.131.204.204, DISK HIT from 223.94.95.138\" 1511792 223.94.95.138"
    val a = readhdfs2es(line1)
    printLine(a)

  }

}