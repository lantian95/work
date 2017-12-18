package common

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FSDataInputStream, FileSystem}

import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import java.io._

/**
  * Created by hry on 2017/10/19.
  */
class FileUtil extends Serializable{
  def getDataFromFile(path: String): Array[Array[Long]] = {
    Source.fromFile(path).getLines()
      .map(_.split(" ").map(x => x.toLong)).toArray
  }

  def getDataFromUrl(path:String): Array[Array[Long]] = {
    Source.fromURL(path).getLines()
      .map(_.split(" ").map(x => x.toLong)).toArray
  }

  def readHdfsFile(path:String): ArrayBuffer[String] = {
    val list = new ArrayBuffer[String]()

    val conf: Configuration = new Configuration
    val fs: FileSystem = FileSystem.get(URI.create(path), conf)
    var in: FSDataInputStream = null
    var reader: BufferedReader = null
    try {
      in = fs.open(new Path(path))
      reader = new BufferedReader(new InputStreamReader(in))
      var line: String = null
      while ({line = reader.readLine; line} != null) {
        list.append(line)
      }
    }
    catch {
      case e: IOException => {
        e.printStackTrace
      }
    } finally {
      if (reader != null) {
        reader.close
      }
    }
    return list
  }

  def writeToHdfs(path:String, contents:Array[String]) = {
    val pt: Path = new Path(path)
    val conf: Configuration = new Configuration
    val fs: FileSystem = FileSystem.get(URI.create(path), conf)
    val bw: BufferedWriter = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)))

    for (line <- contents) {
      bw.write(line)
      bw.newLine
    }
    bw.close
  }

  def appendToHdfs(path:String, contents:Array[String]): Unit = {
    val pt: Path = new Path(path)
    val conf: Configuration = new Configuration
    val fs: FileSystem = FileSystem.get(URI.create(path), conf)
    if (!fs.exists(pt)) {
      writeToHdfs(path, contents)
      return
    }
    val bw: BufferedWriter = new BufferedWriter(new OutputStreamWriter(fs.append(pt)))
    for (line <- contents) {
      bw.write(line)
      bw.newLine
    }
    bw.close
  }

  def writeToExcel(path: String, sheetName: String,data: Array[String]) = {
    val wb = new XSSFWorkbook() // 创建一个excel
    // 声明一个单子并命名
    val sheet = wb.createSheet(sheetName) // excel的一个sheet
    // 给单子名称一个长度
    sheet.setDefaultColumnWidth(15.toShort)
    // 生成一个样式
    val style = wb.createCellStyle() // excel 的 style

    // 创建第一行（也可以称为表头）
    for (i <- 0 until data.size) {
      val temp = data(i).split(",", -1)
      val row = sheet.createRow(i)
      for (j <- 0 until temp.size) {
        val cell = row.createCell(j)
        cell.setCellStyle(style)
        cell.setCellValue(temp(j))
      }
    }
    val out = new FileOutputStream(path);
    wb.write(out);
    out.close()
  }

}

object FileUtil extends FileUtil{
  def main(args: Array[String]) {
      val perhdfs = "hdfs://192.168.100.14:8020/"
      val path = perhdfs + "/cdnlog/cdn_ip_area.txt"
      val contents = FileUtil.readHdfsFile(path)
      contents.foreach(x=>println(x))
  }
}