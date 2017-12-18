package common

import org.joda.time.{DateTimeZone, DateTime}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

/**
  * Created by hry on 2017/10/19.
  */
class DateUtil extends Serializable {
  def datetimeRange(start: DateTime, end: DateTime, step: Int = 1): Iterator[DateTime] = {
    Iterator.iterate(start)(_.plusDays(step)).takeWhile(!_.isAfter(end))
  }

  def strToDateTime(dayStr: String, dayFormatter: DateTimeFormatter): DateTime = {
    dayFormatter.parseDateTime(dayStr)
  }

  def getAllDays(startDay: String, endDay: String, dayFormat: String): Array[String] = {
    val dayFormatter = DateTimeFormat.forPattern(dayFormat)
    datetimeRange(strToDateTime(startDay, dayFormatter), strToDateTime(endDay, dayFormatter))
      .toArray
      .map(day => dayFormatter.print(day))
  }

  def timestampToDateStr(timestamp: Long, dayFormat: String): String = {
    val dayFormatter = DateTimeFormat.forPattern(dayFormat)
    dayFormatter.print(
      new DateTime(timestamp * 1000, DateTimeZone.forID("UTC"))
    )
  }

  def dayStrPlusDays(dayStr: String, dayFormat: String, step: Int = 1): String = {
    val dayFormatter = DateTimeFormat.forPattern(dayFormat)
    dayFormatter.print(strToDateTime(dayStr, dayFormatter).plusDays(step))
  }

  def getHourTimestampBySecondTimestamp(secondTimestamp: String): String = {
    try {
      (secondTimestamp.toLong / 3600 * 3600).toString()
    } catch {
      case _: Throwable => secondTimestamp
    }
  }

  def getFiveTimestampBySecondTimestamp(secondTimestamp: Long): Long = {
    try {
      (secondTimestamp / 300 * 300 + 300)
    } catch {
      case _: Throwable => secondTimestamp
    }
  }


  /** one minute timestamp TO five minute timestamp **/
  def getTimestampByMinutesTimestamp(secondTimestamp: String): String = {
    try {
      (secondTimestamp.toLong / 60 * 60).toString()
    } catch {
      case _: Throwable => secondTimestamp
    }
  }
}

object DateUtil extends DateUtil{
  def main(args: Array[String]) {
    val a = DateUtil.dayStrPlusDays("20171212", "yyyyMMdd",10)
    println(DateUtil.strToDateTime("20171212-02", DateTimeFormat.forPattern("yyyyMMdd-HH")))
  }
}
