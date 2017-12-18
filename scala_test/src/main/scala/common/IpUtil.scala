package common

/**
  * Created by Administrator on 2017/10/19.
  */
class IpUtil extends Serializable {
  case class IpData(startIp: Long, endIp: Long, company: String)

  def ipStr2Long(ipStr: String): Long = {
    try {
      ipStr.split("\\.").reverse.zipWithIndex.map {
        case (field, index) =>
          field.toInt * math.pow(256, index).toLong
      }.sum
    } catch {
      case e: Exception =>
        -1
    }
  }

  def binarySearch(ipData: Array[IpData], ipToSearch: String): Option[String] = {
    val ipVal = ipStr2Long(ipToSearch)
    var low = 0
    var high = ipData.length - 1
    var mid = 0
    while (low <= high) {
      mid = (low + high) / 2
      val data = ipData(mid)
      if (data.startIp <= ipVal && ipVal <= data.endIp) {
        return Some(data.company)
      } else {
        if (data.startIp > ipVal) {
          high = mid - 1
        } else {
          low = mid + 1
        }
      }
    }
    None
  }

  def recursiveBinarySearch(ipData: Array[IpData], ipToSearch: String): Option[String] = {
    val ipVal = ipStr2Long(ipToSearch)
    @annotation.tailrec
    def recursiveSearch(low: Int, high: Int): Option[String] = {
      low > high match {
        case true =>
          None
        case false =>
          val mid = low + (high - low) / 2
          ipData(mid) match {
            case IpData(startIp, endIp, company)  if startIp <= ipVal && ipVal <= endIp =>
              Some(company)
            case IpData(startIp, endIp, company)  if startIp > ipVal =>
              recursiveSearch(low, mid - 1)
            case IpData(startIp, endIp, company) =>
              recursiveSearch(mid + 1, high)
          }
      }
    }
    recursiveSearch(0, ipData.length - 1)
  }
}

object IpUtil extends IpUtil
