package common

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
/**
  * Created by Administrator on 2017/10/19.
  */

case class KeywordsSearchRequest(domain: String, stream_name: String,
                                 node: String)
class ParseJson[A] extends Serializable {
  // here is the scala reflection trick, currently confusing...
  def parseJson(req: String)(implicit m: scala.reflect.Manifest[A]): Option[A] = {
    implicit val formats = DefaultFormats
    util.Try {
      JsonMethods.parse(req).extract[A]
    }.toOption
  }
}

/*
 * The ParseRequest will ensure that all needed fields exist in the request
 * Or the return value will be empty, so we don't need to check field exist before using.
 */

object ParseJson extends ParseJson[KeywordsSearchRequest] {
  def apply(req: String): Option[KeywordsSearchRequest] = parseJson(req)

  def main(args: Array[String]) {
    println(parseJson("{\"domain\":\"ps15.live.panda.tv\",\"stream_name\":\"google\",\"node\":\"124.232.128.15\",\"type\":\"start\",\"sign\":\"65f90235a0f79bb04073fa653c680c97\",\"app_name\":\"live\",\"op_time\":1450923700}"))
  }
}
