package com.despegar.www.haystack.jaeger_collector

import org.json4s.JsonDSL._
import com.expedia.open.tracing.span.{Log, Span, Tag}
import org.json4s.{CustomSerializer, DefaultFormats}
import org.json4s.JsonAST.{JInt, JString}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s._
import java.text.SimpleDateFormat
import java.util.TimeZone

import com.expedia.open.tracing.span.Tag.Myvalue
import com.expedia.open.tracing.span.Tag.Myvalue.VStr
import com.google.protobuf.ByteString

object TagCustomDeserializer extends CustomSerializer[Tag](_ => ( {
  case jsonObj: JObject =>
    import Tag.TagType._
    import Tag.Myvalue._

    implicit val formats: Formats = DefaultFormats + StringToLong
    val objectList: List[(String, JValue)] = jsonObj.obj

    def tagTypeFromString(s: String): Tag.TagType = {
      s match {
        case "BOOL" => BOOL
        case "STRING" => STRING
        case "DOUBLE" => DOUBLE
        case "LONG" => LONG
        case "BINARY" => BINARY
        case _ => Unrecognized(-1)
      }
    }

    objectList.length match {
      case 1 =>
        val key = (jsonObj \ "key").extract[String]
        Tag(key)
      case 2 =>
        val key = (jsonObj \ "key").extract[String]
        val tagTypeOp = (jsonObj \ "vType").toOption.map(value => tagTypeFromString(value.extract[String]))
        val value = tagTypeOp.map{
          case BOOL => VBool(false)
          case STRING => VStr("")
          case BINARY => VBytes(ByteString.EMPTY)
          case DOUBLE => VDouble(0)
          case LONG => VLong(0L)
          case Unrecognized(_) => Myvalue.Empty
        }.getOrElse(VStr((jsonObj \ "vStr").extract[String]))
        Tag(key, myvalue = value)
      case 3 =>
        val key = (jsonObj \ "key").extract[String]
        val `type` = tagTypeFromString(( jsonObj \ "vType").extract[String])
        val value = `type` match {
          case STRING => VStr((jsonObj \ "vStr").extract[String])
          case BOOL => VBool((jsonObj \ "vBool").extract[Boolean])
          case DOUBLE => VDouble((jsonObj \ "vDouble").extract[Double])
          case LONG => VLong((jsonObj \ "vLong").extract[Long])
          case BINARY => VBytes((jsonObj \ "vBytes").extract[ByteString])
          case Unrecognized(_) => Myvalue.Empty
        }
        Tag(key, `type`, value)
    }
}, {
  case tag: Tag =>
    JObject()
}))

object SpanCustomDeserializer extends CustomSerializer[Span](_ => ( {

  case jsonObj: JObject =>
    implicit val formats: Formats = DefaultFormats + StringToLong + TagCustomDeserializer

    val traceId = (jsonObj \ "traceId").extract[String]
    val spanId = (jsonObj \ "spanId").extract[String]
    val parentId = (jsonObj \ "parentSpanId").extract[Option[String]]
    val serviceName = (jsonObj \ "process" \ "serviceName").extract[String]
    val operationName = (jsonObj \ "operationName").extract[Option[String]]
    val startTime = (jsonObj \ "startTime").extract[Long]
    val duration = (jsonObj \ "duration").extract[Option[Long]]
    val logs = (jsonObj \ "logs").extract[Seq[Log]]
    val tags = (jsonObj \ "tags").extract[Seq[Tag]]

    val parentSpanId = parentId.getOrElse(
      tags.find(_.key == "parentID").map(t =>
        t.myvalue match {
          case VStr(psid) => psid
          case _ => ""
        }
      ).getOrElse(""))

    Span(traceId, spanId, parentSpanId, serviceName, operationName.getOrElse("operation-not-found"),
        startTime, duration.getOrElse(1000L), logs, tags.filterNot(_.key == "parentID"))

}, {
  case span: Span =>
    ("traceId" -> span.traceId) ~
      ("spanId" -> span.spanId) ~
      ("parentSpanId" -> span.parentSpanId)

}))

object StringToLong extends CustomSerializer[Long](_ => (
  { case JString(x) =>  try {
    val datePattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
    DateTime.parse(x, DateTimeFormat.forPattern(datePattern)).getMillis * 1000
  } catch {
    case _: Exception =>
      val durationPattern = "s.SSS's'"
      val dateFormat = new SimpleDateFormat(durationPattern)
      dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
      dateFormat.parse(x).toInstant.toEpochMilli * 1000
  }},
  { case x: Long => JInt(x) }))