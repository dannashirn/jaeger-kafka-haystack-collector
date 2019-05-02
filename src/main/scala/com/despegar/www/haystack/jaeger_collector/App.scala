package com.despegar.www.haystack.jaeger_collector

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.json4s.CustomSerializer
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import com.expedia.open.tracing.span.{Log, Span, Tag}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.json4s.{CustomSerializer, DefaultFormats}
import org.json4s.JsonAST.{JInt, JString}
import scalapb.json4s.JsonFormat
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s._
import org.json4s.native.JsonMethods._


object App {
  def main(args: Array[String]): Unit = {
    import org.apache.kafka.streams.scala.Serdes._

    implicit val serde: SpanSerde = new SpanSerde

    val props: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "collector-test")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "")
      p
    }

    implicit val formats: Formats = DefaultFormats + SpanCustomDeserializer

    val builder = new StreamsBuilder
    val jaegerTraces: KStream[String,String] = builder.stream[String, String]("jaeger-spans")
    val haystackTraces: KStream[String, Span] = jaegerTraces
      .flatMapValues( (_, b) => List(parse(b).extract[Span]) )
    haystackTraces.to("proto-spans")

    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.start()

    sys.ShutdownHookThread {
      streams.close(10, TimeUnit.SECONDS)
    }
  }
}

object SpanCustomDeserializer extends CustomSerializer[Span](format => ( {

  case jsonObj: JObject =>
    implicit val formats: Formats = DefaultFormats + StringToLong

    val traceId = (jsonObj \ "traceId").extract[String]
    val spanId = (jsonObj \ "spanId").extract[String]
    val parentSpanId = ""
    println(s"JObject: $jsonObj")
    val serviceName = (jsonObj \ "process" \ "serviceName").extract[String]
    val operationName = (jsonObj \ "operationName").extract[Option[String]]
    val startTime = (jsonObj \ "startTime").extract[Long]
    val duration = (jsonObj \ "duration").extract[Option[Long]]
    val logs = (jsonObj \ "logs").extract[Seq[Log]]
    val tags = (jsonObj \ "tags").extract[Seq[Tag]]

    val span = Span(traceId, spanId, parentSpanId, serviceName, operationName.getOrElse("operation-not-found"), startTime, duration.getOrElse(0L), logs, tags)
    println(span)
    span
}, {
  case span: Span =>
      ("traceId" -> span.traceId) ~
      ("spanId" -> span.spanId) ~
      ("parentSpanId" -> span.parentSpanId)

}))

object StringToLong extends CustomSerializer[Long](format => (
  { case JString(x) =>  try {
    val datePattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
    DateTime.parse(x, DateTimeFormat.forPattern(datePattern)).getMillis * 1000
  } catch {
    case _: Exception =>
      val durationPattern = "s.SSS's'"
      DateTime.parse(x, DateTimeFormat.forPattern(durationPattern)).getMillis * 1000
  }},
  { case x: Long => JInt(x) }))