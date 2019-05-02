package com.despegar.www.haystack.jaeger_collector

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.expedia.open.tracing.span.Span
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

    val datePattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
    val durationPattern = "s.SSS's'"

    object StringToLong extends CustomSerializer[Long](format => (
      { case JString(x) =>  try {
        DateTime.parse(x, DateTimeFormat.forPattern(datePattern)).getMillis
      } catch {
        case _: Exception => DateTime.parse(x, DateTimeFormat.forPattern(durationPattern)).getMillis * 1000
      }},
      { case x: Long => JInt(x) }))


    implicit val formats: Formats = DefaultFormats + StringToLong

    val builder = new StreamsBuilder
    val jaegerTraces: KStream[String,String] = builder.stream[String, String]("jaeger-spans")
    val haystackTraces: KStream[String, Span] = jaegerTraces
      .flatMapValues( (_, b) => List(parse(b).extract[Span]) )
    haystackTraces.to("test-topic")

    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.start()

    sys.ShutdownHookThread {
      streams.close(10, TimeUnit.SECONDS)
    }
  }
}
