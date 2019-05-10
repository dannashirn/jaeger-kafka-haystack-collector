package com.despegar.www.haystack.jaeger_collector

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.expedia.open.tracing.span.Span
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.json4s.DefaultFormats
import org.json4s._
import org.json4s.native.JsonMethods._

object Configuration {
  val bootstrapServers: String = sys.env("KAFKA_BOOTSTRAP_SERVER")
  val kafkaConsumeTopic: String = sys.env("KAFKA_CONSUME_TOPIC")
  val kafkaProduceTopic: String = sys.env("KAFKA_PRODUCE_TOPIC")
}

object App {
  def main(args: Array[String]): Unit = {
    import org.apache.kafka.streams.scala.Serdes._

    implicit val serde: SpanSerde = new SpanSerde

    val props: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "collector-test")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Configuration.bootstrapServers)
      p
    }

    implicit val formats: Formats = DefaultFormats + SpanCustomDeserializer

    val builder = new StreamsBuilder
    val jaegerTraces: KStream[String,String] = builder.stream[String, String](Configuration.kafkaConsumeTopic)
    val haystackTraces: KStream[String, Span] = jaegerTraces.flatMap((key, b) => {
        val span = parse(b).extract[Span]
        List((span.traceId, span))
      })
    haystackTraces.to(Configuration.kafkaProduceTopic)
    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.start()

    sys.ShutdownHookThread {
      streams.close(10, TimeUnit.SECONDS)
    }
  }
}
