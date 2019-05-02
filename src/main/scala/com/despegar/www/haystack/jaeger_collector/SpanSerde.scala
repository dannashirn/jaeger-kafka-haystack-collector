package com.despegar.www.haystack.jaeger_collector


import java.util

import com.expedia.open.tracing.span.Span
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

class SpanSerde extends Serde[Span] {

  override def configure(configs: util.Map[String, _], b: Boolean): Unit = ()

  override def close(): Unit = ()

  def serializer: Serializer[Span] = {
    new SpanSerializer
  }

  def deserializer: Deserializer[Span] = {
    new SpanDeserializer
  }
}

class SpanSerializer extends Serializer[Span] {
  override def configure(configs: util.Map[String, _], b: Boolean): Unit = ()

  override def close(): Unit = ()

  override def serialize(topic: String, obj: Span): Array[Byte] = if (obj != null) obj.toByteArray else null
}

class SpanDeserializer extends Deserializer[Span]  {

  override def configure(configs: util.Map[String, _], b: Boolean): Unit = ()

  override def close(): Unit = ()

  override def deserialize(topic: String, data: Array[Byte]): Span = performDeserialize(data)

  /**
    * converts the binary protobuf bytes into Span object
    *
    * @param data serialized bytes of Span
    * @return
    */
  private def performDeserialize(data: Array[Byte]): Span = {
    try {
      if (data == null || data.length == 0) null else Span.parseFrom(data)
    } catch {
      case _: Exception =>
        /* may be log and add metric */
        null
    }
  }
}
