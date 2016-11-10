package com.toptrade.kafka.serializer

import java.util
import com.alibaba.fastjson.JSON

import org.apache.kafka.common.serialization.Serializer

class PojoSerializer extends Serializer[Object] {
  override def configure(map: util.Map[String, _], b: Boolean): Unit = {}

  override def serialize(s: String, t: Object): Array[Byte] = {
    JSON.toJSONBytes(t)
  }

  override def close(): Unit = {}
}
