package com.kafkadelta.project

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import com.fasterxml.jackson.databind.ObjectMapper
import java.io.IOException

class KafkaSalesEventSchema extends AbstractDeserializationSchema[SalesOrderEvent] {
  private val objectMapper: ObjectMapper = new ObjectMapper()

  @throws[IOException]
  override def deserialize(message: Array[Byte]): SalesOrderEvent = {
    objectMapper.readValue(message, classOf[SalesOrderEvent])
  }
}
