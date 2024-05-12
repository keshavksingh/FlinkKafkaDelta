package com.kafkadelta.project

import java.sql.Timestamp
import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}

class SalesOrderEvent {
  @JsonProperty("SalesId")
  var SalesId: String = _

  @JsonProperty("ProductName")
  var ProductName: String = _

  @JsonProperty("SalesDateTime")
  var SalesDateTime: Timestamp = _

  @JsonProperty("SalesAmount")
  var SalesAmount: Int = _

  @JsonProperty("EventProcessingTime")
  var EventProcessingTime: Timestamp = _

  @JsonCreator
  def this(@JsonProperty("SalesId") SalesId: String,
           @JsonProperty("ProductName") ProductName: String,
           @JsonProperty("SalesDateTime") SalesDateTime: Timestamp,
           @JsonProperty("SalesAmount") SalesAmount: Int,
           @JsonProperty("EventProcessingTime") EventProcessingTime: Timestamp) {
    this()
    this.SalesId = SalesId
    this.ProductName = ProductName
    this.SalesDateTime = SalesDateTime
    this.SalesAmount = SalesAmount
    this.EventProcessingTime = EventProcessingTime
  }
}

