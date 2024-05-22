package com.kafkadelta.project

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.table.data.{GenericRowData, RowData, StringData, TimestampData}
import org.apache.flink.util.Collector
import java.sql.Timestamp

class DeduplicateAndAggregateSales extends KeyedProcessFunction[String, SalesOrderEvent, RowData] {
  // Map to store the last processed event's EventProcessingTime for each product
  private lazy val lastProcessedTimeMap: MapState[String, Timestamp] = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, Timestamp]("lastEventProcessingTimeMap", classOf[String], classOf[Timestamp])
  )

  // Map to store the aggregated sales amount for each product
  private lazy val productSalesAmountMap: MapState[String, Int] = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, Int]("productSalesAmountMap", classOf[String], classOf[Int])
  )

  override def processElement(
                               event: SalesOrderEvent,
                               ctx: KeyedProcessFunction[String, SalesOrderEvent, RowData]#Context,
                               out: Collector[RowData]): Unit = {
    val productName = event.ProductName
    val lastProcessedTime = Option(lastProcessedTimeMap.get(productName))
    if (lastProcessedTime.isEmpty || event.EventProcessingTime.compareTo(lastProcessedTime.get) > 0) {
      // Update the last processed time for the product
      lastProcessedTimeMap.put(productName, event.EventProcessingTime)
      // Update the aggregated sales amount for the product
      val currentSalesAmount = Option(productSalesAmountMap.get(productName)).getOrElse(0)
      productSalesAmountMap.put(event.ProductName, currentSalesAmount + event.SalesAmount)

      // Emit the event with aggregated sales amount
      val rowData = new GenericRowData(5)
      rowData.setField(0, StringData.fromString(event.SalesId))
      rowData.setField(1, StringData.fromString(event.ProductName))
      rowData.setField(2, TimestampData.fromTimestamp(event.SalesDateTime))
      rowData.setField(3, currentSalesAmount + event.SalesAmount) // Aggregate sales amount
      rowData.setField(4, TimestampData.fromTimestamp(event.EventProcessingTime))
      out.collect(rowData)
    }
  }
}
