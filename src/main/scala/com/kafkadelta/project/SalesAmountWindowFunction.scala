package com.kafkadelta.project
import org.apache.flink.table.data.{GenericRowData, RowData, StringData, TimestampData}
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.lang
import java.util.UUID

class SalesAmountWindowFunction extends WindowFunction[Int,RowData, String, TimeWindow] {
  override def apply(key: String, w: TimeWindow, iterable: lang.Iterable[Int], collector: Collector[RowData]): Unit = {
    val totalSales = iterable.iterator().next()
    val salesId = UUID.randomUUID().toString
    val rowData = new GenericRowData(5)
    rowData.setField(0, StringData.fromString(salesId))
    rowData.setField(1, StringData.fromString(key))
    rowData.setField(2, TimestampData.fromEpochMillis(w.getStart))
    rowData.setField(3, totalSales)
    rowData.setField(4, TimestampData.fromEpochMillis(w.getEnd))
    collector.collect(rowData)
  }
}

/*
class SalesAmountWindowFunction extends ProcessWindowFunction[SalesOrderEvent, RowData, String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[SalesOrderEvent], out: Collector[RowData]): Unit = {
    val totalSales = elements.map(_.SalesAmount).sum
    val salesId = UUID.randomUUID().toString
    val rowData = new GenericRowData(5)
    rowData.setField(0, StringData.fromString(salesId))
    rowData.setField(1, StringData.fromString(key))
    rowData.setField(2, TimestampData.fromEpochMillis(context.window.getEnd))
    rowData.setField(3, totalSales.asInstanceOf[AnyRef]) // Cast to AnyRef if needed
    rowData.setField(4, TimestampData.fromEpochMillis(System.currentTimeMillis()))
    out.collect(rowData)
  }
}
*/
