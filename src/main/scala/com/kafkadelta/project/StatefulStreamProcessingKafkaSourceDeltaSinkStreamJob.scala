package com.kafkadelta.project
import com.typesafe.config.ConfigFactory
import io.delta.flink.sink.DeltaSink
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.{DataStream,KeyedStream}
import org.apache.flink.table.data.{GenericRowData, RowData, StringData, TimestampData}
import org.apache.flink.table.types.logical.{IntType, RowType, TimestampType, VarCharType}
import org.apache.hadoop.conf.Configuration

import java.util
import org.apache.flink.streaming.api.CheckpointingMode

object StatefulStreamProcessingKafkaSourceDeltaSinkStreamJob {
  def main(args: Array[String]):Unit= {
    val TOPIC = "saleseventhub"
    val config = ConfigFactory.load("kafka.consumer.conf").getConfig("confighome")
    val kafkaconfig = config.getConfig("kafka-consumer")
    val deltaTablePath_sink = "abfss://flink@<storage>.dfs.core.windows.net/Streams/SalesOrderAggregatesByProduct"

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(10000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    val ROW_TYPE: RowType = new RowType(util.Arrays.asList(new RowType.RowField("SalesId", new VarCharType(VarCharType.MAX_LENGTH))
      , new RowType.RowField("ProductName", new VarCharType(VarCharType.MAX_LENGTH))
      , new RowType.RowField("SalesDateTime", new TimestampType)
      , new RowType.RowField("TotalSalesAmount", new IntType)
      , new RowType.RowField("EventProcessingTime", new TimestampType)))

    val kafkaSource = KafkaSource.builder()
      .setBootstrapServers(kafkaconfig.getString("bootstrap.servers"))
      .setProperty("sasl.mechanism", kafkaconfig.getString("sasl.mechanism"))
      .setProperty("sasl.jaas.config", kafkaconfig.getString("sasl.jaas.config"))
      .setProperty("security.protocol", kafkaconfig.getString("security.protocol"))
      .setTopics(TOPIC)
      .setGroupId("$Default")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new KafkaSalesEventSchema())
      .build()
    val stream:DataStream[SalesOrderEvent] = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource")
    val newStream: DataStream[RowData] = stream
      .keyBy((value: SalesOrderEvent) => value.ProductName)
      .process(new DeduplicateAndAggregateSales())

    /*
    val newStream: DataStream[RowData] = stream
    .keyBy(new KeySelector[SalesOrderEvent, String]() {
    @throws[Exception]
    override def getKey(value: SalesOrderEvent): String = value.ProductName
     })
     .process(new DeduplicateAndAggregateSales())

     */
    //Print Message Is needed!
    //stream.map(event => s"SalesId: ${event.SalesId}, ProductName: ${event.ProductName}, SalesDateTime: ${event.SalesDateTime}, SalesAmount: ${event.SalesAmount}, EventProcessingTime: ${event.EventProcessingTime}")
    //  .print()

    createADLSDeltaSink(newStream, deltaTablePath_sink, ROW_TYPE)
    env.execute("AzureEventHubKafkaReadADLSDeltaStatefulWriteExampleJob")
  }
  def createADLSDeltaSink(stream: DataStream[RowData], deltaTablePath: String, rowType: RowType): DataStream[RowData] = {
    val deltaSink = DeltaSink.forRowData(new Path(deltaTablePath), new Configuration, rowType).build()
    stream.sinkTo(deltaSink)
    stream
  }
}
