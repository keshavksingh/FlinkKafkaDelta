package com.kafkadelta.project

import com.typesafe.config.ConfigFactory
import io.delta.flink.sink.DeltaSink
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.data.{GenericRowData, RowData, StringData, TimestampData}
import org.apache.flink.table.types.logical.{IntType, RowType, TimestampType, VarCharType}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.WindowedStream
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter
import org.apache.flink.util.Collector
import org.apache.hadoop.conf.Configuration
import org.slf4j.LoggerFactory

import java.time.{Duration, Instant}
import java.{lang, util}
import java.util.UUID

object StatefulProcessingTimeWindowingStreamProcessingKafkaSourceDeltaSinkStreamJob {
  val logger = LoggerFactory.getLogger(StatefulProcessingTimeWindowingStreamProcessingKafkaSourceDeltaSinkStreamJob.getClass)
  def main(args: Array[String]): Unit = {
    val TOPIC = "saleseventhub"
    val config = ConfigFactory.load("kafka.consumer.conf").getConfig("confighome")
    val kafkaConfig = config.getConfig("kafka-consumer")
    val deltaTablePathSink = "abfss://flink@synapseadlsdeveus01.dfs.core.windows.net/Streams/SalesOrderProcessingTimeAggregate"

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Configure checkpointing
    env.enableCheckpointing(10000) // Checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //env.getCheckpointConfig.setMinPauseBetweenCheckpoints(5000) // Minimum pause between checkpoints
    //env.getCheckpointConfig.setCheckpointTimeout(60000) // Checkpoint timeout 60 seconds
    //env.getCheckpointConfig.setMaxConcurrentCheckpoints(1) // Allow only one checkpoint at a time
    //env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3) // Tolerate up to 3 consecutive failures

    val rowType: RowType = new RowType(
      util.Arrays.asList(
        new RowType.RowField("SalesId", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("ProductName", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("WindowStartTime", new TimestampType),
        new RowType.RowField("SalesAmount", new IntType),
        new RowType.RowField("WindowEndTime", new TimestampType)
      )
    )
    val kafkaSource = KafkaSource.builder()
      .setBootstrapServers(kafkaConfig.getString("bootstrap.servers"))
      .setProperty("sasl.mechanism", kafkaConfig.getString("sasl.mechanism"))
      .setProperty("sasl.jaas.config", kafkaConfig.getString("sasl.jaas.config"))
      .setProperty("security.protocol", kafkaConfig.getString("security.protocol"))
      .setTopics(TOPIC)
      .setGroupId("$Default")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new KafkaSalesEventSchema())
      .build()

    val stream: DataStream[SalesOrderEvent] = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource")

    val processedStream: DataStream[RowData]= stream
      .keyBy((value: SalesOrderEvent) => value.ProductName)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
      //.allowedLateness(Time.seconds(20))
      .aggregate(new SalesAmountAggregator, new SalesAmountWindowFunction)
      .setParallelism(1)

    createADLSDeltaSink(processedStream, deltaTablePathSink, rowType)
    env.execute("AzureEventHubKafkaReadADLSDeltaProcessingWindowedAggWriteExampleJob")
  }

  def createADLSDeltaSink(stream: DataStream[RowData], deltaTablePath: String, rowType: RowType): DataStream[RowData] = {
    val deltaSink = DeltaSink.forRowData(new Path(deltaTablePath), new Configuration, rowType).build()
    stream.sinkTo(deltaSink)
    stream
  }


}
