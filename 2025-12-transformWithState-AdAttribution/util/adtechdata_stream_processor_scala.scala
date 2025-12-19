// Databricks notebook source
// DBTITLE 1,Define Input and Output Schema
package data_object

import java.sql.Timestamp

object LogRecordObjects {

  /**
   * Represents an input row from the log stream.
   *
   * @param id Unique identifier for the log record.
   * @param type Type of the log record (e.g., 2 for request, others for ack).
   * @param transaction_id Transaction identifier associated with the log.
   * @param timestamp Event timestamp (epoch seconds).
   * @param value Serialized value as byte array.
   */
  case class InputRow(
    id: String,
    `type`: Int,
    transaction_id: String,
    timestamp: Long,
    value: Array[Byte]
  )

  /**
   * Represents a processed log record combining request and acknowledgment.
   *
   * @param transaction_id Transaction identifier.
   * @param key_request Key of the request record.
   * @param record_timestamp_request Timestamp of the request record.
   * @param value_request Value of the request record as byte array.
   * @param key_ack Key of the acknowledgment record.
   * @param record_timestamp_ack Timestamp of the acknowledgment record.
   * @param value_ack Value of the acknowledgment record as byte array.
   */
  case class LogRecord(
    transaction_id: String,
    key_request: String,
    record_timestamp_request: Timestamp,
    value_request: Array[Byte], 
    key_ack: String,
    record_timestamp_ack: Timestamp,
    value_ack: Array[Byte]
  )
}

// COMMAND ----------

// DBTITLE 1,Define stateful application logic using an object-oriented model
package stateprocess

import data_object.LogRecordObjects._
import org.apache.spark.sql.streaming._
import java.time._
import org.apache.spark.sql.Encoders

/**
 * A stateful processor for handling log records (supports both RTM and MBM).
 *
 * In real-time mode, handleInputRows is invoked per row; the iterator has exactly one element.
 * In micro-batch mode, handleInputRows is invoked once per key with all rows for the key in the batch.
 *
 * @param timeout_duration The duration (in minutes) for holding records in state before they expire.
 *                         (TTL is disabled in this implementation; argument retained for signature compatibility.)
 */
class LogRecordProcessor(timeout_duration: Long)
  extends StatefulProcessor[String, InputRow, LogRecord] {

  // State to store acknowledgment log records (pending until a request is seen)
  @transient private var _ackLogRecordListState: ListState[InputRow] = _

  // State to store the latest request log record for this transaction id (key)
  @transient private var _requestLogRecordState: ValueState[InputRow] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    // TTL disabled for both state variables
    _ackLogRecordListState = getHandle.getListState(
      "ack",
      Encoders.product[InputRow],
      TTLConfig.NONE
    )
    _requestLogRecordState = getHandle.getValueState[InputRow](
      "req",
      Encoders.product[InputRow],
      TTLConfig.NONE
    )
  }

  /**
   * Auto-detect execution mode:
   * - If the iterator has exactly one element, treat as Real-Time Mode (RTM).
   * - If the iterator has multiple elements, treat as Micro-Batch Mode (MBM).
   */
  override def handleInputRows(
      key: String,
      inputRows: Iterator[InputRow],
      timerValues: TimerValues
  ): Iterator[LogRecord] = {

    if (!inputRows.hasNext) return Iterator.empty

    // Peek the first element to decide the branch
    val first = inputRows.next()
    val hasMore = inputRows.hasNext

    val output =
      if (!hasMore) {
        // Real-Time Mode branch: single row per invocation
        processSingleRow(key, first)
      } else {
        // Micro-Batch Mode branch: reconstruct full iterator (first + rest)
        val fullIter = Iterator.single(first) ++ inputRows
        processBatchRows(key, fullIter)
      }

    // Register a processing-time timer to fire in timeout_duration minutes from the current processing time
    val nowMs = timerValues.getCurrentProcessingTimeInMs()
    getHandle.registerTimer(nowMs + Duration.ofMinutes(timeout_duration).toMillis)

    output
  }

  /**
   * Timer handler fired when a registered processing-time timer expires.
   * TTL is disabled; this handler flushes any pending acks if a request exists.
   */
  override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo
  ): Iterator[LogRecord] = {
    // Safely read the request if present
    val requestOpt = _requestLogRecordState.getOption()
    if (requestOpt.isEmpty) {
      // No request stored; nothing to emit or remove
      Iterator.empty
    } else {
      val request = requestOpt.get

      // Drain any pending acks
      val pendingAcks = _ackLogRecordListState.get()
      if (!pendingAcks.hasNext) {
        // No acks; proactively clear the request to avoid indefinite retention
        _requestLogRecordState.clear()
        Iterator.empty
      } else {
        // Emit matches and then clear state to force removals
        val out = pendingAcks.map { ack =>
          LogRecord(
            ack.transaction_id,
            request.id,
            new java.sql.Timestamp(request.timestamp),
            request.value,
            ack.id,
            new java.sql.Timestamp(ack.timestamp),
            ack.value
          )
        }.toArray.iterator

        // Explicit removals since TTL is disabled
        _ackLogRecordListState.clear()
        _requestLogRecordState.clear()

        println(s"###### TIMER: flushed pending acks and cleared state for transaction id: $key")
        out
      }
    }
  }

  /**
   * RTM: process exactly one row and correlate with existing state.
   */
  private def processSingleRow(key: String, row: InputRow): Iterator[LogRecord] = {
    if (row.`type` == 2) {
      // Request row: update request state; flush any pending acks
      _requestLogRecordState.update(row)

      val pendingAcksIter = _ackLogRecordListState.get()
      if (pendingAcksIter.hasNext) {
        val out = scala.collection.mutable.ListBuffer[LogRecord]()
        while (pendingAcksIter.hasNext) {
          val ack = pendingAcksIter.next()
          out += LogRecord(
            ack.transaction_id,
            row.id,
            new java.sql.Timestamp(row.timestamp),
            row.value,
            ack.id,
            new java.sql.Timestamp(ack.timestamp),
            ack.value
          )
        }
        _ackLogRecordListState.clear()
        println(s"###### RTM: request & pending acks matched for transaction id: $key")
        out.iterator
      } else {
        println(s"###### RTM: stored request; no acks yet for transaction id: $key")
        Iterator.empty
      }
    } else {
      // Ack row: emit immediately if request exists; otherwise store ack
      if (_requestLogRecordState.exists()) {
        val request = _requestLogRecordState.get()
        val record = LogRecord(
          row.transaction_id,
          request.id,
          new java.sql.Timestamp(request.timestamp),
          request.value,
          row.id,
          new java.sql.Timestamp(row.timestamp),
          row.value
        )
        println(s"###### RTM: ack matched with existing request for transaction id: $key")
        Iterator.single(record)
      } else {
        _ackLogRecordListState.appendValue(row)
        println(s"###### RTM: stored ack; no request yet for transaction id: $key")
        Iterator.empty
      }
    }
  }

  /**
   * MBM: process all rows for the key in this batch (preserves your original semantics).
   */
  private def processBatchRows(key: String, inputRows: Iterator[InputRow]): Iterator[LogRecord] = {
    val (request, acks) = getRequestAcksRecords(inputRows)
    val existingAcks = _ackLogRecordListState.get()

    val outputRecords =
      (request, acks.isEmpty && !existingAcks.hasNext) match {
        case (null, _) =>
          // No request in this batch: accumulate acks in state
          _ackLogRecordListState.appendList(acks)
          println(s"###### MBM: Request is empty/null for transaction id: $key")
          Iterator.empty

        case (_, true) =>
          // Request present but no acks (batch nor existing): store request only
          _requestLogRecordState.update(request)
          println(s"###### MBM: Ack is empty/null for transaction id: $key")
          Iterator.empty

        case (_, false) =>
          // Request + acks present: update request, merge with existing acks if any, then emit
          _requestLogRecordState.update(request)
          println(s"###### MBM: request & acks available for transaction id: $key")

          val allAcks =
            if (existingAcks.hasNext) {
              // Drain existing acks and clear state
              val drained = existingAcks.toArray
              _ackLogRecordListState.clear()
              acks ++ drained
            } else {
              acks
            }

          val requestAckRecords = allAcks.map { ack =>
            LogRecord(
              ack.transaction_id,
              request.id,
              new java.sql.Timestamp(request.timestamp),
              request.value,
              ack.id,
              new java.sql.Timestamp(ack.timestamp),
              ack.value
            )
          }
          requestAckRecords.iterator
      }

    outputRecords
  }

  /**
   * Extract the request and acknowledgment records from the iterator.
   * - Identifies the first request record (type == 2) in the batch (or falls back to request state).
   * - Collects all other rows as acknowledgments.
   */
  private def getRequestAcksRecords(inputRows: Iterator[InputRow]): (InputRow, Array[InputRow]) = {
    var requestOpt: Option[InputRow] = None
    val acks = scala.collection.mutable.ArrayBuffer[InputRow]()

    inputRows.foreach { row =>
      if (row.`type` == 2 && requestOpt.isEmpty) {
        requestOpt = Some(row) // First request row
      } else {
        acks += row // Other rows treated as acks
      }
    }

    val request = requestOpt.headOption.getOrElse(_requestLogRecordState.getOption().orNull)
    (request, acks.toArray)
  }
}

// COMMAND ----------

package com.demo.data

import data_object.LogRecordObjects._
import stateprocess._
import org.apache.spark.sql.streaming._
import java.time._
import org.apache.spark.sql.protobuf.functions._
import scala.collection.JavaConverters._
import org.apache.spark.sql._
import org.apache.spark.util.Utils
import org.apache.spark.sql.execution.streaming.RealTimeTrigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.Encoders

/**
 * AdTechStreamProcessor handles the end-to-end streaming ETL pipeline for ad tech log records.
 *
 * @param kafkaConfig Kafka configuration map.
 * @param schema      Input schema for the log records.
 * @param spark       Implicit SparkSession.
 */
class AdTechStreamProcessor(
  kafkaConfig: Map[String, String],
  schema: org.apache.spark.sql.types.StructType
)(implicit val spark: SparkSession) {

  import spark.implicits._

  // UDF to generate current ETL timestamp
  val etlTimestampUdf = udf(() => java.sql.Timestamp.from(java.time.Instant.now()))

  // Extract value_ack and value_request field schemas from the provided schema
  val value_ack_field: StructType = schema("value_ack").dataType.asInstanceOf[StructType]
  val value_request_field: StructType = schema("value_request").dataType.asInstanceOf[StructType]

  /**
   * Initialize Spark configuration for stateful streaming and RocksDB state store.
   */
  def initializeSparkConfig(): Unit = {
    spark.conf.set(
      "spark.sql.streaming.stateStore.providerClass",
      "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
    )
    spark.conf.set("spark.sql.streaming.stateStore.checkpointFormatVersion", 2)
    spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", 10)
    spark.conf.set("spark.databricks.streaming.statefulOperator.asyncCheckpoint.enabled", "true")
  }
  initializeSparkConfig()

  /**
   * Main method to process the input Kafka stream and write processed records to output Kafka topic.
   *
   * @param mode Streaming trigger mode ("trigger" or other).
   * @param stateTimeoutInmin State timeout in minutes for stateful processing.
   * @return StreamingQuery object.
   */
  def processStream(
    mode: String,
    stateTimeoutInmin: Int,
    inputTopicName: String,
    outputTopicName: String
  ): StreamingQuery = {

    // Checkpoint location for streaming state
    val checkpointLocation =
      s"/tmp/datta_demo/dev/data/$outputTopicName/${java.util.UUID.randomUUID.toString}/"

    // Read from Kafka and parse key and value fields
    var sourceStream: DataFrame = spark.readStream
      .format("kafka")
      .options(kafkaConfig)
      .option("subscribe", inputTopicName)
      .option("startingOffsets", "latest")
      .option("includeHeaders", "true")
      .load()
      .withColumn("key", col("key").cast("string"))
      .withColumn("id", col("key"))
      .withColumn("transaction_id", col("key"))
      .withColumn(
        "type",
        expr("CAST(filter(headers, x -> x.key = 'record_type')[0].value AS STRING)").cast("int")
      )
      .withColumn("timestamp", unix_millis(col("timestamp")))

    // Group by transaction_id and apply stateful processing using LogRecordProcessor
    val transactionDF = sourceStream
      .as[InputRow]
      .groupByKey(_.transaction_id)
      .transformWithState(
        new LogRecordProcessor(timeout_duration = stateTimeoutInmin),
        TimeMode.ProcessingTime,
        OutputMode.Append
      )
      .toDF()

    // Select and format output columns, including parsing JSON fields and adding ETL timestamp
    var outputDf = transactionDF.select(
      $"transaction_id",
      $"key_request",
      date_format($"record_timestamp_request", "yyyy-MM-dd HH:mm:ss.SSS").alias("record_timestamp_request"),
      from_json($"value_request".cast("string"), value_request_field).alias("value_request"),
      from_json($"value_ack".cast("string"), value_ack_field).alias("value_ack"),
      $"key_ack",
      date_format($"record_timestamp_ack", "yyyy-MM-dd HH:mm:ss.SSS").alias("record_timestamp_ack")
    )
    outputDf = outputDf
      .withColumn("etl_timestamp", etlTimestampUdf())
      .withColumn(
        "value",
        struct(
          col("value_request"),
          col("value_ack"),
          col("etl_timestamp"),
          col("record_timestamp_request"),
          col("record_timestamp_ack")
        )
      )

    // Write the processed records to the output Kafka topic
    val query = outputDf
      .select(
        $"transaction_id".cast("string").alias("key"),
        to_json($"value").alias("value")
      )
      .writeStream
      .format("kafka")
      .options(kafkaConfig)
      .option("topic", outputTopicName)
      .option("asyncProgressTrackingEnabled", "true")
      .option("checkpointLocation", checkpointLocation)
      .trigger(
        if (mode == "trigger") Trigger.ProcessingTime("0 minutes")
        else RealTimeTrigger.apply("5 minutes")
      )
      .outputMode("update")
      .start()

    query
  }

}