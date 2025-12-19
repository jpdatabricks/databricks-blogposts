// Databricks notebook source
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType, TimestampType}

// Event Hub configuration, replace as per your setup Kafka/Eventhub
val EH_NAMESPACE = "gc-streaming" //replace with your namespace if eventhub as source
val EVENTHUB_POLICY_NAME = dbutils.secrets.get(scope = "datta_eh", key = "eventhub_policy_name") //replace with your value
val EVENTHUB_KEY = dbutils.secrets.get(scope = "datta_eh", key = "eventhub_key") //replace with your value

// Build Event Hub connection string
val EH_CONN_STR = s"Endpoint=sb://$EH_NAMESPACE.servicebus.windows.net/;" +
  s"SharedAccessKeyName=$EVENTHUB_POLICY_NAME;" +
  s"SharedAccessKey=$EVENTHUB_KEY"

// Kafka options for connecting to Event Hub
val kafka_options = Map(
  "kafka.bootstrap.servers" -> s"$EH_NAMESPACE.servicebus.windows.net:9093",
  "kafka.sasl.mechanism" -> "PLAIN",
  "kafka.security.protocol" -> "SASL_SSL",
  "kafka.sasl.jaas.config" ->
    s"""kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$$ConnectionString" password="$EH_CONN_STR";"""
)


//Below code keep same as is to test this use case.
// Sample payload data for adtech request
val adtech_request_value = Map(
  "ad_id" -> "ad123",
  "campaign_id" -> "camp456",
  "bid" -> 1.23,
  "user" -> Map(
    "user_id" -> "user789",
    "geo" -> Map("country" -> "US", "city" -> "San Francisco")
  ),
  "device" -> Map(
    "os" -> "iOS",
    "model" -> "iPhone 13"
  )
)

// Sample payload data for adtech ack
val adtech_ack_value = Map(
  "ad_id" -> "ad123",
  "status" -> "delivered",
  "delivery_time" -> "2025-10-01T12:34:56Z"
)

// Combined payload map
val payload = Map(
  "request" -> adtech_request_value,
  "ack" -> adtech_ack_value
)

// Schema for adtech ack messages
val adtech_ack_schema = StructType(Seq(
  StructField("ad_id", StringType),
  StructField("batchId", StringType),
  StructField("status", StringType),
  StructField("delivery_time", StringType)
))

// Schema for adtech request messages
val adtech_request_schema = StructType(Seq(
  StructField("ad_id", StringType),
  StructField("batchId", StringType),
  StructField("campaign_id", StringType),
  StructField("bid", DoubleType),
  StructField("user", StructType(Seq(
    StructField("user_id", StringType),
    StructField("geo", StructType(Seq(
      StructField("country", StringType),
      StructField("city", StringType)
    )))
  ))),
  StructField("device", StructType(Seq(
    StructField("os", StringType),
    StructField("model", StringType)
  )))
))

// Combined schema for the full payload
val schema = StructType(Seq(
  StructField("value_ack", adtech_ack_schema),
  StructField("value_request", adtech_request_schema),
  StructField("etl_timestamp", TimestampType),
  StructField("record_timestamp_request", TimestampType),
  StructField("record_timestamp_ack", TimestampType)
))