# 🚀 Real-Time Mode vs Trigger Mode: Ad Attribution Demo

A hands-on Databricks demo comparing **Real-Time Mode (RTM)** latency against traditional **Trigger Mode** in Apache Spark Structured Streaming for AdTech workloads.

---

## 📖 What You'll Learn

In this demo, you'll:

1. Set up a stateful streaming pipeline using `transformWithState`
2. Process ad attribution data matching "request" and "acknowledgment" messages
3. Compare latency metrics between RTM and Trigger Mode
4. Visualize the performance difference with charts

---

## 🎯 The Use Case: Ad Attribution Pipeline

### The Business Problem

In digital advertising, measuring campaign effectiveness requires **matching two separate events**:

| Event Type | Description | Example |
|------------|-------------|---------|
| **Request (type=2)** | Ad exposure/impression | User sees an ad for running shoes |
| **Acknowledgment (type=1)** | User action/conversion | User clicks the ad or makes a purchase |

These events arrive **independently** from different systems, often **out of order**, and must be correlated by a shared `transaction_id` (in this case, `ad_id`) to attribute conversions to the correct ad impressions.

### Why This Is Challenging

```
Timeline Example:
─────────────────────────────────────────────────────────▶ time
     │                    │                │
     ▼                    ▼                ▼
  Request A            Ack A           Request B
  (ad shown)         (ad clicked)      (ad shown)
     │                    │                │
     └────── MATCH ───────┘                └── waiting for Ack B...
```

**Challenges:**
- Events arrive **out of order** (ack before request, or vice versa)
- Need to **hold state** until both events arrive
- Must handle **late arrivals** without memory leaks
- Require **low latency** for real-time bidding and reporting

### How This Demo Solves It

The stream processor uses **stateful processing** with `transformWithState` to:

1. **Store unmatched records** in RocksDB state store
2. **Match pairs instantly** when both request and ack arrive
3. **Expire orphan records** using processing-time timers
4. **Output matched pairs** with full attribution data

---

## 📁 Project Structure

```
├── run_adtech_rtm_demo.scala          # Main notebook - run this!
└── util/
    ├── common.scala                   # Configuration (Kafka/Event Hub credentials)
    ├── adtechdata_producer_scala.py   # Data producer (generates test data)
    └── adtechdata_stream_processor_scala.scala  # Stateful stream processor
```

---

## ⚙️ Prerequisites

### 1. Databricks Workspace
- Access to a Databricks workspace
- Ability to create/configure clusters

### 2. Kafka or Azure Event Hub
- A Kafka cluster **OR** Azure Event Hub namespace
- **3 topics** required:
  - 1 input topic (for source data)
  - 2 output topics (one for RTM, one for Trigger Mode)

### 3. Databricks Secrets (for credentials)
Store your Event Hub/Kafka credentials securely:

```bash
# Create a secret scope (if not exists)
databricks secrets create-scope --scope your_scope_name

# Store your credentials
databricks secrets put --scope your_scope_name --key eventhub_policy_name
databricks secrets put --scope your_scope_name --key eventhub_key
```

---

## 🔧 Setup Instructions

### Step 1: Configure Your Cluster

Create a Databricks cluster with these settings:

**Required Library:**
```
Maven: org.apache.kafka:kafka-clients:3.7.0
```

**Spark Configurations (add to cluster config):**
```
spark.databricks.streaming.realTimeMode.enabled true
spark.shuffle.manager org.apache.spark.shuffle.streaming.MultiShuffleManager
spark.databricks.dagScheduler.type ConcurrentStageDAGScheduler
```

### Step 2: Import Notebooks

1. Download all files from this repository
2. Upload to your Databricks workspace
3. Maintain the folder structure (keep `util/` folder intact)

### Step 3: Update Configuration

Open `util/common.scala` and update these values:

```scala
// Replace with your Event Hub namespace (or Kafka bootstrap servers)
val EH_NAMESPACE = "your-eventhub-namespace"

// Replace with your secret scope name
val EVENTHUB_POLICY_NAME = dbutils.secrets.get(scope = "your_scope_name", key = "eventhub_policy_name")
val EVENTHUB_KEY = dbutils.secrets.get(scope = "your_scope_name", key = "eventhub_key")
```

### Step 4: Update Topic Names

Open `run_adtech_rtm_demo.scala` and update these values:

```scala
val input_topic_name = "your-input-topic"      // e.g., "eh4"
val rtm_output_topic = "your-rtm-output"       // e.g., "eh8"
val trigger_output_topic = "your-trigger-output" // e.g., "eh9"
```

---

## ▶️ Running the Demo

Open `run_adtech_rtm_demo.scala` and run cells in order:

### Step 1: Initialize Components
Run the first few cells to:
- Load configuration (`%run ./util/common`)
- Load stream processor (`%run ./util/adtechdata_stream_processor_scala`)
- Load data producer (`%run ./util/adtechdata_producer_scala`)
- Create helper objects

### Step 2: Run Real-Time Mode Test

```scala
// Start RTM stream processing
val query = streamProcessor.processStream(
  mode = "rtm", 
  stateTimeoutInmin = 2, 
  inputTopicName = input_topic_name, 
  outputTopicName = rtm_output_topic
)

// Wait for stream to initialize
Thread.sleep(20000)

// Produce 5 batches of test data (10-second intervals)
dataHelper.startProducer(
  inputTopicName = input_topic_name, 
  numberOfbacthes = 5, 
  delayInSec = 10
)

// Wait and stop
query.awaitTermination(10L * 60 * 1000)
query.stop()
```

### Step 3: Run Trigger Mode Test

```scala
// Start Trigger Mode stream processing  
val query = streamProcessor.processStream(
  mode = "trigger", 
  stateTimeoutInmin = 2, 
  inputTopicName = input_topic_name, 
  outputTopicName = trigger_output_topic
)

// Same producer workflow...
Thread.sleep(20000)
dataHelper.startProducer(
  inputTopicName = input_topic_name, 
  numberOfbacthes = 5, 
  delayInSec = 10
)

query.awaitTermination(2 * 60 * 1000)
query.stop()
```

### Step 4: Compare Results

Run the latency comparison cell to generate a chart showing:
- **AVG** - Average latency
- **P50** - Median latency  
- **P90** - 90th percentile
- **P99** - 99th percentile

**Expected Result:** RTM shows significantly lower latency across all metrics!

---

## 📊 Understanding the Results

The demo generates synthetic data with:
- **100 records per second** continuously for 5 minutes per batch
- **~70% matching pairs** (request + acknowledgment with same transaction ID)
- **~30% unmatched records** (to test state handling and timer expiration)

Latency is measured as:
```
Latency = sink_timestamp - max(request_timestamp, ack_timestamp)
```

This measures the **end-to-end time** from when the last piece of information arrived (either request or ack) to when the matched record was written to the output topic.

---

## 📋 Data Model

### Input Records (from Kafka)

Each record contains:

| Field | Type | Description |
|-------|------|-------------|
| `key` | String | Transaction ID (used for matching) |
| `type` | Int | `1` = Acknowledgment, `2` = Request |
| `value` | JSON | Payload data |
| `timestamp` | Long | Kafka record timestamp (epoch ms) |
| `headers` | Array | Contains `record_type` header |

**Sample Request Payload (type=2):**
```json
{
  "ad_id": "uuid-123",
  "batchId": "batch-001",
  "campaign_id": "camp456",
  "bid": 1.23,
  "user": {
    "user_id": "user789",
    "geo": {"country": "US", "city": "San Francisco"}
  },
  "device": {"os": "iOS", "model": "iPhone 13"}
}
```

**Sample Ack Payload (type=1):**
```json
{
  "ad_id": "uuid-123",
  "batchId": "batch-001",
  "status": "delivered",
  "delivery_time": "2025-10-01T12:34:56Z"
}
```

### Output Records (Matched Pairs)

| Field | Type | Description |
|-------|------|-------------|
| `transaction_id` | String | The matched transaction ID |
| `key_request` | String | Key of the request record |
| `record_timestamp_request` | Timestamp | When request was received |
| `value_request` | Struct | Parsed request payload |
| `key_ack` | String | Key of the ack record |
| `record_timestamp_ack` | Timestamp | When ack was received |
| `value_ack` | Struct | Parsed ack payload |
| `etl_timestamp` | Timestamp | When the match was processed |

---

## 🏗️ Architecture

```
┌─────────────────┐     ┌───────────────┐     ┌──────────────────┐
│  Data Producer  │────▶│  Input Topic  │────▶│ Stream Processor │
│  (100 rec/sec)  │     │  (Kafka/EH)   │     │ (transformWithState)
└─────────────────┘     └───────────────┘     └────────┬─────────┘
                                                       │
                                                       ▼
┌─────────────────┐     ┌───────────────┐     ┌──────────────────┐
│ Latency Metrics │◀────│ Output Topic  │◀────│ Matched Records  │
│   (Avg, P50...) │     │  (Kafka/EH)   │     │ (req + ack pairs)│
└─────────────────┘     └───────────────┘     └──────────────────┘
```

---

## 🔑 Key Concepts

### Real-Time Mode (RTM) - Deep Dive

RTM is a Databricks optimization that fundamentally changes how Spark Structured Streaming processes data:

| Aspect | Trigger Mode | Real-Time Mode (RTM) |
|--------|--------------|----------------------|
| **Processing Model** | Micro-batch (waits to collect records) | Per-record (processes immediately) |
| **Scheduling** | Full DAG scheduling per batch | Continuous execution pipeline |
| **Shuffle Manager** | Standard shuffle | `MultiShuffleManager` (optimized) |
| **DAG Scheduler** | Default | `ConcurrentStageDAGScheduler` |
| **Latency** | Seconds to minutes | Milliseconds |

**RTM Cluster Configuration Explained:**

```
spark.databricks.streaming.realTimeMode.enabled true
  → Enables per-record processing instead of micro-batch

spark.shuffle.manager org.apache.spark.shuffle.streaming.MultiShuffleManager  
  → Optimized shuffle for streaming with reduced overhead

spark.databricks.dagScheduler.type ConcurrentStageDAGScheduler
  → Allows concurrent stage execution for pipeline parallelism
```

**How RTM Works in This Demo:**

```
Traditional Trigger Mode:
┌─────────────────────────────────────────────────────────┐
│  Collect batch → Schedule DAG → Execute → Commit state  │
│      ~100ms+         ~50ms+       ~Xms        ~50ms+    │
└─────────────────────────────────────────────────────────┘
                    Total: 200ms+ per batch

Real-Time Mode (RTM):
┌──────────────────────────────────────┐
│  Record arrives → Process → Emit     │
│                    ~10-150ms         │
└──────────────────────────────────────┘
                 Continuous pipeline
```

### Trigger Mode (Traditional)
- Collects records into micro-batches before processing
- Full DAG scheduling overhead for each batch
- Standard shuffle operations add latency
- Better for high-throughput, latency-tolerant workloads

---

## ⏱️ Timer & State Expiration (Processing Time)

### The Problem: Orphan Records

Not all records will find their match. Consider:
- A request arrives but the user never clicks → **orphan request**
- An ack arrives but the request was lost → **orphan ack**

Without cleanup, these orphan records would **accumulate forever** in state, causing:
- Memory pressure
- Increased checkpoint sizes
- Slower state lookups

### The Solution: Processing-Time Timers

This demo uses **processing-time timers** to expire orphan records:

```scala
// When a record arrives, register a timer to fire in `timeout_duration` minutes
val nowMs = timerValues.getCurrentProcessingTimeInMs()
getHandle.registerTimer(nowMs + Duration.ofMinutes(timeout_duration).toMillis)
```

**Timer Flow:**

```
Record Arrives (t=0)
       │
       ▼
┌──────────────────────┐
│ Process & Store in   │
│ State (if unmatched) │
│ Register timer: t+2m │
└──────────────────────┘
       │
       ▼ (2 minutes later)
┌──────────────────────┐
│ handleExpiredTimer() │
│ called by Spark      │
└──────────────────────┘
       │
       ▼
┌──────────────────────┐
│ Flush pending acks   │
│ if request exists,   │
│ then clear state     │
└──────────────────────┘
```

### State Management Implementation

The processor maintains **two types of state** per `transaction_id`:

```scala
// State for storing the request record (one per transaction)
private var _requestLogRecordState: ValueState[InputRow]

// State for storing acknowledgment records (can be multiple per transaction)
private var _ackLogRecordListState: ListState[InputRow]
```

**Matching Logic:**

| Scenario | Action |
|----------|--------|
| Request arrives, no pending acks | Store request in state, wait |
| Request arrives, pending acks exist | Match immediately, emit output, clear ack state |
| Ack arrives, request already in state | Match immediately, emit output |
| Ack arrives, no request yet | Store ack in list state, wait |
| Timer expires | Flush any pending matches, clear all state for this key |

### Why Processing Time (Not Event Time)?

This demo uses **ProcessingTime** mode because:

1. **Simpler implementation** - No watermark management needed
2. **Predictable cleanup** - State expires after fixed wall-clock duration
3. **Suitable for real-time systems** - AdTech typically cares about "now" not historical replay

```scala
.transformWithState(
  new LogRecordProcessor(timeout_duration = stateTimeoutInmin),
  TimeMode.ProcessingTime,  // ← Processing time, not event time
  OutputMode.Append
)
```

---

## 🛠️ Troubleshooting

| Issue | Solution |
|-------|----------|
| Stream not processing | Check Kafka credentials and topic names |
| High latency in RTM | Verify RTM cluster configs are set correctly |
| State errors | Check checkpoint location permissions |
| No matched records | Ensure producer is running and topics exist |

---

## 📈 Helper Methods

```scala
// View processed output data
dataHelper.readAndParseKafkaOutputTopic(rtm_output_topic)

// View input data
dataHelper.readAndParseKafkaInputTopic(input_topic_name)

// Calculate latency statistics
dataHelper.calculateLatency(outputTopicName)
```

---

## 📚 Additional Resources

- [Spark Structured Streaming Documentation](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [transformWithState API Guide](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/streaming/StatefulProcessor.html)
- [Databricks Streaming Best Practices](https://docs.databricks.com/structured-streaming/index.html)

---

**Happy Streaming! 🎉**
