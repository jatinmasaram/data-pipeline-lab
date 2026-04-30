# High-Throughput Kafka Sensor Producer (Python)

Design Decisions, Trade-offs, and Failure Modes

This is a Kafka producer designed to simulate industrial sensor data and sustain high throughput (~150K–200K events/sec) under real system constraints.

Environment:

* 62 GB RAM server
* ~20 GB available memory during execution
* 16 parallel producer workers
* Kafka broker running locally

Pipeline:

Sensor Simulation → Kafka Producer → Kafka Broker → Flink Consumer

The goal was not just to send data, but to understand how Kafka behaves under sustained load, where bottlenecks appear, and how failure modes emerge.

---

## 1. Sensor Simulation (Deterministic + Noise)

Instead of using random data, sensor values are generated using a sine wave model:

value = base + amp × sin(2π × freq × t) + noise

Parameters:

* base → steady-state value
* amp → variation range
* freq → oscillation frequency
* noise → random jitter

Sensors configured:

* Temperature → base=300, amp=15, freq=0.5
* Pressure → base=12, amp=3, freq=1.0
* Airflow → base=55, amp=10, freq=1.5

Rationale:

* Produces structured, oscillating data instead of random noise
* Useful for testing time-window aggregations in Flink
* Closer to real industrial telemetry behavior

---

## 2. Serialization Choice

Replaced Python’s json module with orjson.

Reason:

* Lower serialization latency under high throughput
* Returns bytes directly (no encoding step)
* Uses SIMD and optimized memory handling

Impact:

* Reduced CPU overhead at ~200K events/sec
* Faster producer loop execution

Future:

* Migration to Protobuf for schema enforcement and smaller payload size

---

## 3. Kafka Producer Configuration

```python
Producer({
  "bootstrap.servers": "localhost:9092",
  "acks": 1,
  "linger.ms": 5,
  "batch.size": 1024 * 1024,
  "compression.type": "lz4",
  "queue.buffering.max.messages": 1000000,
  "queue.buffering.max.kbytes": 2097152,
  "batch.num.messages": 50000,
  "retries": 3
})
```

---

### acks = 1 (Latency vs Durability)

* Leader-only acknowledgement
* No wait for replica confirmation

Effect:

* Lower latency
* Higher throughput

Risk:

* Data loss if leader fails before replication

Decision:
Acceptable for high-frequency sensor data where occasional loss is tolerable.

---

### Batching (linger.ms + batch.size)

* linger.ms = 5 → waits up to 5 ms to accumulate messages
* batch.size = 1 MB → max batch size per partition

Effect:

* Reduces number of network calls
* Improves throughput significantly

Trade-off:

* Slight increase in latency (~5 ms)

---

### Compression (LZ4)

* Applied at batch level before network send

Reason:

* Fast compression/decompression
* Good compression ratio for streaming workloads

Effect:

* Reduced network bandwidth usage
* Lower end-to-end latency

---

## 4. Backpressure and Memory Behavior

Key observation:
The producer can generate data faster than the broker can acknowledge.

When this happens:

* Messages accumulate in producer buffer
* Memory usage grows
* Risk of process termination

Configuration used:

* queue.buffering.max.messages = 1,000,000
* queue.buffering.max.kbytes = 2 GB

Behavior:

* Once limits are reached, produce() blocks
* Prevents unbounded memory growth

---

### Failure Mode: OOM Kill

Observed sequence:

* Producer serializes events rapidly
* Broker becomes slow (I/O, network, load)
* ACKs are delayed
* Buffer fills up
* Memory usage spikes
* Linux OOM killer terminates the process

This was the primary failure encountered during testing.

---

### Memory Calculation

Available memory: ~20 GB
Buffer per producer: 2 GB
Parallel producers: 16

Worst case:
16 × 2 GB = 32 GB → exceeds available memory

Adjustment:
Reduced effective buffer usage (~1.2 GB per producer)

Key point:
Buffer configuration must be aligned with total system memory and parallelism.

---

### Broker-Side Considerations

Producer stability depends heavily on broker performance.

Observed bottlenecks and fixes:

* Low thread capacity → increase num.io.threads and num.network.threads
* Slow disk → use NVMe (faster than HDD)
* Network latency → tune TCP buffers

Kafka writes are handled via OS page cache, so disk performance directly affects ACK latency.

---

## 5. Partitioning Strategy

Current:

* No key → round-robin partitioning

Effect:

* Balanced load across partitions
* Maximum throughput

Trade-off:

* No ordering guarantee per sensor

Planned:

* Use tag_id as key
* Enables per-sensor ordering
* Required for Flink keyed window aggregations

---

## 6. Retry Behavior and Ordering

Configuration:

* retries = 3

Issue:
Retries can break ordering.

Example:

* Batch A fails
* Batch B succeeds
* Batch A retry succeeds later

Result:
Order becomes B → A

Solution:

* enable.idempotence = true
* max.in.flight.requests.per.connection = 1

Effect:

* Prevents duplicates
* Preserves ordering

---

## 7. Producer Internals (poll)

producer.poll(0) is called in every loop iteration.

Function:

* Processes delivery reports
* Frees completed messages from buffer
* Handles ACK events

Without poll():

* ACKs remain unprocessed
* Buffer does not release memory
* Throughput degrades over time

---

## 8. Message Schema

```json
{
  "seq": 1,
  "tag_id": 1,
  "sensor": "Temperature",
  "msec": "ISO 8601 timestamp",
  "value": 301.47,
  "quality": 100
}
```

Notes:

* Timestamp used for Flink windowing
* quality simulates sensor degradation (5% probability)

---

## 9. Key Learnings

1. Broker throughput is often the bottleneck, not the producer
2. Backpressure is expected behavior, not a failure
3. Memory limits must be calculated with parallelism in mind

---

## 10. Next Steps

* Flink consumer (windowed aggregations, parallelism = 16)
* Protobuf integration (schema + binary format)
* Idempotent producer configuration
* Real-time alerting based on data quality

---

The full producer code is available on GitHub. The next phase focuses on Flink processing and system-level scaling challenges.
