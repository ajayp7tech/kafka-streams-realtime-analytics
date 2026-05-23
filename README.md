# Kafka Streams Real-Time Analytics

<p align="left">
  <img src="https://img.shields.io/badge/Java_21-ED8B00?style=flat-square&logo=openjdk&logoColor=white"/>
  <img src="https://img.shields.io/badge/Kafka_Streams-231F20?style=flat-square&logo=apachekafka&logoColor=white"/>
  <img src="https://img.shields.io/badge/Spring_Boot_3.x-6DB33F?style=flat-square&logo=springboot&logoColor=white"/>
  <img src="https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white"/>
  <img src="https://img.shields.io/badge/Grafana-F46800?style=flat-square&logo=grafana&logoColor=white"/>
  <img src="https://img.shields.io/badge/License-MIT-green?style=flat-square"/>
</p>

A real-time stream processing application built with Kafka Streams and Java 21. Consumes high-volume event streams, applies windowed aggregations and stateful transformations, routes failures to a dead letter queue, and exposes live analytics via a REST API — all without a separate processing cluster.

Built to demonstrate production-grade Kafka Streams patterns: exactly-once semantics, consumer group management, state store queries, DLQ handling, and idempotent processing.

---

## 📐 Architecture overview

```
                    ┌─────────────────────────────────┐
                    │         Event Producers          │
                    │  (orders, clicks, transactions)  │
                    └────────────────┬────────────────┘
                                     │
                    ┌────────────────▼────────────────┐
                    │           Apache Kafka           │
                    │                                  │
                    │  Topics:                         │
                    │  • raw-events                    │
                    │  • enriched-events               │
                    │  • aggregated-metrics            │
                    │  • alerts                        │
                    │  • events-dlq                    │
                    └────────────────┬────────────────┘
                                     │
                    ┌────────────────▼────────────────┐
                    │      Kafka Streams Processor     │
                    │         (Java 21)                │
                    │                                  │
                    │  ┌──────────────────────────┐   │
                    │  │  Stream Topology         │   │
                    │  │                          │   │
                    │  │  filter → enrich         │   │
                    │  │    → window aggregate    │   │
                    │  │    → alert detection     │   │
                    │  │    → DLQ routing         │   │
                    │  └──────────────────────────┘   │
                    │                                  │
                    │  ┌──────────────────────────┐   │
                    │  │  State Stores (RocksDB)  │   │
                    │  │  • order-counts          │   │
                    │  │  • revenue-windows       │   │
                    │  │  • error-rates           │   │
                    │  └──────────────────────────┘   │
                    └──────┬──────────────┬───────────┘
                           │              │
              ┌────────────▼──┐    ┌──────▼──────────┐
              │  REST API     │    │    Grafana       │
              │  /metrics     │    │   Dashboard      │
              │  /alerts      │    │  (live charts)   │
              └───────────────┘    └─────────────────┘
```

---

## ✨ Features

- **Real-time windowed aggregations** — tumbling and sliding windows for order counts, revenue totals, and error rates per minute/hour
- **Stateful stream processing** — RocksDB-backed state stores for low-latency lookups without external DB calls
- **Dead letter queue (DLQ)** — malformed or unprocessable events automatically routed to `events-dlq` topic with error metadata attached
- **Idempotent processing** — exactly-once semantics enabled to prevent duplicate aggregations under failure/retry scenarios
- **Alert detection** — real-time threshold monitoring publishes to `alerts` topic when error rate exceeds configured limits
- **Interactive queries** — REST API exposes live state store data without re-consuming the stream
- **Grafana dashboard** — pre-built dashboard visualising throughput, error rates, and windowed metrics in real time
- **Java 21 virtual threads** — lightweight concurrency for handling parallel stream tasks efficiently

---

## 🛠 Tech stack

| Layer | Technology |
|---|---|
| Language | Java 21 (Virtual Threads, Records, Pattern Matching) |
| Stream Processing | Apache Kafka Streams 3.6 |
| Framework | Spring Boot 3.x, Spring Kafka |
| State Store | RocksDB (embedded via Kafka Streams) |
| Metrics | Micrometer, Prometheus, Grafana |
| Testing | JUnit 5, Kafka Streams TestDriver, Testcontainers |
| Build | Maven |
| Containers | Docker, Docker Compose |
| CI/CD | GitHub Actions |

---

## 📁 Project structure

```
kafka-streams-realtime-analytics/
├── .github/
│   └── workflows/
│       └── ci.yml
├── src/
│   ├── main/
│   │   ├── java/com/ajayp/streams/
│   │   │   ├── KafkaStreamsApplication.java
│   │   │   ├── config/
│   │   │   │   ├── KafkaStreamsConfig.java      # Streams topology builder
│   │   │   │   └── PrometheusConfig.java
│   │   │   ├── topology/
│   │   │   │   ├── EventStreamTopology.java     # Main stream DSL
│   │   │   │   ├── WindowAggregator.java        # Tumbling/sliding windows
│   │   │   │   ├── AlertDetector.java           # Threshold monitoring
│   │   │   │   └── DlqRouter.java              # Dead letter queue routing
│   │   │   ├── processor/
│   │   │   │   ├── EventEnricher.java           # Stateful enrichment
│   │   │   │   └── IdempotencyFilter.java       # Duplicate detection
│   │   │   ├── controller/
│   │   │   │   ├── MetricsController.java       # Query state stores via REST
│   │   │   │   └── AlertsController.java
│   │   │   ├── model/
│   │   │   │   ├── RawEvent.java               # Java 21 Record
│   │   │   │   ├── EnrichedEvent.java           # Java 21 Record
│   │   │   │   ├── AggregatedMetric.java        # Java 21 Record
│   │   │   │   └── Alert.java                  # Java 21 Record
│   │   │   └── serde/
│   │   │       └── JsonSerde.java              # Custom JSON serializer
│   │   └── resources/
│   │       ├── application.yml
│   │       └── grafana/
│   │           └── dashboard.json              # Pre-built Grafana dashboard
│   └── test/
│       └── java/com/ajayp/streams/
│           ├── topology/
│           │   └── EventStreamTopologyTest.java # TopologyTestDriver tests
│           └── integration/
│               └── StreamsIntegrationTest.java  # Testcontainers
├── docker-compose.yml
├── Dockerfile
└── pom.xml
```

---

## 🚀 Getting started

### Prerequisites

- Java 21+
- Docker & Docker Compose
- Maven 3.8+

### Run locally

```bash
# Clone the repo
git clone https://github.com/ajayp7tech/kafka-streams-realtime-analytics.git
cd kafka-streams-realtime-analytics

# Start Kafka, Zookeeper, Prometheus, and Grafana
docker-compose up -d

# Run the streams application
mvn spring-boot:run

# Services available at:
# Streams REST API  → http://localhost:8080
# Grafana Dashboard → http://localhost:3000  (admin / admin)
# Kafka UI          → http://localhost:8090
# Prometheus        → http://localhost:9090
```

### Produce test events

```bash
# Publish sample events to the raw-events topic
docker exec -it kafka kafka-console-producer \
  --broker-list localhost:9092 \
  --topic raw-events

# Paste a sample event:
{"eventId":"EVT-001","type":"ORDER_PLACED","customerId":"CUST-42","amount":59.99,"timestamp":"2025-04-10T14:00:00Z"}
```

---

## 📡 REST API

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/v1/metrics/orders/count` | Live order count from state store |
| `GET` | `/api/v1/metrics/revenue/window` | Revenue aggregated by time window |
| `GET` | `/api/v1/metrics/errors/rate` | Current error rate per minute |
| `GET` | `/api/v1/alerts/active` | Active threshold breach alerts |
| `GET` | `/actuator/health` | Application health |
| `GET` | `/actuator/prometheus` | Prometheus metrics scrape endpoint |

### Sample response — windowed revenue

```json
GET /api/v1/metrics/revenue/window

{
  "windowStart": "2025-04-10T14:00:00Z",
  "windowEnd":   "2025-04-10T14:05:00Z",
  "totalRevenue": 18540.75,
  "orderCount": 312,
  "avgOrderValue": 59.43,
  "currency": "USD"
}
```

---

## 🔀 Stream topology

```
raw-events topic
      │
      ▼
  [Filter]  ── rejects null/malformed events ──▶ events-dlq
      │
      ▼
  [Enrich]  ── adds customer tier, region metadata
      │
      ▼
  [Branch]
      ├──▶ ORDER events
      │         │
      │         ▼
      │    [Tumbling Window - 5 min]
      │    Count orders, sum revenue
      │         │
      │         ▼
      │    aggregated-metrics topic
      │
      └──▶ ERROR events
                │
                ▼
           [Sliding Window - 1 min]
           Calculate error rate
                │
                ├── rate > threshold ──▶ alerts topic
                └── rate OK          ──▶ enriched-events topic
```

---

## 🧪 Testing approach

Kafka Streams testing uses the **TopologyTestDriver** — no real Kafka cluster needed for unit tests:

```java
@Test
void shouldAggregateOrderCountInTumblingWindow() {
    // Push test events through the topology
    inputTopic.pipeInput("key-1", buildOrderEvent("CUST-01", 49.99));
    inputTopic.pipeInput("key-2", buildOrderEvent("CUST-02", 89.99));
    inputTopic.pipeInput("key-3", buildOrderEvent("CUST-01", 25.00));

    // Assert aggregated output
    KeyValue<String, AggregatedMetric> result = outputTopic.readKeyValue();
    assertThat(result.value.orderCount()).isEqualTo(3);
    assertThat(result.value.totalRevenue()).isEqualTo(164.98);
}
```

---

## 📊 Test coverage

| Layer | Coverage |
|---|---|
| Stream topology | 93% |
| REST controllers | 88% |
| Serde / models | 95% |
| Overall | **92%** |

---

## 🗺 Roadmap

- [x] Windowed aggregations (tumbling + sliding)
- [x] DLQ routing with error metadata
- [x] Idempotent exactly-once processing
- [x] Interactive REST queries on state stores
- [x] Grafana dashboard + Prometheus metrics
- [ ] Schema Registry integration (Avro serialization)
- [ ] Multi-partition consumer group scaling demo
- [ ] Kafka Streams changelog topic compaction config
- [ ] AWS MSK deployment guide

---

## 👤 Author

**Ajay Pingali** — Senior Java Developer

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=flat-square&logo=linkedin&logoColor=white)](https://linkedin.com/in/ajayp7tech)
[![Portfolio](https://img.shields.io/badge/Portfolio-0f3460?style=flat-square&logo=github&logoColor=white)](https://ajayp7tech.github.io)

---

## 📄 License

This project is licensed under the MIT License — see [LICENSE](LICENSE) for details.
