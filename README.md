# Kafka → Elasticsearch Sink Connector Spike

Demonstrates end-to-end message flow from a Java Kafka producer through the
[Confluent Elasticsearch Sink Connector](https://docs.confluent.io/kafka-connectors/elasticsearch/current/overview.html)
into Elasticsearch.

## Architecture

```
┌─────────────────┐        ┌───────────────┐        ┌──────────────────┐
│  Java Producer  │──────▶ │  Kafka (KRaft)│──────▶ │  Kafka Connect   │
│  (OrderEvents)  │        │  topic:orders │        │  ES Sink         │
└─────────────────┘        └───────────────┘        └────────┬─────────┘
                                                             │
                                                             ▼
                                                   ┌──────────────────┐
                                                   │  Elasticsearch   │
                                                   │  index: orders   │
                                                   └──────────────────┘
```

**Components:**

| Service | Image | Port |
|---|---|---|
| Kafka (KRaft) | `confluentinc/cp-kafka:7.6.1` | `9092` |
| Kafka Connect | custom (cp-kafka-connect + ES plugin) | `8083` |
| Elasticsearch | `docker.elastic.co/elasticsearch/elasticsearch:8.13.4` | `9200` |
| Kibana | `docker.elastic.co/kibana/kibana:8.13.4` | `5601` |

The producer sends `OrderEvent` JSON messages (orderId, customerId, product, quantity, totalPrice, status, createdAt) to the `orders` topic. The connector upserts each message into the `orders` Elasticsearch index, using the Kafka message key (orderId) as the document `_id`.

---

## Prerequisites

- Docker + Docker Compose
- Java 21 + Maven (for the producer)

---

## Running the Spike

### 1. Start the stack

```bash
docker compose up -d --build
```

The `--build` flag builds the custom Kafka Connect image with the ES connector installed. This takes a few minutes on first run.

Wait for all services to be healthy:

```bash
docker compose ps
```

### 2. Register the Elasticsearch Sink Connector

```bash
./scripts/register-connector.sh
```

Verify it is running:

```bash
curl -s http://localhost:8083/connectors/elasticsearch-sink-orders/status | jq .
```

Expected `state`: `RUNNING`.

### 3. Build and run the Java producer

```bash
# Build the fat JAR
./scripts/build-producer.sh

# Produce 20 messages (default)
./scripts/run-producer.sh

# Produce a custom number with a shorter delay
MESSAGE_COUNT=100 DELAY_MS=100 ./scripts/run-producer.sh
```

### 4. Verify documents in Elasticsearch

```bash
# Document count
curl -s "http://localhost:9200/orders/_count" | jq .

# Sample documents
curl -s "http://localhost:9200/orders/_search?size=5&pretty"

# Fetch a specific order by ID
curl -s "http://localhost:9200/orders/_doc/<orderId>" | jq .
```

Or open Kibana at http://localhost:5601 → **Discover** → create a data view for `orders`.

---

## Project Structure

```
.
├── docker-compose.yml          # Full stack definition
├── connect/
│   └── Dockerfile              # Kafka Connect + ES connector plugin
├── connector/
│   └── elasticsearch-sink.json # Connector configuration
├── producer/
│   ├── pom.xml
│   └── src/main/java/com/spike/producer/
│       ├── OrderEvent.java         # Message model
│       └── OrderEventProducer.java # Kafka producer
└── scripts/
    ├── build-producer.sh
    ├── run-producer.sh
    └── register-connector.sh
```

---

## Key Configuration Decisions

| Setting | Value | Reason |
|---|---|---|
| `write.method` | `upsert` | Re-running the producer won't create duplicates |
| `key.ignore` | `false` | Uses orderId as ES `_id` for deterministic document identity |
| `schema.ignore` | `true` | Raw JSON values — no Schema Registry needed for this spike |
| `value.converter` | `ByteArrayConverter` | Passes raw JSON bytes directly; ES connector handles deserialization |
| `xpack.security.enabled` | `false` | Simplified local setup — enable and configure TLS/auth for any real environment |

---

## Teardown

```bash
docker compose down -v   # -v removes volumes (clears all Kafka and ES data)
```
