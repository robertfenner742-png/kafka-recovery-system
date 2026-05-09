# Kafka Multi-Region Disaster Recovery, Failover and Fallback Architecture

A production-style local simulation of a highly available and disaster-resilient Apache Kafka infrastructure using:

- Multi-broker Kafka clusters
- Multi-region deployment simulation
- Cross-cluster replication using MirrorMaker2
- Automatic failover and fallback support
- Distributed producer and consumer architecture
- PostgreSQL persistence layer
- Kafka UI for management
- Prometheus + Grafana monitoring stack

---

# Problem Statement

Modern distributed systems depend heavily on Apache Kafka for:

- Event-driven communication
- Real-time analytics
- Distributed microservices
- Async processing
- CDC pipelines
- Streaming workloads
- Large-scale data ingestion

However, production Kafka systems face several critical challenges:

## Challenges

### Single Region Failure

If an entire region/datacenter goes down:

- Producers stop publishing
- Consumers stop consuming
- Critical data pipelines fail
- Event loss risk increases

---

### Broker Failures

If one or more Kafka brokers fail:

- Partition leaders become unavailable
- Producers receive timeout exceptions
- Consumers experience lag spikes
- Message durability becomes uncertain

---

### Network Partition

Network issues between services and brokers can cause:

- Temporary unavailability
- Duplicate processing
- Offset synchronization issues
- Consumer rebalance storms

---

### Disaster Recovery

Organizations require:

- Secondary backup clusters
- Real-time replication
- Fast recovery mechanisms
- Consumer continuity
- Minimal downtime

---

### High Availability Requirements

Modern systems require:

- Zero or minimal downtime
- Data durability
- Multi-region replication
- Automatic failover
- Automatic fallback
- Horizontal scalability
- Observability and monitoring

---

# Objective

This project simulates a production-grade Kafka disaster recovery architecture locally using Docker Compose.

The setup demonstrates:

- Multi-region Kafka deployment
- Cross-cluster replication
- Automatic failover support
- Automatic fallback support
- High availability Kafka configuration
- Distributed producer/consumer setup
- Monitoring and observability
- Data persistence using PostgreSQL

---

# Architecture Overview

```text
                                     ┌────────────────────┐
                                     │   Producer App     │
                                     └─────────┬──────────┘
                                               │
                                               ▼

                    ┌────────────────────────────────────────────┐
                    │       PRIMARY REGION - CLUSTER A           │
                    │                                            │
                    │  Broker A   Broker B   Broker C            │
                    │                                            │
                    │  RF=2 | ISR=2 | Partitions=12              │
                    └────────────────────────────────────────────┘
                                       │
                                       │
                                       │ MirrorMaker2 Replication
                                       ▼

                    ┌────────────────────────────────────────────┐
                    │      SECONDARY REGION - CLUSTER B          │
                    │                                            │
                    │  Broker A   Broker B   Broker C            │
                    │                                            │
                    │  DR / Backup / Failover Cluster            │
                    └────────────────────────────────────────────┘
                                       │
                                       ▼

                             ┌──────────────────┐
                             │ Consumer Services│
                             └────────┬─────────┘
                                      │
                                      ▼

                             ┌──────────────────┐
                             │   PostgreSQL DB  │
                             └──────────────────┘


                ┌─────────────────────────────────────┐
                │ Monitoring & Observability          │
                │                                     │
                │ Prometheus + Grafana + Kafka UI     │
                └─────────────────────────────────────┘
```

---

# Core Concepts Covered

This project demonstrates:

- High Availability (HA)
- Disaster Recovery (DR)
- Multi-Region Kafka Setup
- Failover and Fallback Strategies
- Distributed Messaging
- Topic Replication
- Offset Synchronization
- Broker Redundancy
- Consumer Group Recovery
- Horizontal Scaling
- Observability and Monitoring

---

# Infrastructure Components

# 1. Kafka Cluster A (Primary Region)

Primary Kafka cluster where producers initially publish messages.

## Configuration

- 3 Kafka brokers
- 3 ZooKeeper nodes
- Replication factor = 2
- Min ISR = 2
- 12 partitions per topic

## Brokers

| Broker | Internal Port | External Port |
|---|---|---|
| Broker A | 9092 | 29092 |
| Broker B | 9093 | 29093 |
| Broker C | 9094 | 29094 |

---

# 2. Kafka Cluster B (Secondary Region)

Secondary Kafka cluster acting as:

- Disaster recovery cluster
- Backup cluster
- Failover cluster
- Fallback cluster

## Configuration

- 3 Kafka brokers
- 3 ZooKeeper nodes
- Cross-region replication target

## Brokers

| Broker | Internal Port | External Port |
|---|---|---|
| Broker A | 9095 | 29095 |
| Broker B | 9096 | 29096 |
| Broker C | 9097 | 29097 |

---

# 3. ZooKeeper Ensembles

Both Kafka clusters use dedicated ZooKeeper ensembles.

## Why Multiple ZooKeepers?

Using multiple ZooKeeper nodes provides:

- Quorum-based consistency
- Better fault tolerance
- Leader election support
- Metadata durability

---

# 4. MirrorMaker2 (MM2)

MirrorMaker2 replicates data between Cluster A and Cluster B.

## Responsibilities

- Topic replication
- Message replication
- Consumer offset synchronization
- Disaster recovery support
- Cross-region synchronization

---

# Failover and Fallback Architecture

This project includes both:

- Failover
- Fallback

support patterns.

---

# What is Failover?

Failover means:

When the primary Kafka cluster becomes unavailable, applications automatically switch to the secondary cluster.

---

# Failover Flow

```text
Primary Cluster Available
        │
        ▼
 Producer → Cluster A

        │
        │ Cluster A Failure
        ▼

 Producer → Cluster B
 Consumer → Cluster B
```

---

# What is Fallback?

Fallback means:

Once the primary cluster recovers, traffic gradually returns back to the original primary cluster.

---

# Fallback Flow

```text
Cluster A Recovered
        │
        ▼

 Producer switches back to Cluster A
 Consumer switches back to Cluster A

 Cluster B remains DR standby
```

---

# How Failover is Achieved

The consumer service is configured with:

```yaml
PRIMARY_BOOTSTRAP_SERVERS: kafka-cluster-a-broker-a:9092,kafka-cluster-a-broker-b:9093,kafka-cluster-a-broker-c:9094
SECONDARY_BOOTSTRAP_SERVERS: kafka-cluster-b-broker-a:9095,kafka-cluster-b-broker-b:9096,kafka-cluster-b-broker-c:9097
```

This enables applications to:

1. Attempt connection with primary cluster
2. Detect failures
3. Automatically switch to secondary cluster
4. Continue processing messages

---

# How Fallback is Achieved

Once Cluster A becomes healthy again:

1. Health checks detect recovery
2. Applications reconnect to primary cluster
3. Consumers resume normal operations
4. Secondary cluster becomes standby again

---

# High Availability Configuration

Each broker uses:

```yaml
KAFKA_DEFAULT_REPLICATION_FACTOR: 2
KAFKA_MIN_INSYNC_REPLICAS: 2
KAFKA_NUM_PARTITIONS: 12
```

---

# Why These Settings Matter

## Replication Factor = 2

Each topic partition is replicated to 2 brokers.

Benefits:

- Prevents data loss
- Improves availability
- Supports broker failures

---

## Min ISR = 2

Producer acknowledgements succeed only when:

- At least 2 replicas are in sync

Benefits:

- Stronger consistency
- Better durability guarantees

---

## 12 Partitions

Benefits:

- Better parallelism
- Higher throughput
- More consumer scalability

---

# Kafka UI

Kafka UI allows management of both Kafka clusters.

## Features

- Topic management
- Message browsing
- Consumer lag monitoring
- Broker monitoring
- Partition visualization
- Cluster inspection

## Access

```text
http://localhost:8087
```

---

# Prometheus Monitoring

Prometheus collects infrastructure metrics.

## Access

```text
http://localhost:9090
```

---

# Grafana Dashboards

Grafana visualizes:

- Kafka metrics
- Replication lag
- Broker health
- Consumer lag
- System metrics

## Access

```text
http://localhost:3000
```

---

# PostgreSQL Database

PostgreSQL acts as the downstream persistence layer.

## Database Configuration

| Property | Value |
|---|---|
| Host | localhost |
| Port | 5436 |
| Database | mydb |
| Username | postgres |
| Password | password |

---

# Producer Application

Producer publishes messages into Cluster A.

## Responsibilities

- Event generation
- Topic publishing
- Throughput simulation
- Failover testing

---

# Consumer Application

Consumer service supports distributed consumption.

## Responsibilities

- Distributed processing
- Offset management
- Failover handling
- PostgreSQL persistence
- Horizontal scaling

---

# Observability Stack

The setup includes:

| Component | Purpose |
|---|---|
| Prometheus | Metrics collection |
| Grafana | Dashboard visualization |
| Kafka UI | Kafka management |
| Logs | Troubleshooting |

---

# Exposed Ports

| Service | Port |
|---|---|
| Kafka UI | 8087 |
| Prometheus | 9090 |
| Grafana | 3000 |
| PostgreSQL | 5436 |
| Producer App | 1992 |

---

# Project Structure

```text
kafka-disaster-recovery -
├── distributed-kafka
│   ├── Dockerfile
│   ├── HELP.md
│   ├── mvnw
│   ├── mvnw.cmd
│   ├── pom.xml
│   ├── src
│   └── target
├── distributer-kafka-consumer
│   ├── Dockerfile
│   ├── HELP.md
│   ├── mvnw
│   ├── mvnw.cmd
│   ├── pom.xml
│   ├── src
│   └── target
├── README.md
└── simulate-local
    ├── docker-compose.yml
    ├── grafana
    ├── mm2.properties
    └── prometheus
```

---

# Prerequisites

Install:

- Docker
- Docker Compose

---

# Start Infrastructure

```bash
docker compose up -d
```

---

# Verify Running Containers

```bash
docker ps
```

---

# View Logs

## MirrorMaker2

```bash
docker logs -f MirrorMaker2
```

## Producer

```bash
docker logs -f kafka-producer-app
```

---

# Disaster Recovery Testing

# Scenario 1 — Single Broker Failure

Stop one broker:

```bash
docker stop kafka-cluster-a-broker-a
```

Expected Result:

- Cluster remains operational
- Producers continue publishing
- Consumers continue consuming
- Replication continues

---

# Scenario 2 — Multiple Broker Failure

Stop two brokers:

```bash
docker stop kafka-cluster-a-broker-a
docker stop kafka-cluster-a-broker-b
```

Expected Result:

- ISR violations may occur
- Some partitions may become unavailable
- Failover logic may activate

---

# Scenario 3 — Complete Primary Cluster Failure

```bash
docker stop kafka-cluster-a-broker-a
docker stop kafka-cluster-a-broker-b
docker stop kafka-cluster-a-broker-c
```

Expected Result:

- Producers switch to Cluster B
- Consumers switch to Cluster B
- Message processing continues
- DR cluster becomes active

---

# Scenario 4 — Primary Cluster Recovery (Fallback)

Restart Cluster A:

```bash
docker start kafka-cluster-a-broker-a
docker start kafka-cluster-a-broker-b
docker start kafka-cluster-a-broker-c
```

Expected Result:

- Applications detect healthy primary cluster
- Traffic returns to Cluster A
- Cluster B returns to standby mode

---

# Useful Kafka Commands

# Create Topic

```bash
docker exec -it kafka-cluster-a-broker-a \
kafka-topics --create \
--topic orders \
--bootstrap-server localhost:9092 \
--partitions 12 \
--replication-factor 2
```

---

# List Topics

```bash
docker exec -it kafka-cluster-a-broker-a \
kafka-topics --list \
--bootstrap-server localhost:9092
```

---

# Consume Messages

```bash
docker exec -it kafka-cluster-a-broker-a \
kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic orders \
--from-beginning
```

---

# Shutdown Infrastructure

```bash
docker compose down
```

---

# Production Improvements

This setup can be extended with:

- KRaft mode (remove ZooKeeper)
- TLS/SSL security
- SASL authentication
- Schema Registry
- Kubernetes deployment
- Helm charts
- OpenTelemetry tracing
- Distributed tracing
- Kafka Connect
- Debezium CDC
- Auto-scaling
- Auto-healing
- Active-active replication

---

# Technologies Used

| Technology | Purpose |
|---|---|
| Apache Kafka | Distributed streaming platform |
| ZooKeeper | Cluster coordination |
| MirrorMaker2 | Cross-cluster replication |
| PostgreSQL | Persistence layer |
| Prometheus | Metrics collection |
| Grafana | Monitoring dashboards |
| Kafka UI | Kafka management |
| Docker Compose | Infrastructure orchestration |

---

# Conclusion

This project simulates a real-world enterprise Kafka disaster recovery architecture with:

- Multi-region deployment
- High availability
- Failover support
- Fallback recovery
- Replication
- Observability
- Distributed consumers
- Producer resiliency

It is highly useful for:

- Learning Kafka internals
- Understanding DR architecture
- Practicing failover testing
- Building HA systems
- Performance benchmarking
- Observability experiments
- Distributed systems learning