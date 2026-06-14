# Real-Time Fraud Detection System

**Event-Driven, Multi-Module Fraud Scoring Pipeline on Apache Kafka**

[![Java](https://img.shields.io/badge/Java-21-orange?logo=openjdk)](https://openjdk.org/projects/jdk/21/)
[![Spring Boot](https://img.shields.io/badge/Spring_Boot-4.0.1-6DB33F?logo=springboot)](https://spring.io/projects/spring-boot)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-Confluent_Cloud-231F20?logo=apachekafka)](https://kafka.apache.org/)
[![Redis](https://img.shields.io/badge/Redis-Sorted_Sets%2FHashes-DC382D?logo=redis)](https://redis.io/)
[![Maven](https://img.shields.io/badge/Build-Maven_Multi--Module-C71A36?logo=apachemaven)](https://maven.apache.org/)

A multi-module Spring Boot system that ingests financial transactions over a REST API, streams them through **Apache Kafka** partitioned by account ID, and scores each one in real time using a **pluggable, configuration-driven rule engine**. Risk scores combine static rules (blacklists, time-of-day) with statistical anomaly detection (EWMA-based Z-scores) and behavioral velocity tracking — all backed by Redis for sub-millisecond lookups.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [How It Works](#how-it-works)
  - [1. Transaction Ingestion](#1-transaction-ingestion)
  - [2. Kafka Partitioning & Ordering](#2-kafka-partitioning--ordering)
  - [3. The Rule Engine](#3-the-rule-engine)
  - [4. Fraud Rules](#4-fraud-rules)
  - [5. Decisioning](#5-decisioning)
- [Features](#features)
- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)
- [Configuration Reference](#configuration-reference)
- [Setup & Running](#setup--running)
- [API Reference](#api-reference)
- [Design Decisions](#design-decisions)
- [Roadmap](#roadmap)

---

## Architecture Overview

The system is split into three Maven modules: a thin ingestion API, a shared DTO library, and the scoring engine. Kafka decouples ingestion from scoring, so the engine can be scaled independently of the API tier.

```
┌────────────────────┐        ┌──────────────────────────────┐        ┌─────────────────────────────┐
│   Transaction API   │  POST  │      Apache Kafka (Cloud)     │ poll   │   Fraud Detection Engine     │
│  (Spring Web MVC)   │ ─────► │   topic: "Transactions"       │ ─────► │   (Spring Kafka Consumer)     │
│                     │        │   partitioned by accountId    │        │                               │
│  /tx  ──► builds    │        │   (strict per-account order)  │        │  TransactionHandler           │
│  TransactionEvent   │        └──────────────────────────────┘        │     └─► RiskEngine             │
└─────────────────────┘                                                 │           ├─ BlacklistRule     │
                                                                          │           ├─ SplurgeRule (Z)   │
                                                                          │           ├─ TimeRule          │
                                                                          │           └─ VelocityRule      │
                                                                          │                 │              │
                                                                          │                 ▼              │
                                                                          │     APPROVED / REVIEW / BLOCK  │
                                                                          └───────────┬───────────────────┘
                                                                                      │
                                                                                      ▼
                                                                          ┌─────────────────────┐
                                                                          │        Redis          │
                                                                          │  • blacklisted IPs    │
                                                                          │    (Set)              │
                                                                          │  • velocity windows    │
                                                                          │    (Sorted Set, TTL)   │
                                                                          │  • spending profiles   │
                                                                          │    (Hash: mean, var)   │
                                                                          └─────────────────────┘
```

---

## How It Works

### 1. Transaction Ingestion

`transaction-api` exposes a single endpoint, `POST /tx`, which accepts a minimal `TransactionRequest` (`transactionId`, `accountId`, `amount`). The controller enriches it server-side — stamping the request's source IP and the current epoch timestamp — and builds a shared `TransactionEvent` DTO (from `common-dtos`) before publishing it to Kafka via `KafkaTemplate`.

```
Client                          transaction-api                        Kafka
  │── POST /tx {accountId, amount, transactionId} ─►│                      │
  │                                                  │── enrich: ip, ts ───┤
  │                                                  │── send(Transactions, accountId, event) ─►│
  │◄──────────── 202 Accepted ──────────────────────│                      │
```

The API layer does **no scoring** — it is a deliberately thin producer, keeping the ingestion path low-latency and decoupled from rule evaluation.

### 2. Kafka Partitioning & Ordering

Every `TransactionEvent` is published with the **account ID as the Kafka message key**. Kafka guarantees that all messages with the same key land on the same partition and are consumed **in send order**. This means:

- All transactions for a given account are processed strictly sequentially by `fraud-detection-engine`.
- Velocity tracking and spending-profile updates (which depend on transaction history) never race against each other for the same account.
- Different accounts can be processed fully in parallel across partitions/consumers.

The engine connects via `@KafkaListener(topics = "Transactions", groupId = "transaction-handler")` and deserializes payloads with `JacksonJsonDeserializer`, restricted to the `dtos` package for safety.

### 3. The Rule Engine

`RiskEngine` holds an injected `List<FraudRule>` — every Spring bean implementing the `FraudRule` interface is automatically collected by Spring's dependency injection. Each rule contributes an integer score; the engine sums them with an early-exit optimization:

```java
public int evaluateRisk(TransactionEvent transactionEvent) {
    int riskScore = 0;
    for (FraudRule rule : activeRules) {
        if (riskScore > blockThreshold) return riskScore; // short-circuit
        riskScore += rule.evaluate(transactionEvent);
    }
    return riskScore;
}
```

Rules are individually toggled via `@ConditionalOnProperty`, so any rule can be disabled — or new rules added — purely through `application.properties`, with **zero code changes** to the engine itself.

### 4. Fraud Rules

| Rule | Signal | Storage | Score Contribution |
|---|---|---|---|
| **BlacklistRule** | Source IP is a member of a known-bad IP set | Redis `Set` (`bip`) | Flat `100` (instant block) |
| **VelocityRule** | More than N transactions for this account within a rolling time window | Redis `Sorted Set` (score = timestamp) | Flat `30` |
| **TimeRule** | Transaction occurs during a configurable "suspicious" UTC window (default 02:00–06:00) | Stateless (timestamp math) | Flat `50` |
| **SplurgeRule** | Transaction amount deviates significantly from the account's learned spending pattern | Redis `Hash` (rolling mean & variance per account) | Scaled, capped at `85` |

**Velocity tracking (sliding window via Redis Sorted Set):**

```
ZADD user-trans:<accountId> <now_ms> <transactionId>
ZREMRANGEBYSCORE user-trans:<accountId> 0 (now_ms - windowDuration)
ZCARD user-trans:<accountId>  →  count in current window
```

The window defaults to **60 seconds**, with a limit of **5 transactions** — the 6th transaction within a minute trips the rule. The implementation keeps one extra entry beyond the limit in the set as a buffer so the boundary check remains correct across consecutive evaluations.

**Splurge detection (EWMA + Z-score):**

Each account has a running `(mean, variance)` pair stored in Redis, updated with an **Exponentially Weighted Moving Average**:

```
z      = (amount - meanPrev) / sqrt(max(variancePrev, ε))
meanₙ  = α · amount + (1 - α) · meanPrev
varₙ   = α · (amount - meanₙ)² + (1 - α) · variancePrev
```

- **Cold start**: the first transaction for an account seeds the profile with `mean = amount` and an inflated `variance = (amount × coldStartMultiplier)²`, avoiding false positives before enough history exists.
- **Soft threshold (default `z > 3`)**: contributes a score proportional to how far past the threshold the transaction is, capped at `85`.
- **Hard threshold (default `z > 6`)**: the profile is *not* updated — a single extreme outlier doesn't permanently skew the account's baseline.
- Below the soft threshold, the profile updates normally — the model continuously adapts to legitimate changes in spending behavior.

### 5. Decisioning

`TransactionHandler` compares the aggregated risk score against two configurable global thresholds (`fraud.global.review-threshold` = 20, `fraud.global.block-threshold` = 100) and logs the outcome as `APPROVED`, `REVIEW`, or `BLOCK`. This is the seam where a real deployment would plug in a downstream action — writing to a decision topic, calling a case-management API, or notifying the account holder.

---

## Features

- **Strictly ordered, per-account event processing** via Kafka key-based partitioning
- **Pluggable Strategy-pattern rule engine** — rules are Spring beans behind a common `FraudRule` interface, individually toggled via `@ConditionalOnProperty`
- **Online statistical anomaly detection** — EWMA-based mean/variance tracking with Z-score scaling and cold-start handling, no offline training step required
- **O(log N) velocity scoring** using Redis sorted sets with automatic window expiry
- **Repository pattern** cleanly isolates all Redis access behind `BlacklistedIPsRepository`, `SplurgeRepository`, and `VelocityRepository`
- **Configuration-driven thresholds** — every score, window size, and toggle lives in `application.properties`, no recompilation needed to retune the model
- **Early-exit scoring** — the risk engine stops evaluating further rules once the block threshold is exceeded
- **Decoupled ingestion and scoring** — the API tier and the engine can be scaled independently and deployed as separate processes

---

## Project Structure

```
RealTimeFraudDetection/
├── pom.xml                              # Parent POM — multi-module aggregator
│
├── common-dtos/                         # Shared DTO library (depended on by both services)
│   └── src/main/java/dtos/
│       └── TransactionEvent.java        # transactionId, accountId, amount, timestamp, ipAddress
│
├── transaction-api/                     # Lightweight ingestion service (port 8080)
│   └── src/main/java/com/inhuman/transactionapi/
│       ├── TransactionApiApplication.java
│       ├── contollers/
│       │   └── TransactionController.java  # POST /tx → publishes to Kafka
│       └── requests/
│           └── TransactionRequest.java     # Inbound payload shape
│
└── fraud-detection-engine/              # Scoring service (port 8081)
    └── src/main/java/com/inhuman/frauddetectionengine/
        ├── FraudDetectionEngineApplication.java
        ├── config/
        │   └── RedisConfig.java           # RedisTemplate bean (String key serializer)
        ├── interfaces/
        │   └── FraudRule.java             # Strategy interface: evaluate(TransactionEvent) → int
        ├── services/
        │   ├── TransactionHandler.java    # @KafkaListener entry point + decisioning
        │   └── RiskEngine.java            # Aggregates all FraudRule beans
        ├── rules/
        │   ├── BlacklistRule.java
        │   ├── VelocityRule.java
        │   ├── TimeRule.java
        │   └── SplurgeRule.java
        ├── repos/
        │   ├── BlacklistedIPsRepository.java
        │   ├── VelocityRepository.java
        │   └── SplurgeRepository.java
        └── models/
            ├── TransactionProfile.java    # mean, variance
            └── ZScoreCalc.java             # zScore, mean, variance (calc result)
```

---

## Tech Stack

| Concern | Library / Tool | Version |
|---|---|---|
| Language | Java | 21 |
| Framework | Spring Boot (Web MVC, Kafka, Data Redis) | 4.0.1 |
| Messaging | Apache Kafka (Confluent Cloud, SASL_SSL) | — |
| Cache / State Store | Redis (Sets, Sorted Sets, Hashes) | — |
| Serialization | Jackson (`JacksonJsonSerializer` / `Deserializer`) | — |
| Boilerplate | Lombok | — |
| Build | Maven (multi-module reactor) | — |
| Testing | JUnit 5, Spring Boot Test (Kafka & Redis test starters) | — |

---

## Configuration Reference

All scoring behavior is controlled from `fraud-detection-engine/src/main/resources/application.properties`. Key properties:

```properties
# Global decisioning thresholds
fraud.global.review-threshold=20
fraud.global.block-threshold=100

# Blacklist rule
fraud.rules.blacklist.enabled=true
fraud.rules.blacklist.key=bip
fraud.rules.blacklist.score=100

# Splurge / anomaly rule
fraud.rules.splurge.enabled=true
fraud.rules.splurge.key=trans-profile:
fraud.rules.splurge.score=20
fraud.rules.splurge.zscore.soft-threshold=3
fraud.rules.splurge.zscore.hard-threshold=6
fraud.rules.splurge.zscore.score.threshold-cap=85
fraud.rules.splurge.smoothing-factor=0.075
fraud.rules.splurge.variance.min=1e-6
fraud.rules.splurge.cold-start-multiplier=2

# Time-of-day rule
fraud.rules.time.enabled=true
fraud.rules.time.score=50
fraud.rules.time.zone-id=UTC
fraud.rules.time.start.time=2
fraud.rules.time.end.time=6

# Velocity rule
fraud.rules.velocity.enabled=true
fraud.rules.velocity.key=user-trans:
fraud.rules.velocity.score=30
fraud.rules.velocity.transaction-limit=5
fraud.rules.velocity.window.duration=60000
```

> **Tip:** to disable a rule entirely (e.g. for A/B testing or staged rollouts), set its `enabled` flag to `false` — `@ConditionalOnProperty` prevents the bean from being registered at all, so it adds zero overhead to `RiskEngine`.

---

## Setup & Running

### Prerequisites

- **Java 21**
- **Maven 3.8+**
- A **Kafka cluster** (the project ships configured for Confluent Cloud over `SASL_SSL` — substitute your own bootstrap servers / credentials, or point at a local broker by simplifying the security block)
- A **Redis** instance reachable by `fraud-detection-engine`

### 1. Clone the repository

```bash
git clone https://github.com/krishnadaga5106/realtimefrauddetection.git
cd realtimefrauddetection
```

### 2. Configure environment variables

Both `transaction-api` and `fraud-detection-engine` read Kafka credentials from the environment:

```bash
export BOOTSTRAP_SERVER=<your-kafka-bootstrap-servers>
export KAFKA_API_KEY=<your-api-key>
export KAFKA_API_SECRET=<your-api-secret>
```

Configure Redis connection details (host/port/password) for `fraud-detection-engine` via the standard `spring.data.redis.*` properties.

### 3. Build all modules

From the project root (the parent `pom.xml` aggregates all three modules):

```bash
mvn clean install
```

### 4. Run the services

In two separate terminals:

```bash
# Terminal 1 — Fraud Detection Engine (consumer, port 8081)
cd fraud-detection-engine
mvn spring-boot:run

# Terminal 2 — Transaction API (producer, port 8080)
cd transaction-api
mvn spring-boot:run
```

### 5. Send a test transaction

```bash
curl -X POST http://localhost:8080/tx/ \
  -H "Content-Type: application/json" \
  -d '{
    "transactionId": "txn-1001",
    "accountId": "acct-42",
    "amount": 250.00
  }'
```

Watch the `fraud-detection-engine` logs — each rule logs its evaluation, followed by the final decision:

```
checked for blacklist
Transactions: 1
Hour 14
Z-Score: 0.42
[APPROVED]: risk: 0, transaction: TransactionEvent(transactionId=txn-1001, ...)
```

---

## API Reference

### `POST /tx/`

Submits a new transaction for asynchronous fraud evaluation.

**Request body:**

```json
{
  "transactionId": "txn-1001",
  "accountId": "acct-42",
  "amount": 250.00
}
```

**Response:** `202 Accepted` — `"Transaction created"`

The transaction is published to the `Transactions` Kafka topic immediately; the response does **not** carry the fraud decision. The system is designed for asynchronous, eventually-consistent scoring — decisions are currently surfaced via `fraud-detection-engine` logs, with the architecture ready to extend into a decision-result topic or webhook (see [Roadmap](#roadmap)).

---

## Design Decisions

**Why Kafka with account-ID partitioning instead of a simple queue?**
Fraud signals like velocity and spending baselines are inherently **per-account, order-sensitive computations**. Keying messages by `accountId` gives strict ordering for free at the broker level — no application-side locking or sequencing logic is needed to keep one account's Redis state consistent.

**Why a Strategy-pattern rule engine over a monolithic scoring function?**
New fraud signals are added constantly in real systems, and not every signal applies to every deployment. Implementing `FraudRule` as an interface, with each rule as an independently toggleable Spring bean, means new rules are *additive* — `RiskEngine` never needs to change, and a rule can be shipped, tested, and rolled out behind a config flag without touching the aggregation logic.

**Why EWMA instead of a fixed-window average for spending baselines?**
A fixed window requires storing N historical transactions per account and recomputing statistics on every evaluation. EWMA collapses this to **two numbers per account** (`mean`, `variance`) updated in O(1), while still giving more weight to recent behavior — ideal for an online, per-event scoring path where storage and latency both matter.

**Why cap the Z-score contribution instead of letting it scale unbounded?**
A single anomalously large transaction (e.g. an account's first-ever purchase) could otherwise produce an enormous Z-score and dominate the total risk score on its own. Capping the splurge contribution (`threshold-cap`) ensures the anomaly signal is *one strong vote*, not an automatic verdict — other rules still get a meaningful say.

**Why a multi-module Maven layout instead of one Spring Boot app?**
`transaction-api` and `fraud-detection-engine` have entirely different scaling characteristics — the API is a thin, stateless write path, while the engine maintains Redis-backed state and does the heavy per-event computation. Separating them into independent deployable modules (sharing only `common-dtos`) means each can be scaled, deployed, and restarted independently, which mirrors how this would be split into microservices in production.

---

## Roadmap

- **Decision feedback channel** — publish `APPROVED` / `REVIEW` / `BLOCK` outcomes to a results topic (or expose via API) so the result is consumable by downstream systems instead of only appearing in logs
- **GeoIP-based velocity rule** — the `common-dtos`/`fraud-detection-engine` modules already depend on MaxMind GeoIP2; a planned rule will flag transactions originating from geographically implausible locations relative to an account's recent history ("impossible travel")
- **Dynamic rule configuration** — move thresholds from `application.properties` into Redis or a config service so they can be tuned without a redeploy
- **Dead-letter handling** — route malformed or repeatedly-failing events to a DLQ topic for inspection
- **Expanded test coverage** — integration tests for each `FraudRule` using embedded Kafka and `spring-boot-starter-data-redis-test`
