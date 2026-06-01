# Real-Time ISA-95 Manufacturing OEE Dashboard
### Project Spec — Dharmesh Patel

> **Stack:** Python · Kafka · PyFlink · TimescaleDB · Iceberg · Grafana · GitHub Actions  
> **Timeline:** 6 weeks  
> **Goal:** Open-source, locally runnable, publicly visible on GitHub — built to showcase in interviews and on LinkedIn

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [ISA-95 Data Model](#3-isa-95-data-model)
4. [Repository Structure](#4-repository-structure)
5. [Component Specs](#5-component-specs)
   - 5.1 [Python Simulator](#51-python-simulator)
   - 5.2 [Kafka Setup](#52-kafka-setup)
   - 5.3 [PyFlink Jobs](#53-pyflink-jobs)
   - 5.4 [Storage Layer](#54-storage-layer)
   - 5.5 [Grafana Dashboards](#55-grafana-dashboards)
   - 5.6 [CI/CD (GitHub Actions)](#56-cicd-github-actions)
6. [Week-by-Week Plan](#6-week-by-week-plan)
7. [OEE Formula & Business Logic](#7-oee-formula--business-logic)
8. [Local Development Setup](#8-local-development-setup)
9. [AWS Deployment Guide](#9-aws-deployment-guide)
10. [Performance Targets & Benchmarks](#10-performance-targets--benchmarks)
11. [Cost Breakdown](#11-cost-breakdown)
12. [Testing Strategy](#12-testing-strategy)
13. [Interview Talking Points](#13-interview-talking-points)
14. [Resume Bullet & LinkedIn Posts](#14-resume-bullet--linkedin-posts)

---

## 1. Project Overview

### What this is

An end-to-end, open-source real-time analytics platform for manufacturing production line data modelled on the ISA-95 standard. It simulates realistic factory floor events — OEE metrics, equipment faults, quality defects — processes them with Apache Flink, and serves live dashboards in Grafana alongside historical trend queries via Athena.

This mirrors the architecture used at industrial companies like Bayer, Siemens, and Bosch for production monitoring — but built open-source, deployable in one command, and designed to be understood from README to `terraform apply`.

### Why this stack

| Choice | Why |
|---|---|
| **Kafka** over Kinesis | Portable, open-source, industry standard — signals you're not AWS-locked |
| **PyFlink** over Glue Streaming | Same stateful streaming model, more portable, Python-native |
| **TimescaleDB** for hot data | PostgreSQL-compatible time-series — SQL familiarity, Grafana native support |
| **Iceberg on S3** for cold data | Schema evolution, time travel, Athena compatible — extends your Protium experience |
| **Grafana** over Tableau | Free, code-provisioned dashboards, real-time capable, industry standard for ops |
| **GitHub Actions** for CI/CD | Automated lint, test, Docker build on every PR — signals production discipline |

### What interviewers see

- You understand ISA-95 (rare outside manufacturing domain)
- You can design a multi-layer streaming architecture (ingest → process → serve → archive)
- You think in production terms: watermarks, exactly-once, checkpointing, cost guardrails
- You ship: live URL, public GitHub, blog post, Loom demo

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  LAYER 1 — SIMULATION                                               │
│                                                                     │
│  Python Simulator                                                   │
│  ├── 3 production lines (LINE-A, LINE-B, LINE-C)                   │
│  ├── 5 machines per line (WU-01 … WU-05)                           │
│  ├── Publishes to 3 Kafka topics (Avro + Schema Registry)          │
│  └── Configurable TPS, fault injection rate, shift schedule        │
└─────────────────────┬───────────────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────────────┐
│  LAYER 2 — KAFKA BROKER                                             │
│                                                                     │
│  Topics:                                                            │
│  ├── line-events          (3 partitions, key = line_id)            │
│  ├── equipment-status     (3 partitions, key = equipment_id)       │
│  └── quality-metrics      (3 partitions, key = line_id)            │
│                                                                     │
│  Schema Registry — Avro schemas, versioned                          │
└──────┬──────────────────────┬──────────────────────────────────────┘
       │                      │
┌──────▼──────┐   ┌───────────▼──────────────────────────────────────┐
│  LAYER 3a   │   │  LAYER 3b — FLINK STREAM PROCESSING              │
│  FLINK      │   │                                                   │
│  S3 Sink    │   │  Job 1: OEE Calculator                           │
│             │   │  ├── Tumbling window 60s per line_id             │
│  Writes     │   │  ├── Computes: Availability, Performance,Quality │
│  Iceberg    │   │  ├── Watermarks for late sensor data (45s)       │
│  tables to  │   │  └── Sink → TimescaleDB oee_metrics              │
│  S3 hourly  │   │                                                   │
│             │   │  Job 2: Equipment Anomaly Detector               │
└──────┬──────┘   │  ├── Keyed stream by equipment_id               │
       │          │  ├── Pattern: RUNNING → FAULT within 5s          │
       │          │  ├── Trigger alert if temp > 85°C OR vib > 120Hz │
       │          │  └── Sink → Postgres alerts table                │
       │          │                                                   │
       │          │  Job 3: Quality SPC Monitor                      │
       │          │  ├── Sliding window 10min / step 1min            │
       │          │  ├── Compute rolling Cpk, defect rate            │
       │          │  └── Sink → TimescaleDB quality_stats            │
       │          └──────────────────────────────────────────────────┘
       │                      │
┌──────▼──────────────────────▼──────────────────────────────────────┐
│  LAYER 4 — STORAGE                                                  │
│                                                                     │
│  TimescaleDB (hot — last 7 days)                                   │
│  ├── oee_metrics        (hypertable, chunk 1hr)                    │
│  ├── quality_stats      (hypertable, chunk 1hr)                    │
│  └── alerts             (regular table, indexed by time+line)      │
│                                                                     │
│  S3 + Iceberg (cold — 90 days)                                     │
│  ├── s3://bucket/iceberg/line_events/                              │
│  ├── s3://bucket/iceberg/equipment_status/                         │
│  └── s3://bucket/iceberg/quality_metrics/                          │
│      Partitioned by date + line_id                                  │
│      Registered in Glue Catalog → queryable via Athena             │
└──────────────────────┬─────────────────────────────────────────────┘
                       │
┌──────────────────────▼─────────────────────────────────────────────┐
│  LAYER 5 — SERVING                                                  │
│                                                                     │
│  Grafana (publicly accessible, read-only anonymous)                 │
│  ├── Dashboard 1: Live OEE per line (TimescaleDB)                  │
│  ├── Dashboard 2: Equipment health heatmap (TimescaleDB)           │
│  ├── Dashboard 3: Quality / SPC control charts (TimescaleDB)       │
│  └── Dashboard 4: 30-day OEE trend (Athena)                        │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. ISA-95 Data Model

### Hierarchy

```
Enterprise (Bayer / Acme Corp)
└── Site (Bangalore Plant)
    └── Area (Assembly)
        └── Work Center (Line-A)
            └── Work Unit (WU-01 … WU-05)  ← events emitted here
```

### Kafka Topic 1: `line-events`

Emitted by each Work Unit every cycle (configurable, default 5s).

```json
{
  "event_id":        "uuid-v4",
  "timestamp":       "2024-03-15T08:32:11.421Z",
  "schema_version":  "1.0",
  "site_id":         "BLR-PLANT-01",
  "area_id":         "ASSEMBLY",
  "line_id":         "LINE-A",
  "work_unit_id":    "WU-02",
  "shift":           "MORNING",
  "event_type":      "CYCLE_COMPLETE",
  "units_produced":  12,
  "units_rejected":  1,
  "ideal_cycle_time_ms": 4800,
  "actual_cycle_time_ms": 5200,
  "planned_production_time_s": 27000
}
```

**`event_type` enum:**
- `CYCLE_COMPLETE` — normal production cycle finished
- `START` — line started / shift started
- `STOP` — planned stop (break, end of shift)
- `FAULT` — unplanned stop / equipment fault
- `CHANGEOVER` — product changeover in progress
- `PLANNED_STOP` — scheduled maintenance window

### Kafka Topic 2: `equipment-status`

Emitted on every state change (event-driven, not periodic).

```json
{
  "event_id":         "uuid-v4",
  "timestamp":        "2024-03-15T08:35:44.100Z",
  "schema_version":   "1.0",
  "equipment_id":     "LINE-A-WU-02",
  "line_id":          "LINE-A",
  "work_unit_id":     "WU-02",
  "status":           "FAULT",
  "previous_status":  "RUNNING",
  "downtime_reason":  "MECHANICAL_FAILURE",
  "planned_downtime": false,
  "temperature_c":    87.4,
  "vibration_hz":     134.2,
  "power_kw":         12.8,
  "fault_code":       "E-042"
}
```

**`status` enum:** `RUNNING` · `IDLE` · `FAULT` · `MAINTENANCE` · `CHANGEOVER` · `STARTUP`

**`downtime_reason` enum:** `MECHANICAL_FAILURE` · `ELECTRICAL_FAULT` · `MATERIAL_SHORTAGE` · `OPERATOR_ABSENCE` · `QUALITY_HOLD` · `PLANNED_MAINTENANCE` · `CHANGEOVER` · `NONE`

### Kafka Topic 3: `quality-metrics`

Emitted at end of each batch inspection (every 50 units produced, configurable).

```json
{
  "event_id":       "uuid-v4",
  "timestamp":      "2024-03-15T08:40:00.000Z",
  "schema_version": "1.0",
  "batch_id":       "BATCH-2024-0315-042",
  "line_id":        "LINE-A",
  "work_unit_id":   "WU-02",
  "shift":          "MORNING",
  "units_inspected": 50,
  "units_passed":    48,
  "units_failed":    2,
  "defect_rate_pct": 4.0,
  "defect_type":     "DIMENSIONAL",
  "yield_pct":       96.0,
  "spec_upper":      10.05,
  "spec_lower":       9.95,
  "measured_mean":   10.01,
  "measured_stddev":  0.018,
  "cpk":              0.93
}
```

**`defect_type` enum:** `DIMENSIONAL` · `SURFACE_FINISH` · `ASSEMBLY_ERROR` · `ELECTRICAL` · `WEIGHT` · `COSMETIC` · `OTHER`

### Avro Schema (example — `line-events`)

Store in `schemas/line_events.avsc`:

```json
{
  "type": "record",
  "name": "LineEvent",
  "namespace": "com.manufacturing.isa95",
  "fields": [
    {"name": "event_id",                  "type": "string"},
    {"name": "timestamp",                 "type": "string"},
    {"name": "schema_version",            "type": "string", "default": "1.0"},
    {"name": "site_id",                   "type": "string"},
    {"name": "area_id",                   "type": "string"},
    {"name": "line_id",                   "type": "string"},
    {"name": "work_unit_id",              "type": "string"},
    {"name": "shift",                     "type": {"type": "enum", "name": "Shift", "symbols": ["MORNING","AFTERNOON","NIGHT"]}},
    {"name": "event_type",                "type": {"type": "enum", "name": "EventType", "symbols": ["CYCLE_COMPLETE","START","STOP","FAULT","CHANGEOVER","PLANNED_STOP"]}},
    {"name": "units_produced",            "type": "int",    "default": 0},
    {"name": "units_rejected",            "type": "int",    "default": 0},
    {"name": "ideal_cycle_time_ms",       "type": "int"},
    {"name": "actual_cycle_time_ms",      "type": "int"},
    {"name": "planned_production_time_s", "type": "int"}
  ]
}
```

---

## 4. Repository Structure

```
isa95-oee-dashboard/
│
├── README.md
├── Makefile                          # make dev / make deploy / make destroy
├── docker-compose.yml               # local stack
├── .env.example
│
├── simulator/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py                      # entrypoint
│   ├── config.py                    # line/machine config, fault rates
│   ├── producers/
│   │   ├── line_event_producer.py
│   │   ├── equipment_status_producer.py
│   │   └── quality_metrics_producer.py
│   └── models/
│       ├── production_line.py       # ISA-95 hierarchy classes
│       ├── work_unit.py
│       └── fault_injector.py        # realistic fault patterns
│
├── schemas/
│   ├── line_events.avsc
│   ├── equipment_status.avsc
│   └── quality_metrics.avsc
│
├── flink/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── jobs/
│   │   ├── oee_calculator.py        # Job 1: OEE tumbling window
│   │   ├── anomaly_detector.py      # Job 2: equipment fault patterns
│   │   └── quality_monitor.py       # Job 3: SPC / Cpk sliding window
│   ├── utils/
│   │   ├── kafka_source.py          # reusable Kafka source factory
│   │   ├── timescale_sink.py        # JDBC sink to TimescaleDB
│   │   └── iceberg_sink.py          # S3 Iceberg sink
│   └── tests/
│       ├── test_oee_calculator.py
│       ├── test_anomaly_detector.py
│       └── fixtures/                # sample events for unit tests
│
├── storage/
│   ├── timescaledb/
│   │   └── init.sql                 # hypertable DDL, indexes, retention policy
│   └── iceberg/
│       └── glue_catalog_setup.py    # Glue Catalog registration script
│
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/
│   │   │   ├── timescaledb.yaml
│   │   │   └── athena.yaml
│   │   └── dashboards/
│   │       └── dashboard.yaml       # auto-load from /dashboards folder
│   └── dashboards/
│       ├── live_oee.json
│       ├── equipment_health.json
│       ├── quality_spc.json
│       └── historical_trend.json
│
└── .github/
    ├── workflows/
    │   ├── ci.yml                   # lint + test + integration test on every PR
    │   ├── publish.yml              # build + push to GHCR on merge to main
    │   └── release.yml              # changelog + GitHub Release on tag
    ├── ISSUE_TEMPLATE/
    │   ├── bug_report.md
    │   └── feature_request.md
    ├── pull_request_template.md
    └── dependabot.yml
```

---

## 5. Component Specs

### 5.1 Python Simulator

**Purpose:** Generate realistic ISA-95 production line events and publish to Kafka.

**Key design decisions:**

- Uses `confluent-kafka` Python client with Avro serialisation via Schema Registry
- Simulator runs 3 `ProductionLine` objects concurrently using `asyncio`
- Each line manages 5 `WorkUnit` state machines independently
- `FaultInjector` introduces realistic failure patterns (see below)

**Fault injection model (make it realistic):**

```python
# config.py — realistic failure patterns
FAULT_CONFIG = {
    "base_fault_rate": 0.02,          # 2% chance per cycle
    "monday_morning_multiplier": 2.5,  # cold start effect
    "shift_changeover_multiplier": 1.8,
    "high_temp_fault_threshold": 85.0, # °C
    "high_vibration_threshold": 120.0, # Hz
    "fault_cluster_probability": 0.4,  # faults cluster — one triggers more
    "mttr_minutes": {                  # mean time to repair by type
        "MECHANICAL_FAILURE": 45,
        "ELECTRICAL_FAULT": 30,
        "MATERIAL_SHORTAGE": 15,
        "QUALITY_HOLD": 60
    }
}

SHIFT_SCHEDULE = {
    "MORNING":   {"start": "06:00", "end": "14:00", "planned_stops": ["10:00"]},
    "AFTERNOON": {"start": "14:00", "end": "22:00", "planned_stops": ["18:00"]},
    "NIGHT":     {"start": "22:00", "end": "06:00", "planned_stops": ["02:00"]}
}

LINE_CONFIG = {
    "LINE-A": {"ideal_cycle_time_ms": 4800, "target_oee": 0.87},
    "LINE-B": {"ideal_cycle_time_ms": 6200, "target_oee": 0.82},
    "LINE-C": {"ideal_cycle_time_ms": 3500, "target_oee": 0.79}
}
```

**Simulator entrypoint (`main.py`) — pseudocode:**

```python
async def run():
    producer = KafkaAvroProducer(schema_registry_url, bootstrap_servers)
    lines = [ProductionLine(cfg) for cfg in LINE_CONFIG.values()]
    await asyncio.gather(*[line.run(producer) for line in lines])

# WorkUnit state machine
class WorkUnit:
    state: Status = RUNNING
    
    async def tick(self, producer):
        if self.fault_injector.should_fault(self.state, self.sensors):
            await self.transition_to(FAULT, producer)
        elif self.state == RUNNING:
            event = self.produce_cycle_event()
            await producer.send("line-events", event)
            await producer.send("equipment-status", self.sensor_reading())
```

**Environment variables:**

```
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SCHEMA_REGISTRY_URL=http://localhost:8081
LINES=LINE-A,LINE-B,LINE-C
MACHINES_PER_LINE=5
EVENTS_PER_SECOND=10
FAULT_INJECTION_ENABLED=true
SIMULATE_SHIFTS=true
```

---

### 5.2 Kafka Setup

**Docker Compose services:**

```yaml
kafka:
  image: confluentinc/cp-kafka:7.5.0
  environment:
    KAFKA_NUM_PARTITIONS: 3
    KAFKA_DEFAULT_REPLICATION_FACTOR: 1
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: false

schema-registry:
  image: confluentinc/cp-schema-registry:7.5.0

kafka-ui:
  image: provectuslabs/kafka-ui:latest  # Kowl replacement, web UI for debugging
  ports: ["8080:8080"]
```

**Topic configuration:**

| Topic | Partitions | Key | Retention | Compaction |
|---|---|---|---|---|
| `line-events` | 3 | `line_id` | 7 days | None |
| `equipment-status` | 3 | `equipment_id` | 7 days | None |
| `quality-metrics` | 3 | `line_id` | 7 days | None |

**Topic creation script (`Makefile`):**

```bash
create-topics:
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
        --create --topic line-events --partitions 3 --replication-factor 1
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
        --create --topic equipment-status --partitions 3 --replication-factor 1
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
        --create --topic quality-metrics --partitions 3 --replication-factor 1
```

---

### 5.3 PyFlink Jobs

#### Job 1: OEE Calculator (`oee_calculator.py`)

**Core logic:**

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.time import Time, Duration

env = StreamExecutionEnvironment.get_execution_environment()
env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
env.get_checkpoint_config().set_checkpointing_interval(30_000)  # 30s

# Watermark strategy: allow 45s late data (sensor reporting delay)
watermark_strategy = (
    WatermarkStrategy
    .for_bounded_out_of_orderness(Duration.of_seconds(45))
    .with_timestamp_assigner(LineEventTimestampAssigner())
)

line_events = (
    env.from_source(kafka_source("line-events"), watermark_strategy, "line-events")
    .key_by(lambda e: e["line_id"])
    .window(TumblingEventTimeWindows.of(Time.seconds(60)))
    .process(OEEWindowFunction())  # computes Availability, Performance, Quality, OEE
    .add_sink(timescale_sink("oee_metrics"))
)

env.execute("OEE Calculator")
```

**`OEEWindowFunction` logic:**

```python
class OEEWindowFunction(ProcessWindowFunction):
    def process(self, line_id, context, events):
        events = list(events)
        
        planned_time_s = 3600  # 1hr window — configurable
        
        # Availability = (Planned time - Downtime) / Planned time
        downtime_s = sum(e["downtime_s"] for e in events if e["event_type"] == "FAULT")
        availability = (planned_time_s - downtime_s) / planned_time_s
        
        # Performance = (Total units × Ideal cycle time) / Operating time
        total_units = sum(e["units_produced"] for e in events)
        ideal_cycle_ms = LINE_CONFIG[line_id]["ideal_cycle_time_ms"]
        operating_time_ms = (planned_time_s - downtime_s) * 1000
        performance = (total_units * ideal_cycle_ms) / operating_time_ms if operating_time_ms > 0 else 0
        
        # Quality = Good units / Total units
        good_units = sum(e["units_produced"] - e["units_rejected"] for e in events)
        quality = good_units / total_units if total_units > 0 else 0
        
        oee = availability * performance * quality
        
        yield {
            "time": context.window().end,
            "line_id": line_id,
            "availability": round(availability, 4),
            "performance": round(performance, 4),
            "quality": round(quality, 4),
            "oee": round(oee, 4),
            "total_units": total_units,
            "good_units": good_units
        }
```

#### Job 2: Equipment Anomaly Detector (`anomaly_detector.py`)

**Pattern detection using Flink CEP:**

```python
from pyflink.cep import Pattern, CEP

# Detect: temperature spike followed by FAULT within 30 seconds
fault_pattern = (
    Pattern.begin("high_temp")
        .where(lambda e: e["temperature_c"] > 85.0)
    .next("fault")
        .where(lambda e: e["status"] == "FAULT")
        .within(Time.seconds(30))
)

alert_stream = (
    equipment_stream
    .key_by(lambda e: e["equipment_id"])
    .apply(CEP.pattern(fault_pattern))
    .select(lambda pattern: {
        "timestamp": pattern["fault"]["timestamp"],
        "equipment_id": pattern["fault"]["equipment_id"],
        "line_id": pattern["fault"]["line_id"],
        "alert_type": "THERMAL_FAULT_PRECURSOR",
        "temperature_c": pattern["high_temp"]["temperature_c"],
        "fault_code": pattern["fault"]["fault_code"],
        "severity": "HIGH"
    })
)

alert_stream.add_sink(postgres_sink("alerts"))
```

#### Job 3: Quality SPC Monitor (`quality_monitor.py`)

```python
# Sliding window: 10min window, 1min slide — rolling Cpk
quality_stream = (
    env.from_source(kafka_source("quality-metrics"), ...)
    .key_by(lambda e: e["line_id"])
    .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
    .process(CpkWindowFunction())
    .add_sink(timescale_sink("quality_stats"))
)

class CpkWindowFunction(ProcessWindowFunction):
    def process(self, line_id, context, events):
        events = list(events)
        means = [e["measured_mean"] for e in events]
        stddevs = [e["measured_stddev"] for e in events]
        
        rolling_mean = statistics.mean(means)
        rolling_std = statistics.mean(stddevs)  # pooled
        
        spec_upper = events[-1]["spec_upper"]
        spec_lower = events[-1]["spec_lower"]
        
        cpk = min(
            (spec_upper - rolling_mean) / (3 * rolling_std),
            (rolling_mean - spec_lower) / (3 * rolling_std)
        ) if rolling_std > 0 else 0
        
        yield {
            "time": context.window().end,
            "line_id": line_id,
            "rolling_cpk": round(cpk, 3),
            "rolling_mean": round(rolling_mean, 4),
            "rolling_stddev": round(rolling_std, 4),
            "window_batches": len(events)
        }
```

**Flink checkpointing config (all jobs):**

```python
env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
env.get_checkpoint_config().set_checkpointing_interval(30_000)       # 30s
env.get_checkpoint_config().set_checkpoint_timeout(60_000)           # 60s timeout
env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
env.get_checkpoint_config().set_min_pause_between_checkpoints(10_000) # 10s
env.get_restart_strategy().set_restart_strategy(
    RestartStrategies.fixed_delay_restart(3, 10_000)  # 3 retries, 10s delay
)
```

---

### 5.4 Storage Layer

#### TimescaleDB DDL (`storage/timescaledb/init.sql`)

```sql
-- OEE metrics hypertable
CREATE TABLE oee_metrics (
    time            TIMESTAMPTZ NOT NULL,
    line_id         TEXT NOT NULL,
    availability    NUMERIC(5,4),
    performance     NUMERIC(5,4),
    quality         NUMERIC(5,4),
    oee             NUMERIC(5,4),
    total_units     INTEGER,
    good_units      INTEGER
);
SELECT create_hypertable('oee_metrics', 'time', chunk_time_interval => INTERVAL '1 hour');
CREATE INDEX ON oee_metrics (line_id, time DESC);

-- Quality stats hypertable
CREATE TABLE quality_stats (
    time            TIMESTAMPTZ NOT NULL,
    line_id         TEXT NOT NULL,
    rolling_cpk     NUMERIC(6,3),
    rolling_mean    NUMERIC(10,4),
    rolling_stddev  NUMERIC(10,4),
    window_batches  INTEGER
);
SELECT create_hypertable('quality_stats', 'time', chunk_time_interval => INTERVAL '1 hour');

-- Alerts table
CREATE TABLE alerts (
    id              SERIAL PRIMARY KEY,
    timestamp       TIMESTAMPTZ NOT NULL,
    equipment_id    TEXT NOT NULL,
    line_id         TEXT NOT NULL,
    alert_type      TEXT NOT NULL,
    severity        TEXT NOT NULL,
    temperature_c   NUMERIC(6,2),
    fault_code      TEXT,
    acknowledged    BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX ON alerts (line_id, timestamp DESC);
CREATE INDEX ON alerts (acknowledged, severity);

-- Retention policy: auto-drop data older than 7 days from hot storage
SELECT add_retention_policy('oee_metrics', INTERVAL '7 days');
SELECT add_retention_policy('quality_stats', INTERVAL '7 days');

-- Continuous aggregate: pre-compute hourly OEE (speeds up Grafana queries)
CREATE MATERIALIZED VIEW oee_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    line_id,
    AVG(oee)          AS avg_oee,
    MIN(oee)          AS min_oee,
    MAX(oee)          AS max_oee,
    AVG(availability) AS avg_availability,
    AVG(performance)  AS avg_performance,
    AVG(quality)      AS avg_quality
FROM oee_metrics
GROUP BY bucket, line_id;

SELECT add_continuous_aggregate_policy('oee_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset   => INTERVAL '1 minute',
    schedule_interval => INTERVAL '30 minutes'
);
```

#### Iceberg Table Design

```python
# Iceberg schema for line_events
from pyiceberg.schema import Schema
from pyiceberg.types import *

line_events_schema = Schema(
    NestedField(1,  "event_id",               StringType(), required=True),
    NestedField(2,  "timestamp",              TimestamptzType(), required=True),
    NestedField(3,  "site_id",                StringType()),
    NestedField(4,  "line_id",                StringType(), required=True),
    NestedField(5,  "work_unit_id",           StringType()),
    NestedField(6,  "event_type",             StringType()),
    NestedField(7,  "units_produced",         IntegerType()),
    NestedField(8,  "units_rejected",         IntegerType()),
    NestedField(9,  "actual_cycle_time_ms",   IntegerType()),
    NestedField(10, "date",                   DateType(), required=True),  # partition col
)

# Partition spec: date + line_id — enables efficient pruning
partition_spec = PartitionSpec(
    PartitionField(source_id=2,  field_id=1000, transform=DayTransform(), name="date"),
    PartitionField(source_id=4,  field_id=1001, transform=IdentityTransform(), name="line_id"),
)
```

**Time travel demo query (for LinkedIn / README):**

```sql
-- Current OEE
SELECT line_id, AVG(oee) as avg_oee
FROM line_events
WHERE date = CURRENT_DATE
GROUP BY line_id;

-- Time travel: OEE on a specific past date
SELECT line_id, AVG(oee) as avg_oee
FROM line_events FOR SYSTEM_TIME AS OF TIMESTAMP '2024-03-01 00:00:00'
WHERE date = DATE '2024-03-01'
GROUP BY line_id;

-- Schema evolution: add a column (no rewrite needed)
ALTER TABLE line_events ADD COLUMN operator_id STRING;
```

---

### 5.5 Grafana Dashboards

All dashboards are provisioned as code — no click-ops.

#### Dashboard 1: Live OEE (`live_oee.json`)

| Panel | Type | Query | Thresholds |
|---|---|---|---|
| OEE per line (now) | Gauge | `SELECT last(oee,time) FROM oee_metrics GROUP BY line_id` | <60% red, 60–85% amber, >85% green |
| OEE trend (5 min) | Time series | `SELECT time, oee FROM oee_metrics WHERE time > NOW()-5m` | — |
| Shift comparison | Bar chart | OEE by shift, last 3 shifts | — |
| Availability heatmap | Heatmap | Line × hour, availability as colour | — |
| Alert count | Stat | `SELECT COUNT(*) FROM alerts WHERE acknowledged=false` | >0 orange |

#### Dashboard 2: Equipment Health (`equipment_health.json`)

| Panel | Type | Description |
|---|---|---|
| Status grid | State timeline | Each row = 1 machine, colour by status |
| Downtime reason | Pie chart | Last 24h, breakdown by `downtime_reason` |
| MTBF by line | Bar chart | Mean time between failures — computed from alerts table |
| MTTR by line | Bar chart | Mean time to repair |
| Fault frequency | Time series | Faults per hour, last 7 days |
| Active alerts | Table | Unacknowledged alerts, sortable by severity + time |

#### Dashboard 3: Quality / SPC (`quality_spc.json`)

| Panel | Type | Description |
|---|---|---|
| Cpk trend | Time series | Rolling Cpk per line — add control limit lines at 1.0 and 1.33 |
| Defect rate | Time series | `defect_rate_pct` with UCL/LCL annotations |
| Yield by line | Gauge | Current shift yield % |
| Control chart | Time series | `measured_mean` with spec_upper / spec_lower reference lines |
| Defect type breakdown | Pie chart | Last 24h |

#### Dashboard 4: 30-Day Trend via Athena (`historical_trend.json`)

```sql
-- Athena query for 30-day daily OEE
SELECT
    DATE(from_iso8601_timestamp(timestamp)) AS day,
    line_id,
    AVG(CAST(units_produced AS DOUBLE) - CAST(units_rejected AS DOUBLE))
        / NULLIF(AVG(CAST(units_produced AS DOUBLE)), 0) AS avg_quality
FROM "glue_catalog"."manufacturing"."line_events"
WHERE date >= DATE_ADD('day', -30, CURRENT_DATE)
GROUP BY 1, 2
ORDER BY 1, 2
```

#### Grafana Alerting Rule

```yaml
# grafana/provisioning/alerting/oee_alert.yaml
groups:
  - name: oee_alerts
    rules:
      - alert: OEECriticallyLow
        expr: |
          SELECT avg(oee) FROM oee_metrics
          WHERE time > NOW() - INTERVAL '2 minutes'
          GROUP BY line_id
          HAVING avg(oee) < 0.60
        for: 2m
        annotations:
          summary: "OEE below 60% on {{ $labels.line_id }}"
        contact_points:
          - slack-webhook
```

---

### 5.6 CI/CD (GitHub Actions)

#### Makefile

```makefile
# Local development
dev:
    docker-compose up -d
    sleep 10
    make create-topics
    make register-schemas
    @echo "Stack ready. Grafana: http://localhost:3000 | Kafka UI: http://localhost:8080"

create-topics:
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
        --create --topic line-events --partitions 3 --replication-factor 1
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
        --create --topic equipment-status --partitions 3 --replication-factor 1
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
        --create --topic quality-metrics --partitions 3 --replication-factor 1

register-schemas:
    python scripts/register_schemas.py

run-simulator:
    docker-compose up -d simulator

run-flink:
    docker-compose up -d flink-oee flink-anomaly flink-quality

# CI
test:
    pytest flink/tests/ -v
    pytest simulator/tests/ -v

lint:
    ruff check .
    black --check .
```

#### `.github/workflows/ci.yml`

```yaml
name: CI
on: [pull_request]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install ruff black
      - run: ruff check .
      - run: black --check .

  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install -r flink/requirements.txt -r simulator/requirements.txt
      - run: pytest flink/tests/ simulator/tests/ -v --tb=short --cov=. --cov-report=xml
      - uses: codecov/codecov-action@v4
        with: { file: ./coverage.xml }

  docker-build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - run: docker build -t simulator ./simulator
      - run: docker build -t flink ./flink

  integration-test:
    runs-on: ubuntu-latest
    needs: [lint, test]
    steps:
      - uses: actions/checkout@v4
      - name: Start stack
        run: |
          docker-compose up -d
          sleep 30
          make create-topics
      - name: Run integration tests
        run: pytest tests/integration/ -v --tb=short
      - name: Tear down
        if: always()
        run: docker-compose down -v
```

#### `.github/workflows/publish.yml` — publish Docker images to GHCR on merge to main

```yaml
name: Publish
on:
  push:
    branches: [main]

env:
  REGISTRY: ghcr.io
  IMAGE_PREFIX: ${{ github.repository_owner }}/isa95-oee

jobs:
  publish:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write
    steps:
      - uses: actions/checkout@v4

      - name: Log in to GHCR
        uses: docker/login-action@v3
        with:
          registry: ${{ env.REGISTRY }}
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - name: Build and push simulator
        uses: docker/build-push-action@v5
        with:
          context: ./simulator
          push: true
          tags: |
            ${{ env.REGISTRY }}/${{ env.IMAGE_PREFIX }}/simulator:latest
            ${{ env.REGISTRY }}/${{ env.IMAGE_PREFIX }}/simulator:${{ github.sha }}

      - name: Build and push flink
        uses: docker/build-push-action@v5
        with:
          context: ./flink
          push: true
          tags: |
            ${{ env.REGISTRY }}/${{ env.IMAGE_PREFIX }}/flink:latest
            ${{ env.REGISTRY }}/${{ env.IMAGE_PREFIX }}/flink:${{ github.sha }}
```

#### GitHub repo polish checklist

| Item | File | Purpose |
|---|---|---|
| CI badge | README.md | `![CI](https://github.com/username/repo/actions/workflows/ci.yml/badge.svg)` |
| Coverage badge | README.md | Codecov badge after first run |
| License badge | README.md | `![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)` |
| PR template | `.github/pull_request_template.md` | Checklist: tests added, `make lint` passes |
| Issue templates | `.github/ISSUE_TEMPLATE/` | `bug_report.md`, `feature_request.md` |
| Dependabot | `.github/dependabot.yml` | Weekly Python + Docker dependency updates |
| Release workflow | `.github/workflows/release.yml` | Tag-triggered — creates GitHub Release with changelog |

#### `.github/dependabot.yml`

```yaml
version: 2
updates:
  - package-ecosystem: pip
    directory: /simulator
    schedule: { interval: weekly }
  - package-ecosystem: pip
    directory: /flink
    schedule: { interval: weekly }
  - package-ecosystem: docker
    directory: /
    schedule: { interval: weekly }
```

---

## 6. Week-by-Week Plan

### Week 1 — Local stack + simulator
**Goal:** Data flowing end-to-end locally in Docker Compose.

| Day | Task |
|---|---|
| 1 | Set up repo, Docker Compose with Kafka, Zookeeper, Schema Registry, Kafka UI, TimescaleDB, Grafana |
| 2 | Write Avro schemas, register with Schema Registry, verify with `curl` |
| 3 | Build `ProductionLine` and `WorkUnit` state machine classes |
| 4 | Build `FaultInjector` with realistic patterns (Monday effect, clustering, MTTR) |
| 5 | Wire all 3 Kafka producers, verify with Kafka UI — confirm all 3 topics receiving data |
| 6–7 | Init SQL for TimescaleDB, test data flowing Kafka → verify schema in Registry |

**Done when:** `make dev && make run-simulator` shows 3 topics with data in Kafka UI.

---

### Week 2 — PyFlink OEE job
**Goal:** Real-time OEE computed per line every 60 seconds, writing to TimescaleDB.

| Day | Task |
|---|---|
| 1 | Set up PyFlink in Docker, connect to Kafka source, verify reading from `line-events` |
| 2 | Implement `OEEWindowFunction` with tumbling 60s window |
| 3 | Add watermark strategy (45s late tolerance), test with delayed events |
| 4 | Wire JDBC sink to TimescaleDB, verify rows in `oee_metrics` table |
| 5 | Build equipment anomaly detector (Job 2) |
| 6 | Build quality SPC monitor (Job 3) |
| 7 | Unit tests for all 3 jobs using `MiniClusterWithClientResource` |

**Done when:** `SELECT * FROM oee_metrics ORDER BY time DESC LIMIT 10;` returns rows.

---

### Week 3 — Grafana dashboards
**Goal:** 3 production-grade dashboards, provisioned as code.

| Day | Task |
|---|---|
| 1 | Configure TimescaleDB datasource in Grafana as YAML (not UI) |
| 2 | Build Dashboard 1: Live OEE — gauges, trend sparklines, shift comparison |
| 3 | Build Dashboard 2: Equipment health — status timeline, downtime breakdown, MTBF/MTTR |
| 4 | Build Dashboard 3: Quality SPC — Cpk trend, control chart, defect breakdown |
| 5 | Set up Grafana alerting: OEE < 60% → Slack webhook |
| 6 | Export all dashboards to JSON, add to `grafana/dashboards/`, verify auto-provisioning |
| 7 | Screenshot all dashboards, record Loom teaser (15 seconds) — post to LinkedIn as week 3 update |

**Done when:** `docker-compose down && docker-compose up -d` — all dashboards auto-load with no clicks.

---

### Week 4 — Historical layer (Iceberg on S3)
**Goal:** 90-day Iceberg archive, time-travel queries, Athena connected.

| Day | Task |
|---|---|
| 1 | Set up local MinIO as S3 substitute (for local testing before AWS) |
| 2 | Build Flink S3/Iceberg sink (Job 4) — hourly micro-batches from all 3 topics |
| 3 | Test Iceberg writes — verify partition structure in MinIO |
| 4 | Register Glue Catalog (or local Hive metastore for local dev), query with Trino locally |
| 5 | Add Athena datasource to Grafana, build Dashboard 4: 30-day OEE trend |
| 6 | Demo time-travel query, screenshot — this is your LinkedIn screenshot moment |
| 7 | Tune Iceberg: compaction strategy, partition pruning test, schema evolution demo |

**Done when:** `SELECT * FROM manufacturing.line_events FOR SYSTEM_TIME AS OF TIMESTAMP '...'` works in Athena.

---

### Week 5 — GitHub CI/CD + repo polish
**Goal:** CI green on every PR, Docker images on GHCR, repo interview-ready.

| Day | Task |
|---|---|
| 1 | Set up `ci.yml` — lint + unit tests + docker build on every PR |
| 2 | Add integration test job to CI — spins up docker-compose, runs pipeline end-to-end, tears down |
| 3 | Set up `publish.yml` — build and push simulator + flink images to GHCR on merge to main |
| 4 | Add coverage reporting (pytest-cov → Codecov), wire badge into README |
| 5 | Add PR template, issue templates (bug/feature), dependabot config |
| 6 | Add release workflow — tag-triggered, auto-generates changelog from commit messages |
| 7 | Final README pass: architecture Mermaid diagram, badges, `make dev` one-liner, screenshots |

**Done when:** Every PR runs CI green. `main` branch triggers Docker image publish to GHCR. README looks polished on GitHub.

---

### Week 6 — Polish, blog, ship
**Goal:** Visible to recruiters. GitHub stars, LinkedIn engagement.

| Day | Task |
|---|---|
| 1 | Write README: architecture Mermaid diagram, prerequisites, `make deploy` one-liner, cost breakdown |
| 2 | Add GitHub badges: CI status, license, last commit |
| 3 | Record 3-minute Loom demo: simulator → Kafka → Flink → live Grafana panel updating |
| 4 | Write Medium/Substack post (see outline below) |
| 5 | Post LinkedIn "shipped" post (template in section 14) |
| 6 | Post to r/dataengineering, Data Engineering Discord, Hacker News (Show HN) |
| 7 | Add GitHub topics, respond to comments, engage with community |

**Blog post outline:**
1. Why ISA-95 matters and what OEE actually means
2. The architecture — why Kafka + Flink over managed services for an open-source project
3. The hardest problem: watermarks and late sensor data
4. The Iceberg layer — time travel for manufacturing data
5. What I'd do differently (always include this — shows reflection)
6. Link to repo + live dashboard

---

## 7. OEE Formula & Business Logic

### OEE Calculation

```
OEE = Availability × Performance × Quality

Availability = (Planned Production Time − Unplanned Downtime) / Planned Production Time

Performance  = (Total Units Produced × Ideal Cycle Time) / Operating Time

Quality      = Good Units / Total Units Produced
```

### OEE Benchmarks (World Class Manufacturing)

| OEE Score | Classification | Meaning |
|---|---|---|
| > 85% | World Class | Top 25% globally |
| 60–85% | Typical | Room for improvement |
| 40–60% | Low | Significant losses |
| < 40% | Poor | Major systemic issues |

### Six Big Losses (mapped to your data model)

| Loss Category | ISA-95 Event | Affects |
|---|---|---|
| Equipment failures | `FAULT` events | Availability |
| Setup and adjustments | `CHANGEOVER` events | Availability |
| Idling and minor stops | `IDLE` status > 5 min | Performance |
| Reduced speed | `actual_cycle_time > ideal_cycle_time` | Performance |
| Process defects | `units_rejected > 0` | Quality |
| Reduced yield on startup | Events in first 15 min of shift | Quality |

### Shift Timing Assumptions

```
Planned production time = shift duration (8h) − planned breaks (30 min) = 450 min
OEE window = 60s tumbling (real-time) / 1hr aggregated (historical)
Watermark tolerance = 45s (covers OPC-UA / sensor polling delay)
```

---

## 8. Local Development Setup

### Prerequisites

- Docker + Docker Compose
- Python 3.11+
- AWS CLI (for week 5 onwards)
- Terraform 1.6+ (for week 5 onwards)

### Quick start

```bash
git clone https://github.com/yourusername/isa95-oee-dashboard
cd isa95-oee-dashboard

cp .env.example .env

make dev          # starts all Docker services
make create-topics
make register-schemas
make run-simulator
make run-flink

# Open:
# Grafana:   http://localhost:3000  (admin / admin)
# Kafka UI:  http://localhost:8080
# Flink UI:  http://localhost:8081
```

### `.env.example`

```env
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SCHEMA_REGISTRY_URL=http://localhost:8081

# TimescaleDB
TIMESCALE_HOST=localhost
TIMESCALE_PORT=5432
TIMESCALE_DB=manufacturing
TIMESCALE_USER=postgres
TIMESCALE_PASSWORD=changeme

# Simulator
LINES=LINE-A,LINE-B,LINE-C
MACHINES_PER_LINE=5
EVENTS_PER_SECOND=10
FAULT_INJECTION_ENABLED=true

# AWS (week 5+)
AWS_REGION=ap-south-1
S3_BUCKET=manufacturing-iceberg-dev
GLUE_CATALOG=manufacturing

# Grafana
GF_AUTH_ANONYMOUS_ENABLED=true
GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer
```

---

## 9. Deployment Guide

### Run anywhere with Docker Compose

All services are containerised. Anyone cloning the repo can have the full stack running in one command — no cloud account needed.

```bash
git clone https://github.com/yourusername/isa95-oee-dashboard
cd isa95-oee-dashboard
cp .env.example .env
make dev
```

### Pull pre-built images from GHCR

After CI publishes on merge to main:

```bash
docker pull ghcr.io/yourusername/isa95-oee/simulator:latest
docker pull ghcr.io/yourusername/isa95-oee/flink:latest
```

Update `docker-compose.yml` to reference GHCR images instead of local builds for a no-build quickstart.

### Optional: deploy to any Linux VPS

The entire stack fits on a $6/month VPS (DigitalOcean Droplet, Hetzner CX11):

```bash
# On the VPS
docker compose pull
docker compose up -d
# Point a subdomain at the VPS IP — Grafana runs on :3000
```

Set `GF_AUTH_ANONYMOUS_ENABLED=true` in `.env` for public read-only Grafana access.

### Deployment checklist

- [ ] GitHub repo is public
- [ ] README has CI badge, coverage badge, architecture diagram
- [ ] GHCR packages are public (Settings → Packages → Change visibility)
- [ ] `make dev` tested on a clean machine (or GitHub Codespaces)
- [ ] Grafana screenshots in README showing live dashboards

---

## 10. Performance Targets & Benchmarks

Run these benchmarks and include results in your README — this is what goes in your resume bullet.

| Metric | Target | How to measure |
|---|---|---|
| End-to-end latency | < 2 seconds | Timestamp event at simulator, measure when Grafana panel refreshes |
| Kafka throughput | 10,000+ events/min | Kafka UI consumer lag + producer rate |
| Flink OEE window compute | < 5s per 60s window | Flink UI job metrics |
| TimescaleDB query time | < 100ms | `EXPLAIN ANALYZE` on Grafana queries |
| Athena 30-day query | < 30s | Athena query history |
| Iceberg partition pruning | > 90% data skipped | Athena `bytesScanned` before/after |
| Flink checkpoint duration | < 10s | Flink UI checkpoint history |
| Grafana dashboard load | < 3s | Browser DevTools |

### Benchmark script

```python
# scripts/benchmark.py
import time
import json
from confluent_kafka import Producer, Consumer

def measure_e2e_latency(iterations=100):
    """Measure time from event publish to TimescaleDB insert"""
    latencies = []
    for _ in range(iterations):
        t0 = time.time()
        publish_event({"timestamp": t0, "line_id": "LINE-A", ...})
        wait_for_timescale_insert(t0)
        latencies.append(time.time() - t0)
    
    print(f"P50: {sorted(latencies)[50]:.2f}s")
    print(f"P95: {sorted(latencies)[95]:.2f}s")
    print(f"P99: {sorted(latencies)[99]:.2f}s")
```

---

## 11. Cost Breakdown

### Local development (weeks 1–5): $0
Everything runs in Docker Compose on your laptop.

### Optional VPS deployment (week 6): ~$6–12/month
Hetzner CX11 (~€4/mo) or DigitalOcean Basic Droplet ($6/mo) runs the full stack.
Keep running during active job search (2–4 weeks), then stop instance.

### GitHub Actions CI: $0
Free tier covers all usage for a public repo (2000 min/month).

---

## 12. Testing Strategy

### Unit tests (Flink jobs)

```python
# flink/tests/test_oee_calculator.py
from pyflink.testing.test_case_utils import MiniClusterWithClientResource

class TestOEECalculator(unittest.TestCase):
    
    def setUp(self):
        self.cluster = MiniClusterWithClientResource(
            MiniClusterResourceConfiguration.new_builder()
                .set_number_of_task_managers(1)
                .set_number_of_slots_per_task_manager(2)
                .build()
        )
    
    def test_oee_calculation_perfect_line(self):
        """100% OEE: no faults, no rejects, ideal cycle time"""
        events = load_fixture("perfect_line_60s.json")
        result = run_oee_job(events)
        self.assertAlmostEqual(result["oee"], 1.0, places=2)
    
    def test_oee_calculation_with_fault(self):
        """OEE drops correctly with a 10-minute fault in 60-min window"""
        events = load_fixture("line_with_10min_fault.json")
        result = run_oee_job(events)
        # Availability = (60-10)/60 = 0.833
        self.assertAlmostEqual(result["availability"], 0.833, places=2)
    
    def test_late_arriving_events_handled(self):
        """Events arriving 44s late should be included; 46s late should not"""
        ...
    
    def test_watermark_excludes_very_late_events(self):
        """Events > 45s late should not corrupt the OEE window"""
        ...
```

### Integration tests

```python
# tests/integration/test_pipeline.py
# Runs against local Docker Compose stack

def test_event_flows_to_timescaledb():
    """Publish 1 event, verify it appears in TimescaleDB within 5 seconds"""
    publish_test_event(line_id="LINE-A", units_produced=10, units_rejected=0)
    time.sleep(5)
    row = query_timescale("SELECT * FROM oee_metrics WHERE line_id='LINE-A' ORDER BY time DESC LIMIT 1")
    assert row is not None
    assert row["oee"] > 0
```

### Load test

```bash
# Use k6 or locust to ramp up simulator TPS and find breaking point
# Target: stable at 10K events/min, measure Flink lag
python simulator/main.py --tps=200 --duration=300
# Monitor: Kafka consumer group lag should stay < 1000 messages
```

---

## 13. Interview Talking Points

This project is designed to unlock specific technical conversations. Study each one.

### 1. Watermarks and late data (your strongest differentiator)

> "In real manufacturing, sensors don't always report in real time. An OPC-UA sensor polling every 5 seconds can arrive at Kafka 30–45 seconds late depending on network conditions and gateway queuing. If I compute OEE over a 60-second tumbling window without watermarks, a machine that was actually RUNNING during that window looks like it was STOPPED — which artificially deflates availability and OEE. I set a 45-second watermark tolerance, which means Flink holds the window open for 45 extra seconds before closing it. Events arriving within that window are included; anything later is dropped. The key interview point is the trade-off: a larger watermark = more accurate OEE but more latency. I chose 45s based on what I saw with real sensor data."

### 2. Why Iceberg over Hudi or Delta Lake

> "All three solve the same problem — ACID transactions on object storage. I chose Iceberg because it has first-class Athena support (Hudi's Athena support is read-only and limited), schema evolution is cleaner (you can rename and reorder columns, not just add), and the metadata layer is pure JSON files — easy to inspect and debug. Hudi is better if you need upserts on a row-level key (like CDC), which I don't need here. Delta Lake is excellent but more tightly coupled to Spark/Databricks."

### 3. Flink vs Spark Structured Streaming

> "The key difference is processing model. Spark processes in micro-batches — it accumulates events for N milliseconds, then processes them as a batch. Flink is truly record-at-a-time — each event triggers computation immediately. For OEE dashboards where operators need to see a fault reflected in under 2 seconds, Flink's sub-second latency matters. Spark Structured Streaming has gotten much faster, but its minimum latency is still the trigger interval, not the event itself. Also, Flink's CEP library makes the anomaly detection pattern (high temp → fault within 30s) much cleaner than rolling window hacks in Spark."

### 4. ISA-95 hierarchy and Kafka topic design

> "ISA-95 defines a strict hierarchy: Enterprise → Site → Area → Work Center → Work Unit. The key design decision was what level to partition Kafka by. I chose `line_id` as the partition key (Work Center level) rather than `work_unit_id` because OEE is computed per line, not per machine — so all events from the same line need to go to the same Flink task for window computation. If I had partitioned by machine, I'd need to re-key the stream before windowing, adding a shuffle. This keeps the processing topology clean."

### 5. TimescaleDB vs InfluxDB

> "InfluxDB 2.x has a great ingestion API and is purpose-built for time-series, but it uses its own Flux query language which means my Grafana team needs to learn something new. TimescaleDB is PostgreSQL under the hood — same SQL, same ecosystem, works natively with Grafana's Postgres datasource. For a team that already knows SQL, the operational familiarity wins. InfluxDB would make sense if we needed its clustering model for extreme cardinality (millions of unique metric series), which manufacturing OEE doesn't have."

### 6. OEE as a business metric

> "OEE is the single most important metric in manufacturing operations. World-class is 85% — that's the top quartile globally. Most plants run 40–60% and don't know it because the data is trapped in siloed SCADA systems that don't talk to each other. The whole point of this architecture is to bring that data out in real time so an operations manager can see on a phone that Line B just dropped to 52% OEE and why — without waiting for the end-of-shift Excel report."

### 7. Exactly-once semantics

> "Flink's exactly-once guarantee requires two things: Kafka transactions (so the consumer doesn't double-count retried records) and an idempotent sink. For TimescaleDB, I use an UPSERT on `(time, line_id)` — so if Flink replays a checkpoint and re-sends the same OEE record, the DB just overwrites it with identical data. For the Iceberg sink, Iceberg's own transactional metadata handles exactly-once at the table level."

---

## 14. Resume Bullet & LinkedIn Posts

### Resume bullet

```
Built open-source real-time manufacturing OEE analytics platform — Python simulator 
generating ISA-95-compliant events → Kafka (3 topics, Avro + Schema Registry) → 
PyFlink (OEE tumbling windows, CEP anomaly detection, SPC quality monitoring) → 
TimescaleDB + Iceberg on S3 — processing 10K+ events/min across 3 simulated 
production lines with <2s end-to-end latency. Grafana dashboards provisioned as code 
with Slack alerting; GitHub Actions CI/CD pipeline with integration tests and 
automated Docker image publishing to GHCR.
```

### LinkedIn post — Week 1 (building in public)

```
Building in public 🔧

I work with ISA-95 manufacturing data at Bayer every day — production line OEE, 
equipment status, real-time quality metrics.

But all of it lives behind a corporate firewall.

So I'm building an open-source version anyone can run:

→ Python simulator generating realistic factory line data (fault injection, 
  shift schedules, the Monday morning cold-start effect)
→ Kafka with 3 topics following ISA-95 hierarchy + Avro schemas
→ PyFlink computing OEE in real-time with tumbling windows + watermarks
→ Grafana dashboards — live OEE, equipment heatmaps, SPC control charts
→ Iceberg on S3 for historical queries with time travel
→ GitHub Actions CI/CD — Docker images published to GHCR, integration tests on every PR

Week 1 update: Docker Compose stack running. Simulator publishing 10K+ events/min 
across 3 production lines.

If you work in manufacturing data, industrial IoT, or just want to learn about OEE 
and ISA-95 — follow along. Weekly updates.

GitHub: [link]

#DataEngineering #Kafka #ApacheFlink #Manufacturing #IoT #ISA95 #OpenSource
```

### LinkedIn post — Week 6 (shipped)

```
Shipped: real-time OEE dashboard for manufacturing 🏭

6 weeks ago I started building an open-source ISA-95 manufacturing analytics platform. 
It's done.

What it does:
• Simulates 3 production lines, 15 machines, realistic ISA-95 events
• Kafka ingestion (Avro + Schema Registry) → PyFlink OEE computation (<2s latency)
• 3 live Grafana dashboards: OEE gauges, equipment heatmaps, SPC control charts
• Iceberg on S3 for 90-day historical analysis + time-travel queries via Athena
• One-command local deploy with Docker Compose + GitHub Actions CI/CD (images on GHCR)

The hardest part wasn't the tech — it was modelling realistic fault injection.

Real factory data has patterns:
- Monday morning startups are messy (cold equipment, shift handover delays)
- Changeovers cause cascading latency
- Quality defects cluster by shift and operator

The most interesting Flink challenge: watermarks for late-arriving sensor data.

A machine reporting 45 seconds late due to OPC-UA gateway queuing shouldn't make 
your OEE look artificially low. Flink's bounded-out-of-orderness watermark handles 
this — hold the window open 45s, then close it. Events arriving later are dropped.

This is the trade-off between accuracy and latency that most streaming courses don't 
teach you.

Live dashboard: [link]
GitHub (⭐ if useful): [link]  
Architecture deep-dive: [blog link]

What's next: adding an LLM layer that explains OEE drops in plain English.

#DataEngineering #Kafka #ApacheFlink #Grafana #Manufacturing #ISA95 #AWS #OpenSource
```

---

*Spec version 1.0 — Dharmesh Patel — May 2026*
