"""
Iceberg Sink — reads all 3 Kafka topics, writes to Iceberg via Nessie/MinIO.
Micro-batch: accumulate 60s of messages, write one Iceberg append per topic.

Production mapping:
  - Consumer groups → Flink KafkaSource with CheckpointedOffsetCommitter
  - Batch writes      → Flink IcebergSink with upsert=false, format=PARQUET
  - Catalog           → NessieCatalog with branchname=main
"""

import json
import logging
import os
import signal
import time
from collections import defaultdict
from datetime import datetime, timezone

import pyarrow as pa
from confluent_kafka import KafkaError
from pyiceberg.catalog.rest import RestCatalog

from flink.utils.kafka_source import make_consumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] iceberg_sink — %(message)s",
)
log = logging.getLogger(__name__)

BATCH_INTERVAL_S = 60
NESSIE_URI = os.environ.get("NESSIE_URI", "http://nessie:19120/iceberg")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
# Warehouse is the name registered server-side in Nessie, not the S3 path
WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE_NAME", "main")

_running = True


def handle_signal(sig, frame):
    global _running
    _running = False


# ── PyArrow schemas ──────────────────────────────────────────────────────────

LINE_EVENTS_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("timestamp", pa.string(), nullable=False),
        pa.field("ingested_at", pa.string(), nullable=False),
        pa.field("schema_version", pa.string()),
        pa.field("site_id", pa.string()),
        pa.field("area_id", pa.string()),
        pa.field("line_id", pa.string()),
        pa.field("work_unit_id", pa.string()),
        pa.field("shift", pa.string()),
        pa.field("event_type", pa.string()),
        pa.field("units_produced", pa.int32()),
        pa.field("units_rejected", pa.int32()),
        pa.field("ideal_cycle_time_ms", pa.int32()),
        pa.field("actual_cycle_time_ms", pa.int32()),
        pa.field("planned_production_time_s", pa.int32()),
        pa.field("downtime_s", pa.int32()),
    ]
)

EQUIPMENT_STATUS_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("timestamp", pa.string(), nullable=False),
        pa.field("ingested_at", pa.string(), nullable=False),
        pa.field("schema_version", pa.string()),
        pa.field("equipment_id", pa.string()),
        pa.field("line_id", pa.string()),
        pa.field("work_unit_id", pa.string()),
        pa.field("status", pa.string()),
        pa.field("previous_status", pa.string()),
        pa.field("downtime_reason", pa.string()),
        pa.field("planned_downtime", pa.bool_()),
        pa.field("temperature_c", pa.float32()),
        pa.field("vibration_hz", pa.float32()),
        pa.field("power_kw", pa.float32()),
        pa.field("fault_code", pa.string()),
    ]
)

QUALITY_METRICS_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("timestamp", pa.string(), nullable=False),
        pa.field("ingested_at", pa.string(), nullable=False),
        pa.field("schema_version", pa.string()),
        pa.field("batch_id", pa.string()),
        pa.field("line_id", pa.string()),
        pa.field("work_unit_id", pa.string()),
        pa.field("shift", pa.string()),
        pa.field("units_inspected", pa.int32()),
        pa.field("units_passed", pa.int32()),
        pa.field("units_failed", pa.int32()),
        pa.field("defect_rate_pct", pa.float32()),
        pa.field("defect_type", pa.string()),
        pa.field("yield_pct", pa.float32()),
        pa.field("spec_upper", pa.float32()),
        pa.field("spec_lower", pa.float32()),
        pa.field("measured_mean", pa.float32()),
        pa.field("measured_stddev", pa.float32()),
        pa.field("cpk", pa.float32()),
    ]
)

TOPIC_CONFIG = {
    "line-events": ("line_events", LINE_EVENTS_SCHEMA),
    "equipment-status": ("equipment_status", EQUIPMENT_STATUS_SCHEMA),
    "quality-metrics": ("quality_metrics", QUALITY_METRICS_SCHEMA),
}


def build_catalog() -> RestCatalog:
    return RestCatalog(
        name="nessie",
        uri=NESSIE_URI,
        warehouse=WAREHOUSE,
        **{
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )


def ensure_tables(catalog: RestCatalog) -> dict:
    """Create namespace + tables if they don't exist. Returns {topic: IcebergTable}."""

    ns = ("manufacturing",)
    existing_ns = [tuple(n) for n in catalog.list_namespaces()]
    if ns not in existing_ns:
        catalog.create_namespace(ns)
        log.info("Created namespace: manufacturing")

    tables = {}
    for topic, (table_name, arrow_schema) in TOPIC_CONFIG.items():
        identifier = (*ns, table_name)
        try:
            tables[topic] = catalog.load_table(identifier)
            log.info("Loaded existing table: %s", table_name)
        except Exception:
            iceberg_schema = _arrow_to_iceberg_schema(arrow_schema)
            tables[topic] = catalog.create_table(
                identifier=identifier,
                schema=iceberg_schema,
            )
            log.info("Created table: %s", table_name)
    return tables


def _arrow_to_iceberg_schema(arrow_schema: pa.Schema):
    """Convert PyArrow schema to PyIceberg schema."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        BooleanType,
        FloatType,
        IntegerType,
        NestedField,
        StringType,
    )

    def _map_type(arrow_type):
        if arrow_type == pa.string():
            return StringType()
        if arrow_type == pa.int32():
            return IntegerType()
        if arrow_type == pa.float32():
            return FloatType()
        if arrow_type == pa.bool_():
            return BooleanType()
        return StringType()

    fields = []
    for i, field in enumerate(arrow_schema, start=1):
        fields.append(
            NestedField(
                field_id=i,
                name=field.name,
                field_type=_map_type(field.type),
                required=not field.nullable,
            )
        )
    return Schema(*fields)


def normalize_record(topic: str, record: dict, now_iso: str) -> dict:
    """Coerce types and add ingested_at."""
    record = dict(record)
    record["ingested_at"] = now_iso

    _, arrow_schema = TOPIC_CONFIG[topic]

    out = {}
    for field in arrow_schema:
        val = record.get(field.name)
        if val is None:
            out[field.name] = None
            continue
        if field.type in (pa.int32(),):
            out[field.name] = int(val)
        elif field.type in (pa.float32(),):
            out[field.name] = float(val) if val is not None else None
        elif field.type == pa.bool_():
            out[field.name] = bool(val)
        else:
            out[field.name] = str(val) if val is not None else None
    return out


def flush_batch(tables: dict, buffers: dict, now_iso: str):
    for topic, rows in buffers.items():
        if not rows:
            continue
        table_name, arrow_schema = TOPIC_CONFIG[topic][0], TOPIC_CONFIG[topic][1]
        normalized = [normalize_record(topic, r, now_iso) for r in rows]

        arrays = {}
        for field in arrow_schema:
            col_vals = [r.get(field.name) for r in normalized]
            arrays[field.name] = pa.array(col_vals, type=field.type)

        batch = pa.RecordBatch.from_pydict(arrays, schema=arrow_schema)
        arrow_table = pa.Table.from_batches([batch])

        iceberg_table = tables[topic]
        iceberg_table.append(arrow_table)
        log.info("Iceberg append %s: %d rows", table_name, len(rows))


def main():
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    log.info("Starting Iceberg Sink (60s micro-batches)")
    log.info("Nessie: %s  Warehouse: %s", NESSIE_URI, WAREHOUSE)

    # Wait for Nessie to be ready
    catalog = None
    for attempt in range(20):
        try:
            catalog = build_catalog()
            tables = ensure_tables(catalog)
            break
        except Exception as e:
            log.warning("Catalog not ready (attempt %d/20): %s", attempt + 1, e)
            time.sleep(10)
    else:
        log.error("Could not connect to Nessie catalog after 20 attempts. Exiting.")
        return

    consumers = {topic: make_consumer(topic, f"iceberg-sink-{topic}") for topic in TOPIC_CONFIG}

    buffers: dict = defaultdict(list)
    batch_start = time.time()

    try:
        while _running:
            now = time.time()

            for topic, consumer in consumers.items():
                msg = consumer.poll(0.1)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error("Kafka error on %s: %s", topic, msg.error())
                    continue
                try:
                    record = json.loads(msg.value().decode("utf-8"))
                    buffers[topic].append(record)
                except Exception as e:
                    log.warning("Decode error on %s: %s", topic, e)

            if now - batch_start >= BATCH_INTERVAL_S:
                now_iso = datetime.fromtimestamp(now, tz=timezone.utc).isoformat()
                try:
                    flush_batch(tables, buffers, now_iso)
                except Exception as e:
                    log.error("Flush failed: %s", e)
                for topic in buffers:
                    buffers[topic] = []
                batch_start = now

    finally:
        for consumer in consumers.values():
            consumer.close()
        log.info("Iceberg Sink stopped.")


if __name__ == "__main__":
    main()
