"""
Iceberg time-travel demo — connects to Nessie/MinIO and queries Iceberg tables.
Demonstrates: snapshot history, time-travel, schema evolution, partition pruning.

Usage:
    python scripts/query_iceberg.py [--table line_events|equipment_status|quality_metrics]
    python scripts/query_iceberg.py --time-travel          # show snapshot history
    python scripts/query_iceberg.py --as-of-snapshot <id>  # query historical snapshot
"""

import argparse
import os
import sys

NESSIE_URI = os.environ.get("NESSIE_URI", "http://nessie.orb.local:19120/iceberg")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio.orb.local:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "s3://manufacturing-iceberg/warehouse")


def build_catalog():
    from pyiceberg.catalog.rest import RestCatalog

    return RestCatalog(
        name="nessie",
        uri=NESSIE_URI,
        warehouse=WAREHOUSE,
        **{
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "s3.path-style-access": "true",
        },
    )


def list_tables(catalog):
    ns = ("manufacturing",)
    print("\n=== Iceberg Tables in manufacturing namespace ===")
    for ident in catalog.list_tables(ns):
        table = catalog.load_table(ident)
        snapshots = table.metadata.snapshots
        current = table.current_snapshot()
        row_count = current.summary.get("total-records", "?") if current else 0
        print(f"  {ident[-1]:30s}  snapshots={len(snapshots)}  rows~={row_count}")


def show_snapshots(catalog, table_name: str):
    table = catalog.load_table(("manufacturing", table_name))
    print(f"\n=== Snapshot history: {table_name} ===")
    for snap in table.metadata.snapshots:
        ts = snap.timestamp_ms / 1000
        from datetime import datetime, timezone

        dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        print(
            f"  snapshot_id={snap.snapshot_id}  timestamp={dt}  "
            f"operation={snap.summary.get('operation', '?')}  "
            f"added-records={snap.summary.get('added-records', '?')}"
        )


def query_table(catalog, table_name: str, limit: int = 20, as_of_snapshot: int = None):
    table = catalog.load_table(("manufacturing", table_name))

    if as_of_snapshot:
        scan = table.scan(snapshot_id=as_of_snapshot)
        print(f"\n=== Time-travel: {table_name} @ snapshot {as_of_snapshot} ===")
    else:
        scan = table.scan()
        print(f"\n=== Current state: {table_name} (first {limit} rows) ===")

    arrow_table = scan.to_arrow()
    total = len(arrow_table)
    print(f"Total rows: {total}")

    if total > 0:
        sample = arrow_table.slice(0, min(limit, total))
        for col in sample.column_names[:6]:
            vals = sample.column(col).to_pylist()
            print(f"  {col}: {vals[:3]}{'...' if len(vals) > 3 else ''}")

    return arrow_table


def oee_summary_from_iceberg(catalog):
    """Example: compute OEE rate from line_events in Iceberg using PyArrow."""
    print("\n=== OEE from Iceberg (line_events) ===")
    table = catalog.load_table(("manufacturing", "line_events"))
    arrow_table = table.scan(row_filter="event_type = 'CYCLE_COMPLETE'").to_arrow()

    if len(arrow_table) == 0:
        print("No CYCLE_COMPLETE events yet.")
        return

    import pyarrow.compute as pc

    for line_id in ["LINE-A", "LINE-B", "LINE-C"]:
        mask = pc.equal(arrow_table.column("line_id"), line_id)
        subset = arrow_table.filter(mask)
        if len(subset) == 0:
            continue
        total = pc.sum(subset.column("units_produced")).as_py() or 0
        rejected = pc.sum(subset.column("units_rejected")).as_py() or 0
        quality = (total - rejected) / total if total > 0 else 0
        print(
            f"  {line_id}: {len(subset)} batches, {total} units, "
            f"quality={quality:.1%}, rejected={rejected}"
        )


def main():
    parser = argparse.ArgumentParser(description="Query Iceberg tables via Nessie")
    parser.add_argument(
        "--table",
        default=None,
        choices=["line_events", "equipment_status", "quality_metrics"],
        help="Table to query (default: list all)",
    )
    parser.add_argument(
        "--time-travel", action="store_true", help="Show snapshot history for --table"
    )
    parser.add_argument(
        "--as-of-snapshot",
        type=int,
        default=None,
        help="Query table at specific snapshot ID",
    )
    parser.add_argument("--oee", action="store_true", help="Compute OEE summary from line_events")
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()

    try:
        catalog = build_catalog()
    except Exception as e:
        print(f"ERROR: Could not connect to Nessie at {NESSIE_URI}: {e}", file=sys.stderr)
        print("Is the stack running? Try: docker compose up -d", file=sys.stderr)
        sys.exit(1)

    if args.oee:
        oee_summary_from_iceberg(catalog)
        return

    if args.table is None:
        list_tables(catalog)
        return

    if args.time_travel:
        show_snapshots(catalog, args.table)
        return

    query_table(catalog, args.table, limit=args.limit, as_of_snapshot=args.as_of_snapshot)


if __name__ == "__main__":
    main()
