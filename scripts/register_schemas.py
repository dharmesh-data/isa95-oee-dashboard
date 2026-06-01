"""
Register Avro schemas with Schema Registry.
Run: python scripts/register_schemas.py
"""

import json
import os
import sys

import requests

SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")

SCHEMAS = [
    ("line-events-value", "schemas/line_events.avsc"),
    ("equipment-status-value", "schemas/equipment_status.avsc"),
    ("quality-metrics-value", "schemas/quality_metrics.avsc"),
]


def register(subject: str, schema_path: str) -> bool:
    with open(schema_path) as f:
        schema_str = f.read()

    # Validate JSON
    json.loads(schema_str)

    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"
    resp = requests.post(
        url,
        json={"schema": schema_str},
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        timeout=10,
    )

    if resp.status_code in (200, 201):
        data = resp.json()
        print(f"  ✓  {subject}  →  id={data.get('id')}")
        return True
    else:
        print(f"  ✗  {subject}  →  {resp.status_code}: {resp.text}")
        return False


def main():
    print(f"Registering schemas with {SCHEMA_REGISTRY_URL}")
    success = all(register(subject, path) for subject, path in SCHEMAS)
    if not success:
        print("\nSome schemas failed to register.")
        sys.exit(1)
    print("\nAll schemas registered.")


if __name__ == "__main__":
    main()
