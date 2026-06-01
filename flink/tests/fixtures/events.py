"""Reusable test fixtures — no I/O, no Kafka, no DB."""

from datetime import datetime, timezone

_NOW = datetime(2024, 3, 15, 8, 0, 0, tzinfo=timezone.utc)


def cycle_event(
    line_id="LINE-A",
    units_produced=10,
    units_rejected=0,
    downtime_s=0,
    event_type="CYCLE_COMPLETE",
    ideal_cycle_time_ms=4800,
    actual_cycle_time_ms=4800,
):
    return {
        "event_id": "test-001",
        "timestamp": _NOW.isoformat(),
        "line_id": line_id,
        "work_unit_id": "WU-01",
        "shift": "MORNING",
        "event_type": event_type,
        "units_produced": units_produced,
        "units_rejected": units_rejected,
        "ideal_cycle_time_ms": ideal_cycle_time_ms,
        "actual_cycle_time_ms": actual_cycle_time_ms,
        "planned_production_time_s": 3600,
        "downtime_s": downtime_s,
    }


def equipment_event(
    equipment_id="LINE-A-WU-01",
    line_id="LINE-A",
    status="RUNNING",
    temperature_c=70.0,
    vibration_hz=80.0,
    fault_code=None,
):
    return {
        "event_id": "test-eq-001",
        "timestamp": _NOW.isoformat(),
        "equipment_id": equipment_id,
        "line_id": line_id,
        "status": status,
        "previous_status": "RUNNING",
        "temperature_c": temperature_c,
        "vibration_hz": vibration_hz,
        "power_kw": 10.0,
        "fault_code": fault_code,
        "downtime_reason": "NONE",
        "planned_downtime": False,
    }


def quality_event(
    line_id="LINE-A",
    units_inspected=50,
    units_passed=48,
    measured_mean=10.01,
    measured_stddev=0.018,
    spec_upper=10.05,
    spec_lower=9.95,
):
    return {
        "event_id": "test-q-001",
        "timestamp": _NOW.isoformat(),
        "batch_id": "BATCH-001",
        "line_id": line_id,
        "work_unit_id": "WU-01",
        "shift": "MORNING",
        "units_inspected": units_inspected,
        "units_passed": units_passed,
        "units_failed": units_inspected - units_passed,
        "defect_rate_pct": round((units_inspected - units_passed) / units_inspected * 100, 2),
        "yield_pct": round(units_passed / units_inspected * 100, 2),
        "spec_upper": spec_upper,
        "spec_lower": spec_lower,
        "measured_mean": measured_mean,
        "measured_stddev": measured_stddev,
        "cpk": 0.93,
    }
