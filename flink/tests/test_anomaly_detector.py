"""Unit tests for anomaly detection pattern logic."""

from flink.jobs.anomaly_detector import HIGH_TEMP, HIGH_VIBRATION, PATTERN_WINDOW_S
from flink.tests.fixtures.events import equipment_event


def _run_detector(events_with_delays):
    """
    Simulate the detector's stateful pattern matching.
    events_with_delays: list of (event_dict, wall_clock_offset_seconds)
    Returns list of generated alerts.
    """
    precursors = {}
    alerts = []
    base_time = 1000.0

    for event, offset in events_with_delays:
        now = base_time + offset
        equipment_id = event.get("equipment_id", "UNKNOWN")
        status = event.get("status", "")
        temp = float(event.get("temperature_c") or 0)
        vib = float(event.get("vibration_hz") or 0)

        is_high_sensor = temp > HIGH_TEMP or vib > HIGH_VIBRATION

        if is_high_sensor and status != "FAULT":
            precursors[equipment_id] = {"time": now, "temp": temp, "vib": vib}
        elif status == "FAULT" and equipment_id in precursors:
            p = precursors[equipment_id]
            if now - p["time"] <= PATTERN_WINDOW_S:
                alerts.append(
                    {
                        "equipment_id": equipment_id,
                        "alert_type": "THERMAL_FAULT_PRECURSOR",
                        "temperature_c": p["temp"],
                    }
                )
            del precursors[equipment_id]

    return alerts


class TestAnomalyDetector:
    def test_high_temp_then_fault_triggers_alert(self):
        events = [
            (equipment_event(status="RUNNING", temperature_c=90.0), 0),
            (equipment_event(status="FAULT"), 10),
        ]
        alerts = _run_detector(events)
        assert len(alerts) == 1
        assert alerts[0]["alert_type"] == "THERMAL_FAULT_PRECURSOR"

    def test_high_vibration_then_fault_triggers_alert(self):
        events = [
            (
                equipment_event(status="RUNNING", vibration_hz=130.0, temperature_c=70.0),
                0,
            ),
            (equipment_event(status="FAULT"), 5),
        ]
        alerts = _run_detector(events)
        assert len(alerts) == 1

    def test_fault_without_precursor_no_alert(self):
        events = [
            (equipment_event(status="FAULT", temperature_c=70.0, vibration_hz=80.0), 0),
        ]
        alerts = _run_detector(events)
        assert len(alerts) == 0

    def test_precursor_outside_window_no_alert(self):
        """High temp 35s before fault — outside 30s window → no alert."""
        events = [
            (equipment_event(status="RUNNING", temperature_c=90.0), 0),
            (equipment_event(status="FAULT"), PATTERN_WINDOW_S + 5),
        ]
        alerts = _run_detector(events)
        assert len(alerts) == 0

    def test_precursor_at_boundary_triggers_alert(self):
        """High temp exactly at window boundary → alert fires."""
        events = [
            (equipment_event(status="RUNNING", temperature_c=90.0), 0),
            (equipment_event(status="FAULT"), PATTERN_WINDOW_S),
        ]
        alerts = _run_detector(events)
        assert len(alerts) == 1

    def test_normal_temp_and_vibration_no_precursor(self):
        """Values at exactly the threshold (not above) don't register as precursor."""
        events = [
            (
                equipment_event(
                    status="RUNNING",
                    temperature_c=HIGH_TEMP,
                    vibration_hz=HIGH_VIBRATION,
                ),
                0,
            ),
            (equipment_event(status="FAULT"), 5),
        ]
        alerts = _run_detector(events)
        assert len(alerts) == 0

    def test_multiple_equipment_independent(self):
        """Two different equipment IDs — only one has precursor."""
        events = [
            (
                equipment_event(equipment_id="EQ-A", status="RUNNING", temperature_c=90.0),
                0,
            ),
            (
                equipment_event(equipment_id="EQ-B", status="FAULT", temperature_c=70.0),
                5,
            ),
            (equipment_event(equipment_id="EQ-A", status="FAULT"), 10),
        ]
        alerts = _run_detector(events)
        assert len(alerts) == 1
        assert alerts[0]["equipment_id"] == "EQ-A"

    def test_precursor_consumed_after_alert(self):
        """Second fault for same equipment doesn't re-trigger (state cleared)."""
        events = [
            (equipment_event(status="RUNNING", temperature_c=90.0), 0),
            (equipment_event(status="FAULT"), 5),
            (equipment_event(status="FAULT"), 10),
        ]
        alerts = _run_detector(events)
        assert len(alerts) == 1
