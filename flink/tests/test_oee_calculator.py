"""Unit tests for OEE calculation logic — no Kafka, no DB, no Docker."""

from datetime import datetime, timezone

from flink.jobs.oee_calculator import compute_oee
from flink.tests.fixtures.events import cycle_event

_NOW = datetime(2024, 3, 15, 8, 0, 0, tzinfo=timezone.utc)


class TestComputeOEE:
    def test_perfect_line(self):
        """No faults, no rejects, ideal cycle time → OEE ≈ 1.0."""
        events = [cycle_event(units_produced=750, units_rejected=0, downtime_s=0)]
        result = compute_oee("LINE-A", events, _NOW)

        assert result["availability"] == 1.0
        assert result["quality"] == 1.0
        assert result["oee"] == 1.0
        assert result["line_id"] == "LINE-A"

    def test_availability_drops_with_fault(self):
        """10-min fault in 60-min planned window → availability = 50/60 = 0.8333."""
        events = [
            cycle_event(units_produced=5, downtime_s=600, event_type="FAULT"),
            cycle_event(units_produced=5),
        ]
        result = compute_oee("LINE-A", events, _NOW)
        assert abs(result["availability"] - (3600 - 600) / 3600) < 0.001

    def test_quality_drops_with_rejects(self):
        """10 units rejected out of 100 → quality = 0.9."""
        events = [cycle_event(units_produced=100, units_rejected=10, downtime_s=0)]
        result = compute_oee("LINE-A", events, _NOW)
        assert abs(result["quality"] - 0.9) < 0.001

    def test_oee_is_product_of_components(self):
        """OEE = availability × performance × quality."""
        events = [cycle_event(units_produced=50, units_rejected=5, downtime_s=360)]
        result = compute_oee("LINE-A", events, _NOW)
        expected = result["availability"] * result["performance"] * result["quality"]
        assert abs(result["oee"] - expected) < 1e-3

    def test_zero_units_gives_zero_oee(self):
        """No production → performance = 0 → OEE = 0."""
        events = [cycle_event(units_produced=0, units_rejected=0)]
        result = compute_oee("LINE-A", events, _NOW)
        assert result["oee"] == 0.0
        assert result["performance"] == 0.0

    def test_total_and_good_units_counted(self):
        events = [
            cycle_event(units_produced=30, units_rejected=3),
            cycle_event(units_produced=20, units_rejected=2),
        ]
        result = compute_oee("LINE-A", events, _NOW)
        assert result["total_units"] == 50
        assert result["good_units"] == 45

    def test_performance_capped_at_one(self):
        """If actual cycle time faster than ideal, performance must not exceed 1.0."""
        events = [cycle_event(units_produced=10000, downtime_s=0)]
        result = compute_oee("LINE-A", events, _NOW)
        assert result["performance"] <= 1.0

    def test_downtime_only_counted_for_fault_events(self):
        """downtime_s on CYCLE_COMPLETE events is ignored."""
        fault_events = [cycle_event(units_produced=0, downtime_s=600, event_type="FAULT")]
        cycle_events = [cycle_event(units_produced=0, downtime_s=600, event_type="CYCLE_COMPLETE")]

        result_fault = compute_oee("LINE-A", fault_events, _NOW)
        result_cycle = compute_oee("LINE-A", cycle_events, _NOW)

        assert result_fault["availability"] < 1.0
        assert result_cycle["availability"] == 1.0

    def test_all_three_lines_have_config(self):
        for line_id in ("LINE-A", "LINE-B", "LINE-C"):
            events = [cycle_event(line_id=line_id, units_produced=10)]
            result = compute_oee(line_id, events, _NOW)
            assert result["line_id"] == line_id

    def test_result_keys_present(self):
        events = [cycle_event()]
        result = compute_oee("LINE-A", events, _NOW)
        for key in (
            "time",
            "line_id",
            "availability",
            "performance",
            "quality",
            "oee",
            "total_units",
            "good_units",
        ):
            assert key in result

    def test_oee_world_class_threshold(self):
        """Simulate realistic world-class line: OEE should exceed 85%."""
        events = [
            cycle_event(units_produced=700, units_rejected=7, downtime_s=180),
        ]
        result = compute_oee("LINE-A", events, _NOW)
        assert result["oee"] >= 0.85, f"Expected ≥85%, got {result['oee']:.1%}"
