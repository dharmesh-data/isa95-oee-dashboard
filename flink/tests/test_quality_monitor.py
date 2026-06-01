"""Unit tests for quality SPC / Cpk computation."""

from flink.jobs.quality_monitor import compute_cpk
from flink.tests.fixtures.events import quality_event


class TestComputeCpk:
    def test_returns_none_for_single_event(self):
        """Need at least 2 events for statistics."""
        result = compute_cpk([quality_event()])
        assert result is None

    def test_returns_none_for_zero_stddev(self):
        """All stddevs = 0 → can't compute Cpk."""
        events = [quality_event(measured_stddev=0.0) for _ in range(3)]
        result = compute_cpk(events)
        assert result is None

    def test_cpk_within_spec(self):
        """Mean centered, tight spread → Cpk > 1.33 (capable process)."""
        events = [
            quality_event(
                measured_mean=10.0,
                measured_stddev=0.01,
                spec_upper=10.05,
                spec_lower=9.95,
            )
            for _ in range(5)
        ]
        result = compute_cpk(events)
        assert result is not None
        assert result["rolling_cpk"] > 1.33

    def test_cpk_below_one_indicates_incapable_process(self):
        """Mean near upper spec + high variance → Cpk < 1.0."""
        events = [
            quality_event(
                measured_mean=10.04,
                measured_stddev=0.04,
                spec_upper=10.05,
                spec_lower=9.95,
            )
            for _ in range(5)
        ]
        result = compute_cpk(events)
        assert result is not None
        assert result["rolling_cpk"] < 1.0

    def test_result_keys_present(self):
        events = [quality_event() for _ in range(3)]
        result = compute_cpk(events)
        assert result is not None
        for key in ("rolling_cpk", "rolling_mean", "rolling_stddev", "window_batches"):
            assert key in result

    def test_window_batches_count(self):
        n = 7
        events = [quality_event() for _ in range(n)]
        result = compute_cpk(events)
        assert result["window_batches"] == n

    def test_cpk_uses_min_of_upper_and_lower(self):
        """Mean shifted toward upper spec → Cpk limited by upper side."""
        events_shifted = [
            quality_event(
                measured_mean=10.04,
                measured_stddev=0.01,
                spec_upper=10.05,
                spec_lower=9.95,
            )
            for _ in range(3)
        ]
        events_centered = [
            quality_event(
                measured_mean=10.00,
                measured_stddev=0.01,
                spec_upper=10.05,
                spec_lower=9.95,
            )
            for _ in range(3)
        ]
        result_shifted = compute_cpk(events_shifted)
        result_centered = compute_cpk(events_centered)
        assert result_shifted["rolling_cpk"] < result_centered["rolling_cpk"]

    def test_rolling_mean_is_average_of_means(self):
        events = [
            quality_event(measured_mean=10.0, measured_stddev=0.02),
            quality_event(measured_mean=10.2, measured_stddev=0.02),
        ]
        result = compute_cpk(events)
        assert result is not None
        assert abs(result["rolling_mean"] - 10.1) < 0.001
