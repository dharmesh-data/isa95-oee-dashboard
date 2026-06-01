import random
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from simulator.config import AREA_ID, SENSOR_CONFIG, SITE_ID
from simulator.models.fault_injector import FaultInjector


class WorkUnit:
    """
    ISA-95 Work Unit — lowest level of the hierarchy.
    Manages its own state machine: RUNNING → FAULT → RUNNING etc.
    """

    STATUSES = ["RUNNING", "IDLE", "FAULT", "MAINTENANCE", "CHANGEOVER", "STARTUP"]

    def __init__(self, line_id: str, work_unit_id: str, line_config: Dict[str, Any]):
        self.line_id = line_id
        self.work_unit_id = work_unit_id
        self.equipment_id = f"{line_id}-{work_unit_id}"
        self.line_config = line_config
        self.fault_injector = FaultInjector(line_id)

        self.status = "STARTUP"
        self.previous_status = "UNKNOWN"
        self._fault_remaining_s: float = 0.0
        self._units_since_last_quality_batch: int = 0
        self._batch_counter: int = 0

        # Sensor state — evolves over time
        self._temp_c: float = random.gauss(
            SENSOR_CONFIG["temp_normal_mean"], SENSOR_CONFIG["temp_normal_std"]
        )
        self._vibration_hz: float = random.gauss(
            SENSOR_CONFIG["vibration_normal_mean"],
            SENSOR_CONFIG["vibration_normal_std"],
        )
        self._power_kw: float = random.gauss(
            SENSOR_CONFIG["power_normal_mean"], SENSOR_CONFIG["power_normal_std"]
        )

    def _update_sensors(self):
        if self.status == "FAULT":
            self._temp_c = random.gauss(
                SENSOR_CONFIG["temp_fault_mean"], SENSOR_CONFIG["temp_fault_std"]
            )
            self._vibration_hz = random.gauss(
                SENSOR_CONFIG["vibration_fault_mean"],
                SENSOR_CONFIG["vibration_fault_std"],
            )
        else:
            # Gradual mean-reversion toward normal
            self._temp_c = 0.9 * self._temp_c + 0.1 * random.gauss(
                SENSOR_CONFIG["temp_normal_mean"], SENSOR_CONFIG["temp_normal_std"]
            )
            self._vibration_hz = 0.9 * self._vibration_hz + 0.1 * random.gauss(
                SENSOR_CONFIG["vibration_normal_mean"],
                SENSOR_CONFIG["vibration_normal_std"],
            )
        self._power_kw = random.gauss(
            SENSOR_CONFIG["power_normal_mean"], SENSOR_CONFIG["power_normal_std"]
        )

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _make_equipment_event(
        self, reason: str = "NONE", fault_code: Optional[str] = None
    ) -> Dict[str, Any]:
        return {
            "event_id": str(uuid.uuid4()),
            "timestamp": self._now_iso(),
            "schema_version": "1.0",
            "equipment_id": self.equipment_id,
            "line_id": self.line_id,
            "work_unit_id": self.work_unit_id,
            "status": self.status,
            "previous_status": self.previous_status,
            "downtime_reason": reason,
            "planned_downtime": reason in ("PLANNED_MAINTENANCE", "CHANGEOVER"),
            "temperature_c": round(self._temp_c, 2),
            "vibration_hz": round(self._vibration_hz, 2),
            "power_kw": round(self._power_kw, 2),
            "fault_code": fault_code,
        }

    def _make_cycle_event(
        self, shift: str, units: int, rejected: int, actual_ms: int, downtime_s: int = 0
    ) -> Dict[str, Any]:
        return {
            "event_id": str(uuid.uuid4()),
            "timestamp": self._now_iso(),
            "schema_version": "1.0",
            "site_id": SITE_ID,
            "area_id": AREA_ID,
            "line_id": self.line_id,
            "work_unit_id": self.work_unit_id,
            "shift": shift,
            "event_type": "CYCLE_COMPLETE",
            "units_produced": units,
            "units_rejected": rejected,
            "ideal_cycle_time_ms": self.line_config["ideal_cycle_time_ms"],
            "actual_cycle_time_ms": actual_ms,
            "planned_production_time_s": 27000,
            "downtime_s": downtime_s,
        }

    def _make_quality_event(
        self, shift: str, units_inspected: int, units_failed: int
    ) -> Dict[str, Any]:
        units_passed = units_inspected - units_failed
        defect_rate = round(units_failed / units_inspected * 100, 2) if units_inspected > 0 else 0.0
        spec_upper = self.line_config["spec_upper"]
        spec_lower = self.line_config["spec_lower"]
        spec_nominal = self.line_config["spec_nominal"]

        # Simulate measurement distribution
        measured_mean = round(random.gauss(spec_nominal, (spec_upper - spec_lower) / 6), 4)
        measured_stddev = round(abs(random.gauss(0.015, 0.005)), 4)

        cpk = 0.0
        if measured_stddev > 0:
            cpk = round(
                min(
                    (spec_upper - measured_mean) / (3 * measured_stddev),
                    (measured_mean - spec_lower) / (3 * measured_stddev),
                ),
                3,
            )

        self._batch_counter += 1
        defect_type = (
            "NONE"
            if units_failed == 0
            else random.choice(
                [
                    "DIMENSIONAL",
                    "SURFACE_FINISH",
                    "ASSEMBLY_ERROR",
                    "WEIGHT",
                    "COSMETIC",
                ]
            )
        )

        return {
            "event_id": str(uuid.uuid4()),
            "timestamp": self._now_iso(),
            "schema_version": "1.0",
            "batch_id": f"BATCH-{datetime.now().strftime('%Y%m%d')}-{self._batch_counter:04d}",
            "line_id": self.line_id,
            "work_unit_id": self.work_unit_id,
            "shift": shift,
            "units_inspected": units_inspected,
            "units_passed": units_passed,
            "units_failed": units_failed,
            "defect_rate_pct": defect_rate,
            "defect_type": defect_type,
            "yield_pct": (
                round(units_passed / units_inspected * 100, 2) if units_inspected > 0 else 0.0
            ),
            "spec_upper": spec_upper,
            "spec_lower": spec_lower,
            "measured_mean": measured_mean,
            "measured_stddev": measured_stddev,
            "cpk": cpk,
        }

    def get_current_shift(self) -> str:
        hour = datetime.now().hour
        if 6 <= hour < 14:
            return "MORNING"
        elif 14 <= hour < 22:
            return "AFTERNOON"
        return "NIGHT"

    async def tick(
        self,
        line_events: list,
        equipment_events: list,
        quality_events: list,
        cycle_sleep_s: float,
    ):
        """Single production tick. Appends events to the provided lists."""
        self._update_sensors()
        shift = self.get_current_shift()

        if self.status == "STARTUP":
            self.previous_status = self.status
            self.status = "RUNNING"
            equipment_events.append(self._make_equipment_event())
            return

        if self.status == "FAULT":
            self._fault_remaining_s -= cycle_sleep_s
            if self._fault_remaining_s <= 0:
                self.previous_status = "FAULT"
                self.status = "RUNNING"
                equipment_events.append(self._make_equipment_event())
            return

        # Check for new fault
        if self.fault_injector.should_fault(self._temp_c, self._vibration_hz):
            reason = self.fault_injector.get_downtime_reason(self._temp_c, self._vibration_hz)
            fault_code = self.fault_injector.get_fault_code(reason)
            self._fault_remaining_s = self.fault_injector.get_mttr_seconds(reason)

            self.previous_status = self.status
            self.status = "FAULT"

            equipment_events.append(
                self._make_equipment_event(reason=reason, fault_code=fault_code)
            )

            # Emit fault line event so OEE calculator can account for downtime
            fault_cycle = self._make_cycle_event(
                shift=shift,
                units=0,
                rejected=0,
                actual_ms=0,
                downtime_s=int(self._fault_remaining_s),
            )
            fault_cycle["event_type"] = "FAULT"
            line_events.append(fault_cycle)
            return

        # Normal running cycle
        ideal_ms = self.line_config["ideal_cycle_time_ms"]
        # Performance variation: ±15%
        actual_ms = int(ideal_ms * random.uniform(0.90, 1.15))
        units = random.randint(10, 15)
        # Quality: reject 0–3% of units
        rejected = sum(1 for _ in range(units) if random.random() < 0.02)

        line_events.append(self._make_cycle_event(shift, units, rejected, actual_ms))

        # Equipment status every 5th cycle
        if random.random() < 0.2:
            equipment_events.append(self._make_equipment_event())

        # Quality batch every N units
        self._units_since_last_quality_batch += units
        batch_size = self.line_config["quality_batch_size"]
        if self._units_since_last_quality_batch >= batch_size:
            inspected = self._units_since_last_quality_batch
            failed = sum(1 for _ in range(inspected) if random.random() < 0.025)
            quality_events.append(self._make_quality_event(shift, inspected, failed))
            self._units_since_last_quality_batch = 0
