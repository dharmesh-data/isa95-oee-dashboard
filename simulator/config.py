from typing import Any, Dict

LINE_CONFIG: Dict[str, Dict[str, Any]] = {
    "LINE-A": {
        "ideal_cycle_time_ms": 4800,
        "target_oee": 0.87,
        "quality_batch_size": 50,
        "spec_upper": 10.05,
        "spec_lower": 9.95,
        "spec_nominal": 10.00,
    },
    "LINE-B": {
        "ideal_cycle_time_ms": 6200,
        "target_oee": 0.82,
        "quality_batch_size": 50,
        "spec_upper": 25.10,
        "spec_lower": 24.90,
        "spec_nominal": 25.00,
    },
    "LINE-C": {
        "ideal_cycle_time_ms": 3500,
        "target_oee": 0.79,
        "quality_batch_size": 50,
        "spec_upper": 5.025,
        "spec_lower": 4.975,
        "spec_nominal": 5.00,
    },
}

FAULT_CONFIG: Dict[str, Any] = {
    "base_fault_rate": 0.02,  # 2% chance per cycle
    "monday_morning_multiplier": 2.5,  # cold start
    "shift_changeover_multiplier": 1.8,
    "high_temp_fault_threshold": 85.0,  # °C
    "high_vibration_threshold": 120.0,  # Hz
    "fault_cluster_probability": 0.4,  # one fault triggers more
    "mttr_minutes": {
        "MECHANICAL_FAILURE": 45,
        "ELECTRICAL_FAULT": 30,
        "MATERIAL_SHORTAGE": 15,
        "QUALITY_HOLD": 60,
        "OPERATOR_ABSENCE": 20,
        "PLANNED_MAINTENANCE": 30,
        "CHANGEOVER": 25,
    },
}

SHIFT_SCHEDULE: Dict[str, Dict[str, Any]] = {
    "MORNING": {"start": "06:00", "end": "14:00", "planned_stops": ["10:00"]},
    "AFTERNOON": {"start": "14:00", "end": "22:00", "planned_stops": ["18:00"]},
    "NIGHT": {"start": "22:00", "end": "06:00", "planned_stops": ["02:00"]},
}

SENSOR_CONFIG: Dict[str, Any] = {
    "temp_normal_mean": 65.0,
    "temp_normal_std": 5.0,
    "temp_fault_mean": 88.0,
    "temp_fault_std": 4.0,
    "vibration_normal_mean": 45.0,
    "vibration_normal_std": 8.0,
    "vibration_fault_mean": 130.0,
    "vibration_fault_std": 15.0,
    "power_normal_mean": 10.0,
    "power_normal_std": 1.5,
}

SITE_ID = "BLR-PLANT-01"
AREA_ID = "ASSEMBLY"
MACHINES_PER_LINE = 5
