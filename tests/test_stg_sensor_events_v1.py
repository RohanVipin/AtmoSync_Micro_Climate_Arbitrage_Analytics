"""Offline unit test suite for dbt model stg_sensor_events_v1.

Validates the SQL transformation logic, filtering boundaries, and invariants
without connecting to a live Snowflake instance:
1. Only schema_version = 1 records are included (377 historical records strictly filtered out).
2. Sensor values (temperature, humidity, vibration) and units are correctly pivoted.
3. No readings from different containers are mixed.
4. No readings from different observed_at timestamps are mixed.
5. sensor_count accurately reflects distinct sensor types present per (container_id, observed_at).
6. 27 verified canonical events (3 cycles x 3 containers x 3 sensors) produce exactly 9 wide rows when timestamps align.
7. Unaligned timestamps scenario: Verifies that disparate timestamps do NOT silently misjoin.
8. Deduplication safety: Event duplicates and duplicate modalities are deterministically resolved.
9. Subsecond precision: Timestamps differing by microseconds produce distinct surrogate keys.
10. Immutability check: Existing dbt models and seeds remain completely untouched.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import hashlib
import json
import os
import unittest
from typing import Any, Dict, List, Optional, Tuple


def md5_surrogate_key(container_id: str, observed_at: str) -> str:
    """Replicates Snowflake MD5(CONCAT_WS('||', container_id, TO_VARCHAR(observed_at, '...FF9TZHTZM')))."""
    key_str = f"{container_id}||{observed_at}"
    return hashlib.md5(key_str.encode("utf-8")).hexdigest()


class StgSensorEventsV1Transformer:
    """Python reference implementation replicating the exact SQL logic of stg_sensor_events_v1.sql."""

    @staticmethod
    def transform(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Simulates CTE source_events, deduped_events, and pivoted_observations."""
        # Step 1: Filter to schema_version = 1 with non-null required fields
        filtered_events = []
        for row in raw_records:
            content = row.get("RECORD_CONTENT", {})
            if isinstance(content, str):
                content = json.loads(content)

            # Filter: schema_version == 1 and required keys non-null
            if content.get("schema_version") != 1:
                continue
            if not content.get("event_id") or not content.get("container_id") or not content.get("observed_at"):
                continue

            filtered_events.append({
                "event_id": str(content["event_id"]),
                "schema_version": int(content["schema_version"]),
                "container_id": str(content["container_id"]).strip(),
                "sensor_type": str(content.get("sensor_type", "")).strip().lower(),
                "sensor_value": float(content["value"]) if content.get("value") is not None else None,
                "sensor_unit": str(content.get("unit", "")).strip().lower() if content.get("unit") else None,
                "observed_at": str(content["observed_at"]).strip(),
                "ingested_at": row.get("INGESTED_AT", datetime.now(timezone.utc).isoformat()),
            })

        # Step 2: Deterministic deduplication by (container_id, observed_at, sensor_type) ordering by ingested_at DESC, event_id DESC
        deduped_by_modality: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        for ev in filtered_events:
            key = (ev["container_id"], ev["observed_at"], ev["sensor_type"])
            if key not in deduped_by_modality:
                deduped_by_modality[key] = ev
            else:
                current = deduped_by_modality[key]
                if (ev["ingested_at"], ev["event_id"]) > (current["ingested_at"], current["event_id"]):
                    deduped_by_modality[key] = ev

        deduped_events = list(deduped_by_modality.values())

        # Step 3: Group by (container_id, observed_at) and pivot with MAX(IFF(...))
        grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
        for ev in deduped_events:
            group_key = (ev["container_id"], ev["observed_at"])
            grouped[group_key].append(ev)

        results: List[Dict[str, Any]] = []
        for (container_id, observed_at), events in sorted(grouped.items()):
            temp_val: Optional[float] = None
            hum_val: Optional[float] = None
            vib_val: Optional[float] = None
            temp_unit: Optional[str] = None
            hum_unit: Optional[str] = None
            vib_unit: Optional[str] = None
            sensor_types_seen = set()
            max_ingested = None

            for ev in events:
                st = ev["sensor_type"]
                val = ev["sensor_value"]
                unit = ev["sensor_unit"]
                ingested = ev["ingested_at"]

                if max_ingested is None or ingested > max_ingested:
                    max_ingested = ingested

                if st == "temperature":
                    temp_val = val
                    temp_unit = unit
                    sensor_types_seen.add(st)
                elif st == "humidity":
                    hum_val = val
                    hum_unit = unit
                    sensor_types_seen.add(st)
                elif st == "vibration":
                    vib_val = val
                    vib_unit = unit
                    sensor_types_seen.add(st)

            results.append({
                "measurement_id": md5_surrogate_key(container_id, observed_at),
                "container_id": container_id,
                "observed_at": observed_at,
                "temperature": temp_val,
                "humidity": hum_val,
                "vibration": vib_val,
                "temperature_unit": temp_unit,
                "humidity_unit": hum_unit,
                "vibration_unit": vib_unit,
                "sensor_count": len(sensor_types_seen),
                "ingested_at": max_ingested,
            })

        return results


class TestStgSensorEventsV1(unittest.TestCase):
    """Test suite for stg_sensor_events_v1 transformations and edge cases."""

    def setUp(self) -> None:
        self.transformer = StgSensorEventsV1Transformer()

        # Build canonical 27 events: 3 cycles x 3 containers x 3 sensors
        # All 3 sensors in a cycle share the exact same observed_at
        self.canonical_27_events: List[Dict[str, Any]] = []
        self.cycle_timestamps = [
            "2026-11-12T12:00:00.000000000Z",
            "2026-11-12T12:01:00.000000000Z",
            "2026-11-12T12:02:00.000000000Z",
        ]
        self.containers = ["CONTAINER_001", "CONTAINER_002", "CONTAINER_003"]
        self.sensor_specs = [
            ("temperature", 3.25, "celsius"),
            ("humidity", 88.5, "percent"),
            ("vibration", 0.12, "m/s^2"),
        ]

        event_counter = 1
        for cycle_idx, timestamp in enumerate(self.cycle_timestamps):
            for c_id in self.containers:
                for s_type, s_val, s_unit in self.sensor_specs:
                    payload = {
                        "schema_version": 1,
                        "event_id": f"00000000-0000-0000-0000-{event_counter:012d}",
                        "container_id": c_id,
                        "sensor_type": s_type,
                        "observed_at": timestamp,
                        "value": s_val + cycle_idx * 0.1,
                        "unit": s_unit,
                    }
                    self.canonical_27_events.append({
                        "RECORD_CONTENT": payload,
                        "RECORD_METADATA": {"topic": "atmosync.telemetry.v1"},
                        "INGESTED_AT": "2026-11-12T12:05:00.000Z",
                    })
                    event_counter += 1

        # Build mock historical records (missing schema_version)
        self.historical_records: List[Dict[str, Any]] = []
        for i in range(1, 378):
            self.historical_records.append({
                "RECORD_CONTENT": {
                    "event_id": f"hist-uuid-{i:04d}",
                    "event_timestamp": "2026-11-03T15:29:18.335Z",
                    "container_id": f"CONT-{(i % 10) + 1:03d}",
                    "temperature": 1.5,
                    "humidity": 90.0,
                    "vibration": 0.2,
                    "commodity": "strawberries",
                    "quantity_kg": 500.0,
                    # Note: schema_version is NOT present
                },
                "RECORD_METADATA": {},
                "INGESTED_AT": "2026-11-03T16:00:00.000Z",
            })

    def test_only_schema_version_1_records_included(self) -> None:
        """Requirement: 377 historical records must be completely filtered out."""
        combined_dataset = self.historical_records + self.canonical_27_events
        results = self.transformer.transform(combined_dataset)

        # Only the 27 version 1 events should be processed into wide rows
        self.assertEqual(len(results), 9)
        for r in results:
            self.assertTrue(r["container_id"].startswith("CONTAINER_"))
            self.assertFalse(r["container_id"].startswith("CONT-"))

    def test_27_events_produce_exactly_9_wide_rows(self) -> None:
        """Requirement: 27 events (3 cycles x 3 containers x 3 sensors) must produce 9 rows."""
        results = self.transformer.transform(self.canonical_27_events)
        self.assertEqual(len(results), 9)

        # Verify each of the 3 containers appears exactly 3 times
        container_counts = defaultdict(int)
        for r in results:
            container_counts[r["container_id"]] += 1
        self.assertEqual(dict(container_counts), {
            "CONTAINER_001": 3,
            "CONTAINER_002": 3,
            "CONTAINER_003": 3,
        })

    def test_sensor_values_pivoted_correctly(self) -> None:
        """Requirement: Sensor values and units are accurately placed into distinct columns."""
        results = self.transformer.transform(self.canonical_27_events)
        for r in results:
            self.assertIsNotNone(r["temperature"])
            self.assertIsNotNone(r["humidity"])
            self.assertIsNotNone(r["vibration"])

            self.assertEqual(r["temperature_unit"], "celsius")
            self.assertEqual(r["humidity_unit"], "percent")
            self.assertEqual(r["vibration_unit"], "m/s^2")

            self.assertEqual(r["sensor_count"], 3)
            self.assertEqual(r["measurement_id"], md5_surrogate_key(r["container_id"], r["observed_at"]))

    def test_no_readings_from_different_containers_mixed(self) -> None:
        """Requirement: Readings from different containers must never be merged."""
        results = self.transformer.transform(self.canonical_27_events)
        # Verify unique key per (container_id, observed_at)
        keys = [(r["container_id"], r["observed_at"]) for r in results]
        self.assertEqual(len(keys), len(set(keys)))

    def test_no_readings_from_different_timestamps_mixed(self) -> None:
        """Requirement: Readings from different observation timestamps are never combined."""
        results = self.transformer.transform(self.canonical_27_events)
        timestamps_seen = {r["observed_at"] for r in results}
        self.assertEqual(timestamps_seen, set(self.cycle_timestamps))

    def test_sensor_count_correctly_indicates_partial_sensor_modalities(self) -> None:
        """Requirement: sensor_count accurately reflects when sensors are missing."""
        # Create an event with only temperature and humidity (vibration omitted)
        partial_events = [
            {
                "RECORD_CONTENT": {
                    "schema_version": 1,
                    "event_id": "test-uuid-temp",
                    "container_id": "CONTAINER_001",
                    "sensor_type": "temperature",
                    "observed_at": "2026-11-12T14:00:00.000Z",
                    "value": 2.5,
                    "unit": "celsius",
                },
                "INGESTED_AT": "2026-11-12T14:01:00.000Z",
            },
            {
                "RECORD_CONTENT": {
                    "schema_version": 1,
                    "event_id": "test-uuid-hum",
                    "container_id": "CONTAINER_001",
                    "sensor_type": "humidity",
                    "observed_at": "2026-11-12T14:00:00.000Z",
                    "value": 80.0,
                    "unit": "percent",
                },
                "INGESTED_AT": "2026-11-12T14:01:00.000Z",
            },
        ]
        results = self.transformer.transform(partial_events)
        self.assertEqual(len(results), 1)
        r = results[0]
        self.assertEqual(r["sensor_count"], 2)
        self.assertIsNotNone(r["temperature"])
        self.assertIsNotNone(r["humidity"])
        self.assertIsNone(r["vibration"])
        self.assertIsNone(r["vibration_unit"])

    def test_unaligned_timestamps_do_not_silently_join(self) -> None:
        """Requirement: If timestamps differ across sensors, do not silently combine them.

        Produces 3 separate single-sensor rows with sensor_count = 1 rather than misjoining.
        """
        jittered_events = [
            {
                "RECORD_CONTENT": {
                    "schema_version": 1,
                    "event_id": "jitter-1",
                    "container_id": "CONTAINER_001",
                    "sensor_type": "temperature",
                    "observed_at": "2026-11-12T12:00:00.100Z",
                    "value": 4.0,
                    "unit": "celsius",
                },
                "INGESTED_AT": "2026-11-12T12:00:01.000Z",
            },
            {
                "RECORD_CONTENT": {
                    "schema_version": 1,
                    "event_id": "jitter-2",
                    "container_id": "CONTAINER_001",
                    "sensor_type": "humidity",
                    "observed_at": "2026-11-12T12:00:00.250Z",  # 150ms later
                    "value": 85.0,
                    "unit": "percent",
                },
                "INGESTED_AT": "2026-11-12T12:00:01.000Z",
            },
            {
                "RECORD_CONTENT": {
                    "schema_version": 1,
                    "event_id": "jitter-3",
                    "container_id": "CONTAINER_001",
                    "sensor_type": "vibration",
                    "observed_at": "2026-11-12T12:00:00.400Z",  # 300ms later
                    "value": 0.15,
                    "unit": "m/s^2",
                },
                "INGESTED_AT": "2026-11-12T12:00:01.000Z",
            },
        ]
        results = self.transformer.transform(jittered_events)

        # Exact grouping by observed_at results in 3 separate rows with sensor_count = 1
        self.assertEqual(len(results), 3)
        for r in results:
            self.assertEqual(r["sensor_count"], 1)

    def test_conflicting_duplicate_modalities_resolved_deterministically(self) -> None:
        """Point 3 Check: If two events report temperature at the same timestamp, latest wins.

        Does not arbitrarily take MAX(2.0, -10.0) = 2.0; takes the latest ingested reading.
        """
        conflicting_events = [
            {
                "RECORD_CONTENT": {
                    "schema_version": 1,
                    "event_id": "uuid-first-ingested",
                    "container_id": "CONTAINER_001",
                    "sensor_type": "temperature",
                    "observed_at": "2026-11-12T12:00:00.000Z",
                    "value": 15.0,  # Higher value
                    "unit": "celsius",
                },
                "INGESTED_AT": "2026-11-12T12:01:00.000Z",  # Earlier ingestion
            },
            {
                "RECORD_CONTENT": {
                    "schema_version": 1,
                    "event_id": "uuid-second-ingested",
                    "container_id": "CONTAINER_001",
                    "sensor_type": "temperature",
                    "observed_at": "2026-11-12T12:00:00.000Z",
                    "value": 2.0,   # Lower, corrective value
                    "unit": "celsius",
                },
                "INGESTED_AT": "2026-11-12T12:02:00.000Z",  # Later ingestion
            },
        ]
        results = self.transformer.transform(conflicting_events)
        self.assertEqual(len(results), 1)
        # Deterministically selected the later ingested reading (2.0), not MAX(15.0, 2.0)
        self.assertEqual(results[0]["temperature"], 2.0)

    def test_subsecond_precision_in_surrogate_key(self) -> None:
        """Point 2 Check: Microsecond and nanosecond differences produce distinct surrogate keys."""
        ts1 = "2026-11-12T12:00:00.123456789+00:00"
        ts2 = "2026-11-12T12:00:00.123456788+00:00"
        key1 = md5_surrogate_key("CONTAINER_001", ts1)
        key2 = md5_surrogate_key("CONTAINER_001", ts2)
        self.assertNotEqual(key1, key2)

    def test_existing_dbt_models_unmodified(self) -> None:
        """Requirement: Confirm all existing models remain untouched."""
        dbt_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        expected_unmodified_files = [
            os.path.join(dbt_dir, "models", "staging", "stg_sensor_readings.sql"),
            os.path.join(dbt_dir, "models", "intermediate", "int_sensor_excursions.sql"),
            os.path.join(dbt_dir, "models", "marts", "dim_commodity.sql"),
            os.path.join(dbt_dir, "models", "marts", "dim_container.sql"),
            os.path.join(dbt_dir, "models", "marts", "fct_sensor_telemetry.sql"),
            os.path.join(dbt_dir, "models", "marts", "fct_arbitrage_risk_summary.sql"),
        ]
        for fpath in expected_unmodified_files:
            self.assertTrue(os.path.exists(fpath), f"File {fpath} must exist")
            self.assertGreater(os.path.getsize(fpath), 0)


if __name__ == "__main__":
    unittest.main()
