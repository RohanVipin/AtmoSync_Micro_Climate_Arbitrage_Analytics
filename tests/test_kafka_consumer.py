"""Unit tests for AtmoSync Kafka-to-Snowflake Ingestion Consumer.

Validates:
1. Canonical 7-field Version-1 event schema and rejection of non-conforming payloads.
2. RECORD_METADATA extraction from Kafka messages.
3. Idempotent MERGE SQL generation and execution against Snowflake.
4. Transactional commit order: Snowflake commit precedes Kafka offset commit.
5. Error handling: Snowflake failure rolls back and prevents Kafka offset commit.
6. Processing statistics: Tracking polled, valid, malformed, inserted, and duplicates.
"""

from __future__ import annotations

import json
import math
import os
import sys
import unittest
from unittest.mock import MagicMock, call, patch
import uuid

# Ensure dbt_atmosync scripts folder is in sys.path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(os.path.dirname(CURRENT_DIR), "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from kafka_to_snowflake_consumer import (  # noqa: E402
    CanonicalEventValidator,
    EventValidationError,
    IngestionStats,
    KafkaToSnowflakeConsumer,
    SnowflakeConfig,
    SnowflakeIngestionClient,
    extract_message_metadata,
)


class MockKafkaMessage:
    """Mock implementation of confluent_kafka Message for offline unit testing."""

    def __init__(
        self,
        value: bytes | str | None,
        key: bytes | str | None = b"CONTAINER_001",
        topic: str = "atmosync.telemetry.v1",
        partition: int = 0,
        offset: int = 100,
        timestamp: tuple[int, int] | None = (1, 1731415009000),
        error: Any | None = None,
    ) -> None:
        self._value = value.encode("utf-8") if isinstance(value, str) else value
        self._key = key.encode("utf-8") if isinstance(key, str) else key
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._timestamp = timestamp
        self._error = error

    def value(self) -> bytes | None:
        return self._value

    def key(self) -> bytes | None:
        return self._key

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def timestamp(self) -> tuple[int, int] | None:
        return self._timestamp

    def error(self) -> Any | None:
        return self._error


def create_sample_event(
    sensor_type: str = "temperature",
    value: float = 3.29,
    unit: str = "celsius",
    observed_at: str = "2026-11-12T12:36:49.626Z",
    container_id: str = "CONTAINER_001",
    event_id: str | None = None,
) -> dict[str, Any]:
    """Helper creating a canonical 7-field event dictionary."""
    return {
        "schema_version": 1,
        "event_id": event_id or str(uuid.uuid4()),
        "container_id": container_id,
        "sensor_type": sensor_type,
        "observed_at": observed_at,
        "value": value,
        "unit": unit,
    }


class TestCanonicalEventValidator(unittest.TestCase):
    """Test suite for strict canonical 7-field schema validation."""

    def test_valid_temperature_event(self) -> None:
        event = create_sample_event("temperature", 4.15, "celsius")
        is_valid, err = CanonicalEventValidator.is_valid(event)
        self.assertTrue(is_valid)
        self.assertIsNone(err)

    def test_valid_humidity_event(self) -> None:
        event = create_sample_event("humidity", 85.2, "percent")
        is_valid, err = CanonicalEventValidator.is_valid(event)
        self.assertTrue(is_valid)
        self.assertIsNone(err)

    def test_valid_vibration_event(self) -> None:
        event = create_sample_event("vibration", 0.125, "m/s^2")
        is_valid, err = CanonicalEventValidator.is_valid(event)
        self.assertTrue(is_valid)
        self.assertIsNone(err)

    def test_missing_required_field_rejected(self) -> None:
        event = create_sample_event()
        del event["unit"]
        is_valid, err = CanonicalEventValidator.is_valid(event)
        self.assertFalse(is_valid)
        self.assertIn("Missing required field(s)", err or "")

    def test_extra_unexpected_field_rejected(self) -> None:
        # Simulates older 10-field payload with latitude/longitude/commodity
        event = create_sample_event()
        event["commodity"] = "strawberries"
        event["latitude"] = 12.34
        is_valid, err = CanonicalEventValidator.is_valid(event)
        self.assertFalse(is_valid)
        self.assertIn("Unexpected field(s)", err or "")

    def test_invalid_schema_version_rejected(self) -> None:
        event = create_sample_event()
        event["schema_version"] = 2
        is_valid, err = CanonicalEventValidator.is_valid(event)
        self.assertFalse(is_valid)
        self.assertIn("Unsupported 'schema_version'", err or "")

        event["schema_version"] = "1"
        is_valid, err = CanonicalEventValidator.is_valid(event)
        self.assertFalse(is_valid)

        event["schema_version"] = True
        is_valid, err = CanonicalEventValidator.is_valid(event)
        self.assertFalse(is_valid)

    def test_invalid_event_id_rejected(self) -> None:
        event = create_sample_event()
        event["event_id"] = "not-a-valid-uuid"
        is_valid, err = CanonicalEventValidator.is_valid(event)
        self.assertFalse(is_valid)
        self.assertIn("Invalid UUID", err or "")

        # UUIDv1 rejected (must be UUIDv4)
        v1_uuid = str(uuid.uuid1())
        event["event_id"] = v1_uuid
        is_valid, err = CanonicalEventValidator.is_valid(event)
        self.assertFalse(is_valid)
        self.assertIn("UUID version 4", err or "")

    def test_blank_container_id_rejected(self) -> None:
        event = create_sample_event(container_id="   ")
        is_valid, err = CanonicalEventValidator.is_valid(event)
        self.assertFalse(is_valid)
        self.assertIn("container_id", err or "")

    def test_unsupported_sensor_type_rejected(self) -> None:
        event = create_sample_event(sensor_type="atmospheric_pressure")
        is_valid, err = CanonicalEventValidator.is_valid(event)
        self.assertFalse(is_valid)
        self.assertIn("sensor_type", err or "")

    def test_naive_timestamp_rejected(self) -> None:
        # Naive timestamp without timezone
        event = create_sample_event(observed_at="2026-11-12T12:36:49.626")
        is_valid, err = CanonicalEventValidator.is_valid(event)
        self.assertFalse(is_valid)
        self.assertIn("naive timestamp", err or "")

    def test_non_utc_timestamp_rejected(self) -> None:
        # Non-UTC timezone offset (+05:30)
        event = create_sample_event(observed_at="2026-11-12T12:36:49.626+05:30")
        is_valid, err = CanonicalEventValidator.is_valid(event)
        self.assertFalse(is_valid)
        self.assertIn("UTC offset", err or "")

    def test_non_finite_value_rejected(self) -> None:
        event = create_sample_event(value=float("nan"))
        is_valid, err = CanonicalEventValidator.is_valid(event)
        self.assertFalse(is_valid)
        self.assertIn("finite number", err or "")

        event = create_sample_event(value=float("inf"))
        is_valid, err = CanonicalEventValidator.is_valid(event)
        self.assertFalse(is_valid)

        event = create_sample_event(value=True)  # type: ignore[arg-type]
        is_valid, err = CanonicalEventValidator.is_valid(event)
        self.assertFalse(is_valid)

    def test_blank_unit_rejected(self) -> None:
        event = create_sample_event(unit="   ")
        is_valid, err = CanonicalEventValidator.is_valid(event)
        self.assertFalse(is_valid)
        self.assertIn("unit", err or "")


class TestMessageMetadataExtraction(unittest.TestCase):
    """Test suite for extracting Kafka RECORD_METADATA."""

    def test_extract_metadata_full(self) -> None:
        msg = MockKafkaMessage(
            value=b"{}",
            key=b"CONTAINER_001",
            topic="atmosync.telemetry.v1",
            partition=2,
            offset=42,
            timestamp=(1, 1731415009123),
        )
        meta = extract_message_metadata(msg)
        self.assertEqual(meta["topic"], "atmosync.telemetry.v1")
        self.assertEqual(meta["partition"], 2)
        self.assertEqual(meta["offset"], 42)
        self.assertEqual(meta["key"], "CONTAINER_001")
        self.assertEqual(meta["timestamp"], 1731415009123)
        self.assertEqual(meta["timestamp_type"], 1)

    def test_extract_metadata_none_key_and_timestamp(self) -> None:
        msg = MockKafkaMessage(value=b"{}", key=None, timestamp=None)
        meta = extract_message_metadata(msg)
        self.assertIsNone(meta["key"])
        self.assertIsNone(meta["timestamp"])
        self.assertIsNone(meta["timestamp_type"])


class TestSnowflakeIngestionClient(unittest.TestCase):
    """Test suite for Snowflake client and idempotent MERGE SQL execution."""

    def setUp(self) -> None:
        self.config = SnowflakeConfig(
            account="test_account",
            user="test_user",
            password="test_password",
            role="ACCOUNTADMIN",
            warehouse="COMPUTE_WH",
            database="ATMOSYNC_DB",
            schema="RAW",
        )
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value = self.mock_cursor
        self.client = SnowflakeIngestionClient(self.config, connection=self.mock_conn)
        self.client.connect()

    def test_password_redacted_in_repr(self) -> None:
        repr_str = repr(self.config)
        self.assertNotIn("test_password", repr_str)
        self.assertIn("***REDACTED***", repr_str)

    def test_merge_sql_format(self) -> None:
        sql = self.client.get_merge_sql()
        self.assertIn("MERGE INTO ATMOSYNC_DB.RAW.CONTAINER_TELEMETRY_RAW", sql)
        self.assertIn("ON target.RECORD_CONTENT:event_id::VARCHAR = source.record_content:event_id::VARCHAR", sql)
        self.assertIn("WHEN NOT MATCHED THEN", sql)
        self.assertIn("INSERT (RECORD_METADATA, RECORD_CONTENT)", sql)
        # Verify INGESTED_AT is omitted so default timestamp applies
        self.assertNotIn("INGESTED_AT", sql)

    def test_merge_record_inserted(self) -> None:
        # Mock cursor returning (1, 0) indicating 1 row inserted
        self.mock_cursor.fetchone.return_value = (1, 0)

        meta = {"topic": "atmosync.telemetry.v1", "offset": 1}
        content = create_sample_event()

        inserted = self.client.merge_record(meta, content)
        self.assertTrue(inserted)
        self.mock_cursor.execute.assert_called_once()

    def test_merge_record_duplicate_skipped(self) -> None:
        # Mock cursor returning (0, 0) indicating duplicate skipped
        self.mock_cursor.fetchone.return_value = (0, 0)

        meta = {"topic": "atmosync.telemetry.v1", "offset": 2}
        content = create_sample_event()

        inserted = self.client.merge_record(meta, content)
        self.assertFalse(inserted)

    def test_commit_and_rollback(self) -> None:
        self.client.commit()
        self.mock_conn.commit.assert_called_once()

        self.client.rollback()
        self.mock_conn.rollback.assert_called_once()


class TestKafkaToSnowflakeConsumer(unittest.TestCase):
    """Test suite for consumer coordination, batching, commits, and error boundaries."""

    def setUp(self) -> None:
        self.mock_conn = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_conn.cursor.return_value = self.mock_cursor

        self.config = SnowflakeConfig(
            account="test_account",
            user="test_user",
            password="test_password",
        )
        self.sf_client = SnowflakeIngestionClient(self.config, connection=self.mock_conn)
        self.sf_client.connect()

        self.mock_consumer = MagicMock()
        self.consumer_engine = KafkaToSnowflakeConsumer(
            bootstrap_servers="localhost:9092",
            group_id="atmosync-test-group",
            topic="atmosync.telemetry.v1",
            snowflake_client=self.sf_client,
            consumer=self.mock_consumer,
        )

    def test_27_messages_end_to_end_ingestion(self) -> None:
        """Simulates Task A publishing 27 messages (3 cycles x 3 containers x 3 sensors)."""
        messages = []
        sensor_modalities = [
            ("temperature", 4.1, "celsius"),
            ("humidity", 85.0, "percent"),
            ("vibration", 0.15, "m/s^2"),
        ]

        count = 0
        for cycle in range(3):
            for c_idx in range(1, 4):
                container_id = f"CONTAINER_{c_idx:03d}"
                for sensor_type, val, unit in sensor_modalities:
                    payload = create_sample_event(
                        sensor_type=sensor_type,
                        value=val,
                        unit=unit,
                        container_id=container_id,
                    )
                    msg = MockKafkaMessage(
                        value=json.dumps(payload),
                        key=container_id,
                        partition=c_idx - 1,
                        offset=count,
                    )
                    messages.append(msg)
                    count += 1

        self.assertEqual(len(messages), 27)

        # Mock consumer poll to return messages one by one
        self.mock_consumer.poll.side_effect = list(messages) + [None, None]
        # Mock Snowflake cursor returning 1 row inserted for each record
        self.mock_cursor.fetchone.return_value = (1, 0)

        # Run consumer bounded by 27 messages
        stats = self.consumer_engine.run(max_messages=27, batch_size=10, poll_timeout=0.1)

        self.assertEqual(stats.messages_valid, 27)
        self.assertEqual(stats.rows_inserted, 27)
        self.assertEqual(stats.messages_malformed, 0)
        self.assertEqual(stats.duplicates_skipped, 0)
        self.assertEqual(stats.snowflake_errors, 0)

        # Verify Snowflake committed and Kafka offsets were committed
        self.assertTrue(self.mock_conn.commit.called)
        self.assertTrue(self.mock_consumer.commit.called)

    def test_mixed_messages_across_all_three_partitions(self) -> None:
        """Test 1: Verifies mixed messages across all 3 Kafka partitions are tracked and committed correctly."""
        p0_msg1 = MockKafkaMessage(value=json.dumps(create_sample_event()), partition=0, offset=10)
        p0_msg2 = MockKafkaMessage(value=json.dumps(create_sample_event()), partition=0, offset=11)
        p1_msg1 = MockKafkaMessage(value=json.dumps(create_sample_event()), partition=1, offset=20)
        p1_msg2 = MockKafkaMessage(value=json.dumps(create_sample_event()), partition=1, offset=25)
        p2_msg1 = MockKafkaMessage(value=json.dumps(create_sample_event()), partition=2, offset=5)

        batch = [
            ({"topic": "atmosync.telemetry.v1", "partition": 0, "offset": 10}, {}, p0_msg1),
            ({"topic": "atmosync.telemetry.v1", "partition": 0, "offset": 11}, {}, p0_msg2),
            ({"topic": "atmosync.telemetry.v1", "partition": 1, "offset": 20}, {}, p1_msg1),
            ({"topic": "atmosync.telemetry.v1", "partition": 1, "offset": 25}, {}, p1_msg2),
            ({"topic": "atmosync.telemetry.v1", "partition": 2, "offset": 5}, {}, p2_msg1),
        ]
        self.mock_cursor.fetchone.return_value = (1, 0)

        success = self.consumer_engine.ingest_batch(batch)
        self.assertTrue(success)

        # Verify consumer.commit was called with TopicPartition for all 3 partitions with offset + 1
        self.mock_consumer.commit.assert_called_once()
        commit_kwargs = self.mock_consumer.commit.call_args[1]
        committed_offsets = commit_kwargs.get("offsets", [])

        # Sort and inspect committed partitions
        offsets_map = {tp.partition: tp.offset for tp in committed_offsets}
        self.assertEqual(offsets_map.get(0), 12)  # max 11 + 1
        self.assertEqual(offsets_map.get(1), 26)  # max 25 + 1
        self.assertEqual(offsets_map.get(2), 6)   # max 5 + 1

    def test_successful_snowflake_transaction_commits_offsets_for_all_represented_partitions(self) -> None:
        """Test 2: Verifies Snowflake commit precedes Kafka offset commit and commits all partitions."""
        call_order = []
        self.mock_conn.commit.side_effect = lambda: call_order.append("snowflake_commit")
        self.mock_consumer.commit.side_effect = lambda **kwargs: call_order.append("kafka_commit")

        p0_msg = MockKafkaMessage(value=json.dumps(create_sample_event()), partition=0, offset=1)
        p1_msg = MockKafkaMessage(value=json.dumps(create_sample_event()), partition=1, offset=2)
        batch = [
            ({"topic": "atmosync.telemetry.v1", "partition": 0, "offset": 1}, {}, p0_msg),
            ({"topic": "atmosync.telemetry.v1", "partition": 1, "offset": 2}, {}, p1_msg),
        ]
        self.mock_cursor.fetchone.return_value = (1, 0)

        success = self.consumer_engine.ingest_batch(batch)
        self.assertTrue(success)

        # Ordering check: Snowflake commit must strictly precede Kafka commit
        self.assertEqual(call_order, ["snowflake_commit", "kafka_commit"])

    def test_snowflake_failure_commits_no_kafka_offsets(self) -> None:
        """Test 3: If Snowflake ingestion fails, no Kafka offsets are committed and rollback is executed."""
        payload = create_sample_event()
        msg = MockKafkaMessage(value=json.dumps(payload), partition=0, offset=10)

        # Simulate Snowflake warehouse error
        self.mock_cursor.execute.side_effect = Exception("Snowflake Warehouse Timeout")

        batch = [({"topic": "atmosync.telemetry.v1", "partition": 0, "offset": 10}, payload, msg)]
        success = self.consumer_engine.ingest_batch(batch)

        self.assertFalse(success)
        self.mock_conn.rollback.assert_called_once()
        self.mock_consumer.commit.assert_not_called()
        self.assertEqual(self.consumer_engine.stats.snowflake_errors, 1)

    def test_malformed_message_does_not_advance_past_pending_valid_messages(self) -> None:
        """Test 4: Malformed message halts processing without committing offsets, preserving pending valid messages."""
        valid_payload = create_sample_event()
        valid_msg = MockKafkaMessage(value=json.dumps(valid_payload), partition=0, offset=1)
        bad_msg = MockKafkaMessage(value=b"INVALID_NON_JSON_CORRUPT_PAYLOAD", partition=0, offset=2)

        self.mock_consumer.poll.side_effect = [valid_msg, bad_msg, None]
        self.mock_cursor.fetchone.return_value = (1, 0)

        # Run consumer bounded by 10 messages with batch_size=5 (so valid_msg is pending in current_batch)
        stats = self.consumer_engine.run(max_messages=10, batch_size=5, poll_timeout=0.1)

        # Consumer must have caught the malformed message and halted without committing offsets
        self.assertEqual(stats.messages_valid, 1)
        self.assertEqual(stats.messages_malformed, 1)
        self.mock_consumer.commit.assert_not_called()

    def test_duplicate_event_ids_remain_safe_through_merge(self) -> None:
        """Test 5: Duplicate event IDs are safely deduplicated by MERGE without failing."""
        payload = create_sample_event()
        msg = MockKafkaMessage(value=json.dumps(payload), partition=1, offset=5)

        # Mock cursor fetchone returning (0, 0) indicating duplicate skipped
        self.mock_cursor.fetchone.return_value = (0, 0)

        batch = [({"topic": "atmosync.telemetry.v1", "partition": 1, "offset": 5}, payload, msg)]
        success = self.consumer_engine.ingest_batch(batch)

        self.assertTrue(success)
        self.assertEqual(self.consumer_engine.stats.rows_inserted, 0)
        self.assertEqual(self.consumer_engine.stats.duplicates_skipped, 1)
        self.mock_conn.commit.assert_called_once()
        self.mock_consumer.commit.assert_called_once()

    def test_existing_raw_table_records_remain_untouched(self) -> None:
        """Test 6: Validates that MERGE query contains no destructive operations (no DROP/TRUNCATE/UPDATE/DELETE)."""
        merge_sql = self.sf_client.get_merge_sql()

        # Must be an idempotent MERGE
        self.assertTrue(merge_sql.startswith("MERGE INTO"))
        self.assertIn("WHEN NOT MATCHED THEN", merge_sql)
        self.assertIn("INSERT (RECORD_METADATA, RECORD_CONTENT)", merge_sql)

        # Destructive operations must be completely absent
        self.assertNotIn("WHEN MATCHED", merge_sql)
        self.assertNotIn("DROP", merge_sql)
        self.assertNotIn("TRUNCATE", merge_sql)
        self.assertNotIn("DELETE", merge_sql)
        self.assertNotIn("UPDATE", merge_sql)


if __name__ == "__main__":
    unittest.main()
