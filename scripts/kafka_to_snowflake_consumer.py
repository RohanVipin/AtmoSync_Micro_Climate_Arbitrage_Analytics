#!/usr/bin/env python3
"""AtmoSync Kafka-to-Snowflake Ingestion Consumer.

Consumes canonical Version-1 sensor telemetry events from Apache Kafka and
loads them into Snowflake RAW (ATMOSYNC_DB.RAW.CONTAINER_TELEMETRY_RAW) using
idempotent MERGE operations.

Key Guarantees:
- Validates the strict 7-field canonical Version-1 event schema.
- Preserves original JSON payloads without alteration in RECORD_CONTENT.
- Populates RECORD_METADATA with Kafka topic, partition, offset, key, and timestamp.
- Relies on Snowflake table defaults for INGESTED_AT.
- Idempotent: Uses MERGE on RECORD_CONTENT:event_id to prevent duplicates.
- Atomicity: Commits Snowflake transaction BEFORE committing Kafka offset.
- Offset Safety: Disables auto-commit; never commits offsets for failed Snowflake loads.
- Security: Never logs passwords or sensitive credentials.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json
import logging
import math
import os
import signal
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple
import uuid

# Third-party imports handled with fallback for unit test mocking
try:
    import confluent_kafka
    from confluent_kafka import Consumer, KafkaError, KafkaException, Message, TopicPartition
except ImportError:  # pragma: no cover
    confluent_kafka = None
    Consumer = Any  # type: ignore[assignment,misc]
    KafkaError = Any  # type: ignore[assignment,misc]
    KafkaException = Exception  # type: ignore[assignment,misc]
    Message = Any  # type: ignore[assignment,misc]

    class TopicPartition:  # type: ignore[no-redef]
        """Fallback TopicPartition representation when confluent-kafka is mocked."""

        def __init__(self, topic: str, partition: int, offset: int = -1001) -> None:
            self.topic = topic
            self.partition = partition
            self.offset = offset

        def __eq__(self, other: Any) -> bool:
            return (
                isinstance(other, TopicPartition)
                and self.topic == other.topic
                and self.partition == other.partition
                and self.offset == other.offset
            )

        def __repr__(self) -> str:
            return f"TopicPartition(topic='{self.topic}', partition={self.partition}, offset={self.offset})"

try:
    import snowflake.connector
    from snowflake.connector.errors import DatabaseError, Error as SnowflakeError
except ImportError:  # pragma: no cover
    snowflake = None  # type: ignore[assignment]
    DatabaseError = Exception  # type: ignore[assignment,misc]
    SnowflakeError = Exception  # type: ignore[assignment,misc]

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("atmosync.kafka_consumer")


# ---------------------------------------------------------------------------
# Canonical Schema Definition & Validation (Task A Contract)
# ---------------------------------------------------------------------------

SCHEMA_VERSION: int = 1
SUPPORTED_SENSOR_TYPES: frozenset[str] = frozenset({"temperature", "humidity", "vibration"})
CANONICAL_FIELD_NAMES: frozenset[str] = frozenset({
    "schema_version",
    "event_id",
    "container_id",
    "sensor_type",
    "observed_at",
    "value",
    "unit",
})


class EventValidationError(ValueError):
    """Raised when an event payload fails validation against the canonical contract."""
    pass


class CanonicalEventValidator:
    """Validates incoming messages against the strict 7-field Version-1 contract."""

    @classmethod
    def validate(cls, data: Any) -> None:
        """Validate an event dictionary against the strict Version-1 contract.

        Args:
            data: Decoded JSON dictionary.

        Raises:
            EventValidationError: If any rule from the canonical contract is violated.
        """
        if not isinstance(data, dict):
            raise EventValidationError(f"Expected JSON object, got {type(data).__name__}")

        actual_keys = set(data.keys())

        # 1. Exact 7-field validation (reject missing or unexpected fields)
        missing = CANONICAL_FIELD_NAMES - actual_keys
        if missing:
            raise EventValidationError(f"Missing required field(s): {', '.join(sorted(missing))}")

        unexpected = actual_keys - CANONICAL_FIELD_NAMES
        if unexpected:
            raise EventValidationError(f"Unexpected field(s): {', '.join(sorted(unexpected))}")

        # 2. schema_version
        version = data["schema_version"]
        if isinstance(version, bool) or not isinstance(version, int):
            raise EventValidationError(f"'schema_version' must be integer, got {type(version).__name__}")
        if version != SCHEMA_VERSION:
            raise EventValidationError(f"Unsupported 'schema_version': expected {SCHEMA_VERSION}, got {version}")

        # 3. event_id (UUIDv4)
        event_id = data["event_id"]
        if not isinstance(event_id, str) or not event_id.strip():
            raise EventValidationError(f"'event_id' must be non-empty string, got {repr(event_id)}")
        clean_id = event_id.strip()
        try:
            parsed_uuid = uuid.UUID(clean_id)
            if str(parsed_uuid) != clean_id.lower():
                raise EventValidationError(f"'event_id' is not a canonical UUID string: {repr(event_id)}")
            if parsed_uuid.version != 4:
                raise EventValidationError(f"'event_id' must be UUID version 4, got {parsed_uuid.version}")
        except (ValueError, AttributeError) as exc:
            raise EventValidationError(f"Invalid UUID in 'event_id': {exc}") from exc

        # 4. container_id
        container_id = data["container_id"]
        if not isinstance(container_id, str) or not container_id.strip():
            raise EventValidationError(f"'container_id' must be non-empty string, got {repr(container_id)}")

        # 5. sensor_type
        sensor_type = data["sensor_type"]
        if not isinstance(sensor_type, str) or sensor_type not in SUPPORTED_SENSOR_TYPES:
            raise EventValidationError(
                f"'sensor_type' must be one of {sorted(list(SUPPORTED_SENSOR_TYPES))}, got {repr(sensor_type)}"
            )

        # 6. observed_at (UTC ISO-8601 with millisecond precision)
        observed_at = data["observed_at"]
        if not isinstance(observed_at, str) or not observed_at.strip():
            raise EventValidationError(f"'observed_at' must be non-empty string, got {repr(observed_at)}")
        ts_str = observed_at.strip()
        normalized_ts = ts_str.replace("Z", "+00:00") if ts_str.endswith("Z") else ts_str
        try:
            parsed_dt = datetime.fromisoformat(normalized_ts)
        except ValueError as exc:
            raise EventValidationError(f"'observed_at' is not a valid ISO-8601 timestamp: {exc}") from exc

        if parsed_dt.tzinfo is None:
            raise EventValidationError(f"'observed_at' must be timezone-aware UTC, got naive timestamp: {repr(observed_at)}")

        utcoffset = parsed_dt.utcoffset()
        if utcoffset is None or utcoffset != timedelta(0):
            raise EventValidationError(f"'observed_at' must have UTC offset (+00:00 or 'Z'), got {utcoffset}")

        # 7. value (finite numeric, rejecting bool, NaN, Inf)
        val = data["value"]
        if isinstance(val, bool) or not isinstance(val, (int, float)):
            raise EventValidationError(f"'value' must be numeric (int/float), got {type(val).__name__}")
        if not math.isfinite(val):
            raise EventValidationError(f"'value' must be a finite number, got {repr(val)}")

        # 8. unit
        unit = data["unit"]
        if not isinstance(unit, str) or not unit.strip():
            raise EventValidationError(f"'unit' must be non-empty string, got {repr(unit)}")

    @classmethod
    def is_valid(cls, data: Any) -> Tuple[bool, Optional[str]]:
        """Validate without throwing an unhandled exception.

        Returns:
            Tuple of (is_valid, error_message).
        """
        try:
            cls.validate(data)
            return True, None
        except EventValidationError as exc:
            return False, str(exc)


# ---------------------------------------------------------------------------
# Metadata Builder
# ---------------------------------------------------------------------------

def extract_message_metadata(msg: Any) -> Dict[str, Any]:
    """Construct RECORD_METADATA dictionary from a confluent_kafka Message.

    Captures:
    - topic
    - partition
    - offset
    - message key (UTF-8 decoded string, or None)
    - timestamp (Kafka timestamp in ms, or None)
    - timestamp_type (Kafka timestamp type integer, or None)

    Args:
        msg: confluent_kafka Message instance (or test mock).

    Returns:
        JSON-serializable metadata dictionary.
    """
    key_bytes = msg.key() if hasattr(msg, "key") and callable(msg.key) else None
    key_str: Optional[str] = None
    if key_bytes is not None:
        if isinstance(key_bytes, bytes):
            key_str = key_bytes.decode("utf-8", errors="replace")
        else:
            key_str = str(key_bytes)

    timestamp_val: Optional[int] = None
    timestamp_type: Optional[int] = None
    if hasattr(msg, "timestamp") and callable(msg.timestamp):
        try:
            ts_res = msg.timestamp()
            if isinstance(ts_res, tuple) and len(ts_res) == 2:
                timestamp_type, timestamp_val = ts_res
        except Exception:  # pragma: no cover
            pass

    return {
        "topic": msg.topic() if hasattr(msg, "topic") and callable(msg.topic) else "unknown",
        "partition": msg.partition() if hasattr(msg, "partition") and callable(msg.partition) else 0,
        "offset": msg.offset() if hasattr(msg, "offset") and callable(msg.offset) else -1,
        "key": key_str,
        "timestamp": timestamp_val,
        "timestamp_type": timestamp_type,
    }


# ---------------------------------------------------------------------------
# Snowflake Database Client & Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SnowflakeConfig:
    """Snowflake connection parameters loaded from environment variables."""

    account: str
    user: str
    password: str
    role: str = "ACCOUNTADMIN"
    warehouse: str = "COMPUTE_WH"
    database: str = "ATMOSYNC_DB"
    schema: str = "RAW"

    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        """Load configuration from environment variables safely.

        Raises:
            ValueError: If mandatory credentials (account, user, password) are missing.
        """
        account = os.getenv("SNOWFLAKE_ACCOUNT")
        user = os.getenv("SNOWFLAKE_USER")
        password = os.getenv("SNOWFLAKE_PASSWORD")

        missing = []
        if not account:
            missing.append("SNOWFLAKE_ACCOUNT")
        if not user:
            missing.append("SNOWFLAKE_USER")
        if not password:
            missing.append("SNOWFLAKE_PASSWORD")

        if missing:
            raise ValueError(
                f"Missing required Snowflake environment variable(s): {', '.join(missing)}. "
                "Ensure these are set in your environment before running the consumer."
            )

        return cls(
            account=account,
            user=user,
            password=password,
            role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            database=os.getenv("SNOWFLAKE_DATABASE", "ATMOSYNC_DB"),
            schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
        )

    def __repr__(self) -> str:
        """Safe representation with password masked."""
        return (
            f"SnowflakeConfig(account={repr(self.account)}, user={repr(self.user)}, "
            f"password='***REDACTED***', role={repr(self.role)}, "
            f"warehouse={repr(self.warehouse)}, database={repr(self.database)}, "
            f"schema={repr(self.schema)})"
        )


class SnowflakeIngestionClient:
    """Manages transactional Snowflake operations for RAW telemetry ingestion."""

    def __init__(self, config: SnowflakeConfig, connection: Optional[Any] = None) -> None:
        """Initialize the Snowflake client with autocommit disabled.

        Args:
            config: Validated Snowflake configuration.
            connection: Optional pre-configured or mocked connection instance.
        """
        self.config = config
        self._custom_connection = connection
        self._connection: Optional[Any] = None
        self._cursor: Optional[Any] = None

    def connect(self) -> None:
        """Establish connection with autocommit=False to support transaction boundaries."""
        if self._custom_connection is not None:
            self._connection = self._custom_connection
        else:
            if snowflake is None:
                raise RuntimeError(
                    "snowflake-connector-python is not installed. "
                    "Install it using: pip install snowflake-connector-python==5.2.0"
                )
            logger.info(
                "Connecting to Snowflake account=%s database=%s schema=%s warehouse=%s role=%s",
                self.config.account,
                self.config.database,
                self.config.schema,
                self.config.warehouse,
                self.config.role,
            )
            # Explicitly disable autocommit to allow explicit transaction management
            self._connection = snowflake.connector.connect(
                account=self.config.account,
                user=self.config.user,
                password=self.config.password,
                role=self.config.role,
                warehouse=self.config.warehouse,
                database=self.config.database,
                schema=self.config.schema,
                autocommit=False,
            )

        self._cursor = self._connection.cursor()

    @property
    def target_table(self) -> str:
        """Return the fully qualified target table name."""
        return f"{self.config.database}.{self.config.schema}.CONTAINER_TELEMETRY_RAW"

    def get_merge_sql(self) -> str:
        """Generate the idempotent Snowflake MERGE SQL statement.

        Deduplication rule:
        Matches target.RECORD_CONTENT:event_id::VARCHAR with source.record_content:event_id::VARCHAR.
        If NOT MATCHED, inserts RECORD_METADATA and RECORD_CONTENT.
        INGESTED_AT is omitted so Snowflake applies its default (CURRENT_TIMESTAMP()).
        """
        return f"""
MERGE INTO {self.target_table} AS target
USING (
    SELECT 
        PARSE_JSON(%s) AS record_metadata,
        PARSE_JSON(%s) AS record_content
) AS source
ON target.RECORD_CONTENT:event_id::VARCHAR = source.record_content:event_id::VARCHAR
WHEN NOT MATCHED THEN
    INSERT (RECORD_METADATA, RECORD_CONTENT)
    VALUES (source.record_metadata, source.record_content)
""".strip()

    def merge_record(self, metadata: Dict[str, Any], content: Dict[str, Any]) -> bool:
        """Execute MERGE for a single validated record.

        Args:
            metadata: Kafka metadata dictionary.
            content: Canonical event dictionary.

        Returns:
            True if a new row was inserted, False if record already existed (duplicate).

        Raises:
            Exception: If the query execution fails.
        """
        if self._cursor is None:
            raise RuntimeError("Snowflake client is not connected. Call connect() first.")

        metadata_json = json.dumps(metadata)
        content_json = json.dumps(content)

        sql = self.get_merge_sql()
        self._cursor.execute(sql, (metadata_json, content_json))

        # Snowflake MERGE returns (number of rows inserted, number of rows updated)
        row = self._cursor.fetchone()
        inserted_count = row[0] if row and len(row) > 0 else 0
        return inserted_count > 0

    def commit(self) -> None:
        """Commit the current Snowflake transaction."""
        if self._connection is not None:
            self._connection.commit()

    def rollback(self) -> None:
        """Rollback the current Snowflake transaction."""
        if self._connection is not None:
            try:
                self._connection.rollback()
            except Exception as exc:  # pragma: no cover
                logger.warning("Error during Snowflake rollback: %s", exc)

    def close(self) -> None:
        """Safely close cursor and connection."""
        if self._cursor is not None:
            try:
                self._cursor.close()
            except Exception:  # pragma: no cover
                pass
            self._cursor = None

        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:  # pragma: no cover
                pass
            self._connection = None


# ---------------------------------------------------------------------------
# Statistics Accumulator
# ---------------------------------------------------------------------------

@dataclass
class IngestionStats:
    """Accumulates real-time telemetry ingestion metrics."""

    messages_polled: int = 0
    messages_valid: int = 0
    messages_malformed: int = 0
    rows_inserted: int = 0
    duplicates_skipped: int = 0
    batches_committed: int = 0
    snowflake_errors: int = 0

    def log_progress(self, prefix: str = "Ingestion Progress") -> None:
        """Log a formatted summary of current ingestion metrics."""
        logger.info(
            "%s: Polled=%d | Valid=%d | Malformed=%d | Inserted=%d | Duplicates=%d | Batches=%d | Errors=%d",
            prefix,
            self.messages_polled,
            self.messages_valid,
            self.messages_malformed,
            self.rows_inserted,
            self.duplicates_skipped,
            self.batches_committed,
            self.snowflake_errors,
        )


# ---------------------------------------------------------------------------
# Ingestion Consumer Engine
# ---------------------------------------------------------------------------

class KafkaToSnowflakeConsumer:
    """Orchestrates message consumption from Kafka into Snowflake."""

    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topic: str,
        snowflake_client: SnowflakeIngestionClient,
        consumer: Optional[Any] = None,
    ) -> None:
        """Initialize consumer configuration.

        Args:
            bootstrap_servers: Comma-separated Kafka broker addresses.
            group_id: Kafka consumer group ID.
            topic: Source topic name.
            snowflake_client: Connected SnowflakeIngestionClient instance.
            consumer: Optional pre-configured or mock confluent_kafka.Consumer instance.
        """
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topic = topic
        self.snowflake_client = snowflake_client
        self._custom_consumer = consumer
        self._consumer: Optional[Any] = consumer
        self._shutdown_requested = False
        self.stats = IngestionStats()

    def _init_kafka_consumer(self) -> None:
        """Initialize and subscribe confluent_kafka.Consumer with strict settings."""
        if self._custom_consumer is not None:
            self._consumer = self._custom_consumer
            return

        if confluent_kafka is None:
            raise RuntimeError(
                "confluent-kafka is not installed. "
                "Install it using: pip install confluent-kafka==2.16.1"
            )

        conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",  # Requirement 14: Earliest offset reset
            "enable.auto.commit": False,      # Requirement 13: Disable automatic offset commits
            "session.timeout.ms": 45000,
            "max.poll.interval.ms": 300000,
        }

        logger.info(
            "Initializing Kafka Consumer: brokers=%s group_id=%s topic=%s auto.offset.reset=earliest enable.auto.commit=False",
            self.bootstrap_servers,
            self.group_id,
            self.topic,
        )
        self._consumer = Consumer(conf)
        self._consumer.subscribe([self.topic])

    def request_shutdown(self) -> None:
        """Request graceful shutdown on OS signals."""
        logger.info("Shutdown signal received; completing in-flight batch before exit...")
        self._shutdown_requested = True

    def process_message(
        self, msg: Any
    ) -> Tuple[bool, Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """Decode and validate a single Kafka message.

        Returns:
            Tuple of (is_valid, metadata_dict, content_dict).
        """
        self.stats.messages_polled += 1

        raw_val = msg.value()
        if raw_val is None:
            self.stats.messages_malformed += 1
            logger.warning(
                "Malformed message (empty payload) at partition=%s offset=%s",
                msg.partition(),
                msg.offset(),
            )
            return False, None, None

        try:
            if isinstance(raw_val, bytes):
                text = raw_val.decode("utf-8")
            else:
                text = str(raw_val)
            payload = json.loads(text)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            self.stats.messages_malformed += 1
            logger.warning(
                "Malformed message (JSON decode failure: %s) at partition=%s offset=%s",
                exc,
                msg.partition(),
                msg.offset(),
            )
            return False, None, None

        is_valid, error_msg = CanonicalEventValidator.is_valid(payload)
        if not is_valid:
            self.stats.messages_malformed += 1
            logger.warning(
                "Contract validation failed: %s | event_id=%s partition=%s offset=%s",
                error_msg,
                payload.get("event_id") if isinstance(payload, dict) else "unknown",
                msg.partition(),
                msg.offset(),
            )
            return False, None, None

        self.stats.messages_valid += 1
        metadata = extract_message_metadata(msg)
        return True, metadata, payload

    def ingest_batch(
        self,
        batch: List[Tuple[Dict[str, Any], Dict[str, Any], Any]],
    ) -> bool:
        """Ingest a batch into Snowflake and commit Kafka offset upon success.

        Transaction ordering:
        1. Snowflake MERGE on all batch records.
        2. Snowflake transaction commit.
        3. Kafka offset commit (up to last processed message in batch).

        If Snowflake fails:
        1. Snowflake transaction rollback.
        2. Kafka offset is NOT committed.

        Args:
            batch: List of (metadata_dict, content_dict, kafka_message).

        Returns:
            True if ingestion succeeded and offsets were committed, False otherwise.
        """
        if not batch:
            return True

        # Track the highest successfully processed offset for every topic-partition in this batch
        highest_offsets: Dict[Tuple[str, int], int] = {}
        for metadata, content, msg in batch:
            topic = msg.topic() if hasattr(msg, "topic") and callable(msg.topic) else metadata.get("topic", self.topic)
            partition = msg.partition() if hasattr(msg, "partition") and callable(msg.partition) else metadata.get("partition", 0)
            offset = msg.offset() if hasattr(msg, "offset") and callable(msg.offset) else metadata.get("offset", -1)

            key = (topic, partition)
            if key not in highest_offsets or offset > highest_offsets[key]:
                highest_offsets[key] = offset

        newly_inserted = 0
        duplicates = 0

        try:
            # Step 1: Execute MERGE for each record in transaction
            for metadata, content, _ in batch:
                inserted = self.snowflake_client.merge_record(metadata, content)
                if inserted:
                    newly_inserted += 1
                else:
                    duplicates += 1

            # Step 2: Commit Snowflake transaction FIRST (Requirement 10)
            self.snowflake_client.commit()

            # Step 3: Commit Kafka offsets for all represented partitions
            # Commit the next offset (highest processed offset + 1) for each partition
            if self._consumer is not None and highest_offsets:
                offsets_to_commit = [
                    TopicPartition(t, p, o + 1)
                    for (t, p), o in sorted(highest_offsets.items())
                ]
                self._consumer.commit(offsets=offsets_to_commit, asynchronous=False)

            self.stats.rows_inserted += newly_inserted
            self.stats.duplicates_skipped += duplicates
            self.stats.batches_committed += 1

            partition_summary = ", ".join(
                f"P{p}: next={o + 1}" for (t, p), o in sorted(highest_offsets.items())
            )
            logger.info(
                "Committed batch of %d records: Inserted=%d, Duplicates=%d | Partitions committed: [%s]",
                len(batch),
                newly_inserted,
                duplicates,
                partition_summary,
            )
            return True

        except Exception as exc:
            self.stats.snowflake_errors += 1
            logger.error(
                "Snowflake ingestion failed: %s. Rolling back transaction. Offsets WILL NOT be committed.",
                exc,
            )
            self.snowflake_client.rollback()
            return False

    def run(
        self,
        max_messages: int = 0,
        batch_size: int = 10,
        poll_timeout: float = 1.0,
    ) -> IngestionStats:
        """Run consumer loop until max_messages reached or shutdown requested.

        Args:
            max_messages: Maximum valid messages to ingest (0 for continuous).
            batch_size: Commit batch threshold.
            poll_timeout: Seconds to wait for messages per poll.

        Returns:
            Final IngestionStats accumulator.
        """
        self._init_kafka_consumer()
        self.snowflake_client.connect()

        current_batch: List[Tuple[Dict[str, Any], Dict[str, Any], Any]] = []
        consecutive_empty_polls = 0
        max_empty_polls_for_batch_flush = 3

        logger.info(
            "Starting ingestion loop: target_table=%s max_messages=%s batch_size=%d poll_timeout=%.1fs",
            self.snowflake_client.target_table,
            f"{max_messages} (bounded)" if max_messages > 0 else "continuous",
            batch_size,
            poll_timeout,
        )

        try:
            while not self._shutdown_requested:
                # Stop if max_messages limit reached
                if max_messages > 0 and self.stats.messages_valid >= max_messages:
                    logger.info("Target max_messages limit (%d) reached. Exiting consumer.", max_messages)
                    break

                msg = self._consumer.poll(poll_timeout)

                if msg is None:
                    consecutive_empty_polls += 1
                    # Flush pending batch if idle
                    if current_batch and consecutive_empty_polls >= max_empty_polls_for_batch_flush:
                        success = self.ingest_batch(current_batch)
                        if success:
                            current_batch.clear()
                        consecutive_empty_polls = 0
                    continue

                consecutive_empty_polls = 0

                # Check Kafka-level error
                if msg.error():
                    err = msg.error()
                    if err.code() == KafkaError._PARTITION_EOF:
                        logger.debug("Reached end of partition %s at offset %s", msg.partition(), msg.offset())
                        continue
                    else:
                        logger.error("Kafka consumer error: %s", err)
                        raise KafkaException(err)

                # Decode and validate message
                is_valid, metadata, payload = self.process_message(msg)

                if not is_valid or metadata is None or payload is None:
                    # CRITICAL FIX: Do NOT commit malformed-message offsets immediately.
                    # Safest initial policy: log the malformed event and halt processing without committing its offset.
                    # Discard uncommitted in-memory batch so Kafka offset is never advanced past unpersisted data.
                    logger.error(
                        "CRITICAL: Malformed message detected at topic=%s partition=%s offset=%s. "
                        "Halting consumer loop immediately without committing offset to prevent data loss. "
                        "Earlier uncommitted messages remain preserved in Kafka for retry. "
                        "(Dead Letter Queue Note: In production, route malformed payloads to a DLQ topic, "
                        "e.g., 'atmosync.telemetry.dlq.v1', to inspect poison pills out-of-band).",
                        msg.topic() if hasattr(msg, "topic") and callable(msg.topic) else "unknown",
                        msg.partition() if hasattr(msg, "partition") and callable(msg.partition) else "unknown",
                        msg.offset() if hasattr(msg, "offset") and callable(msg.offset) else "unknown",
                    )
                    current_batch.clear()
                    break

                current_batch.append((metadata, payload, msg))

                # Flush batch if batch size reached or bounded max_messages achieved
                reached_max = max_messages > 0 and self.stats.messages_valid >= max_messages
                if len(current_batch) >= batch_size or reached_max:
                    success = self.ingest_batch(current_batch)
                    if not success:
                        logger.critical("Aborting consumer due to unrecoverable Snowflake error.")
                        break
                    current_batch.clear()

            # Final flush of any remaining records
            if current_batch and not self.snowflake_client.target_table == "":
                logger.info("Flushing final in-flight batch of %d records...", len(current_batch))
                self.ingest_batch(current_batch)
                current_batch.clear()

        finally:
            self.close()

        self.stats.log_progress(prefix="Final Ingestion Summary")
        return self.stats

    def close(self) -> None:
        """Safely close Kafka consumer and Snowflake client."""
        if self._consumer is not None:
            try:
                logger.info("Closing Kafka consumer...")
                self._consumer.close()
            except Exception as exc:  # pragma: no cover
                logger.warning("Error closing Kafka consumer: %s", exc)
            self._consumer = None

        logger.info("Closing Snowflake client...")
        self.snowflake_client.close()


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

def parse_args(args: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse and validate CLI arguments."""
    parser = argparse.ArgumentParser(
        description="AtmoSync Kafka-to-Snowflake Ingestion Consumer",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--topic",
        type=str,
        default=os.getenv("ATMOSYNC_KAFKA_TOPIC", "atmosync.telemetry.v1"),
        help="Kafka topic to consume from.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=os.getenv("ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap broker addresses (host:port).",
    )
    parser.add_argument(
        "--group-id",
        type=str,
        default=os.getenv("ATMOSYNC_KAFKA_GROUP_ID", "atmosync-snowflake-ingestion-v1"),
        help="Kafka consumer group ID.",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=27,
        help="Maximum valid messages to ingest. Set to 0 for continuous streaming.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of messages per Snowflake MERGE transaction commit.",
    )
    parser.add_argument(
        "--poll-timeout",
        type=float,
        default=1.0,
        help="Poll timeout in seconds.",
    )

    return parser.parse_args(args)


def main() -> None:
    """Application CLI entry point."""
    args = parse_args()

    try:
        sf_config = SnowflakeConfig.from_env()
    except ValueError as exc:
        logger.error("Configuration Error: %s", exc)
        sys.exit(1)

    sf_client = SnowflakeIngestionClient(config=sf_config)
    consumer_engine = KafkaToSnowflakeConsumer(
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        topic=args.topic,
        snowflake_client=sf_client,
    )

    # Register OS signals for graceful shutdown
    def _sig_handler(signum: int, frame: Any) -> None:
        consumer_engine.request_shutdown()

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    logger.info("Initializing AtmoSync Kafka-to-Snowflake Consumer (PID: %d)", os.getpid())

    try:
        stats = consumer_engine.run(
            max_messages=args.max_messages,
            batch_size=args.batch_size,
            poll_timeout=args.poll_timeout,
        )
        if stats.snowflake_errors > 0:
            logger.warning("Completed with %d Snowflake error(s).", stats.snowflake_errors)
            sys.exit(2)
        logger.info("Ingestion completed successfully.")
    except Exception as exc:
        logger.exception("Fatal unhandled exception during ingestion: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
