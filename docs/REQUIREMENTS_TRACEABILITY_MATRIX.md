# AtmoSync Task A: Formal Requirements Traceability Matrix

**Document Version:** 1.0.0  
**Status:** Approved Technical Requirements Traceability Matrix  
**Scope:** Task A — IoT Sensor Simulator, Event Engineering, Unified Runner, and Kafka Producer  
**System:** AtmoSync — Micro-Climate Arbitrage Analytics  
**Repository:** `AtmoSync_Micro_Climate_Arbitrage_Analytics`  
**Branch:** `feature/siddarth-task-a`  
**Baseline Commit:** `6296daa` (*"Chore:import existing atmosync project implementation"*); Verified Milestone: `1f668b0`
**Author:** Siddarth Thota (Lead Data Engineer — Task A)
**Date:** November 11, 2026

---

## 1. Executive Summary & Audit Methodology

This document establishes the authoritative, end-to-end **Requirements Traceability Matrix (RTM)** for **Task A: Simulation and Event Engineering** of the AtmoSync platform. 

The audit evaluates all applicable functional requirements (FR), engineering invariants, and architectural specifications defined across:
- `TASK_A_PART1_SIMULATOR_SPECIFICATION.md` (Working Simulator Specification)
- `docs/ARCHITECTURE.md` (System Architecture Specification)
- `docs/DATA_CONTRACT.md` (Canonical Telemetry Event Data Contract Version 1)
- `docs/STAGE_5_KAFKA_DESIGN.md` (Kafka Producer Architecture & Semantics)
- `docs/STAGE_6_2_RUNNER_DESIGN.md` (Unified Simulation Runner Architecture)
- `docs/STAGE_6_3_CLI_GUIDE.md` (CLI & Operating Lifecycle Design)
- `docs/OPERATIONS_GUIDE.md` (Operational Runbook and Administration Guide)
- `docs/VERIFICATION_REPORT.md` (Technical Verification and Test Audit Ledger)

### Traceability Classification Criteria:
- **Verified Complete:** Fully implemented in source code and proven by passing automated unit/integration tests with documented execution evidence.
- **Implemented (Pending Live Broker):** Fully implemented in source code and unit-tested with mock clients, but awaiting live execution against the active Docker Kafka broker in Phase 4 / Week 2.
- **Documentation Action Required:** Technical implementation complete; requires authoring or formalization of companion documentation or progress tracking logs.
- **Incomplete / Missing:** Requirement not yet satisfied in code or specification.
- **Not Applicable:** Out of scope for Task A.

---

## 2. Requirements Traceability Matrix Table

| Req ID | Requirement Area | Source Specification | Implementation Module | Automated Test File | Status | Verification Evidence | Remaining Gap |
| :-: | :--- | :--- | :--- | :--- | :---: | :--- | :--- |
| **REQ-CFG-01** | **YAML Configuration Loading & Duplicate Key Rejection** | `TASK_A_PART1` §5 (FR-01), `ARCHITECTURE.md` §4 | `simulation/config/config_loader.py` (`ConfigLoader`, `UniqueKeySafeLoader`) | `tests/test_configuration.py` | **Verified Complete** | `test_load_default_project_config`, `test_duplicate_key_rejected_in_yaml`. 51 tests pass. | None. |
| **REQ-CFG-02** | **Container Fleet ID Validation** | `TASK_A_PART1` §5 (FR-01, FR-02) | `simulation/config/config_loader.py` (`AppConfig.containers`) | `tests/test_configuration.py` | **Verified Complete** | `test_multiple_containers_parsing`, `test_duplicate_container_ids_detected`, `test_blank_container_item_rejected`. | None. |
| **REQ-CFG-03** | **Simulation Timing & Random Seed Parsing** | `TASK_A_PART1` §5 (FR-01) | `simulation/config/config_loader.py` (`SimulationSettings`) | `tests/test_configuration.py` | **Verified Complete** | `test_non_positive_simulation_interval`, `test_non_finite_simulation_interval_rejected`, `test_invalid_random_seed_type`. | None. |
| **REQ-CFG-04** | **Provisional Sensor Limits & Units Validation** | `TASK_A_PART1` §5 (FR-01) | `simulation/config/config_loader.py` (`SensorConfig`) | `tests/test_configuration.py` | **Verified Complete** | `test_missing_sensor_enabled_field`, `test_sensor_min_greater_than_max_raises_error`. Limits labeled provisional. | None. |
| **REQ-SNS-01** | **PRNG Stream Isolation & Cross-Process Repeatability** | `TASK_A_PART1` §5 (FR-03), `ARCHITECTURE.md` §5 | `simulation/sensors/base_sensor.py` (`BaseSensor`, SHA-256 derivation) | `tests/test_temperature_sensor.py` | **Verified Complete** | `test_cross_process_seed_derivation`, `test_temperature_sensor_reproducibility`. Eliminates `PYTHONHASHSEED` variance. | None. |
| **REQ-SNS-02** | **Temperature Virtual Sensor Model** | `TASK_A_PART1` §5 (FR-03) | `simulation/sensors/temperature_sensor.py` (`TemperatureSensor`) | `tests/test_temperature_sensor.py` | **Verified Complete** | Gaussian distribution ($\mu=4.0^\circ\text{C}, \sigma=1.5$), physical limits $[-50, 100]^\circ\text{C}$, 2-decimal rounding. 11 tests pass. | None. |
| **REQ-SNS-03** | **Humidity Virtual Sensor Model** | `TASK_A_PART1` §5 (FR-03) | `simulation/sensors/humidity_sensor.py` (`HumiditySensor`) | `tests/test_humidity_sensor.py` | **Verified Complete** | Bounded Gaussian ($\mu=85.0\%, \sigma=5.0$), physical clamping $[0.0, 100.0]\%$, 1-decimal rounding. 10 tests pass. | None. |
| **REQ-SNS-04** | **Vibration Virtual Sensor Model** | `TASK_A_PART1` §5 (FR-03) | `simulation/sensors/vibration_sensor.py` (`VibrationSensor`) | `tests/test_vibration_sensor.py` | **Verified Complete** | Rayleigh distribution ($\sigma=0.05\,\text{m/s}^2$), strictly non-negative ($x \ge 0$), 3-decimal rounding. 11 tests pass. | None. |
| **REQ-SIM-01** | **Container Simulator Coordination** | `TASK_A_PART1` §5 (FR-02) | `simulation/simulator/container_simulator.py` (`ContainerSimulator`) | `tests/test_container_simulator.py` | **Verified Complete** | Coordinates sensor cluster per container; container-specific SHA-256 derived seed. 8 tests pass. | None. |
| **REQ-SIM-02** | **Multi-Container Simulation Engine** | `TASK_A_PART1` §5 (FR-02) | `simulation/simulator/simulation_engine.py` (`SimulationEngine`) | `tests/test_simulation_engine.py` | **Verified Complete** | Coordinates cycles across containers; supports finite runs (`run()`) and continuous streaming (`stream_events()`). 8 tests pass. | None. |
| **REQ-EVT-01** | **Option A Telemetry Multiplicity** | `TASK_A_PART1` §5 (FR-04), `DATA_CONTRACT.md` §2 | `simulation/simulator/container_simulator.py`, `simulation/events/event_schema.py` | `tests/test_container_simulator.py` | **Verified Complete** | Emits 1 independent `SensorEvent` per sensor reading (9 events/cycle for 3 containers $\times$ 3 sensors). | None. |
| **REQ-EVT-02** | **Canonical Version-1 Event Schema (7 Fields)** | `DATA_CONTRACT.md` §3 | `simulation/events/event_schema.py` (`SensorEvent`) | `tests/test_event_validator.py` | **Verified Complete** | Frozen dataclass with immutable fields: `schema_version`, `event_id`, `container_id`, `sensor_type`, `observed_at`, `value`, `unit`. | None. |
| **REQ-EVT-03** | **Event UUIDv4 & UTC Timestamp Generation** | `TASK_A_PART1` §5 (FR-04) | `simulation/events/event_builder.py` (`EventBuilder`) | `tests/test_event_builder.py` | **Verified Complete** | RFC 4122 UUIDv4 identifiers; millisecond UTC ISO-8601 timestamps ending strictly in `'Z'`. 15 tests pass. | None. |
| **REQ-EVT-04** | **Strict Schema Validation & Invariant Checking** | `TASK_A_PART1` §5 (FR-05), `DATA_CONTRACT.md` §5 | `simulation/validation/event_validator.py` (`EventValidator`) | `tests/test_event_validator.py` | **Verified Complete** | Rejects non-UTC offsets, unknown fields, `NaN`/`Inf`, and boolean type coercions. 48 tests pass. | None. |
| **REQ-OUT-01** | **Deterministic Canonical JSON Serialization** | `TASK_A_PART1` §5 (FR-06), `DATA_CONTRACT.md` §4 | `simulation/events/event_serializer.py` (`EventSerializer`) | `tests/test_event_serializer.py` | **Verified Complete** | Canonical key ordering, compact delimiters `(',', ': ')`, `allow_nan=False`, UTF-8 bytes. 28 tests pass. | None. |
| **REQ-OUT-02** | **Append-Only JSON Lines (JSONL) Persistence** | `TASK_A_PART1` §5 (FR-06) | `simulation/output/jsonl_writer.py` (`JsonlWriter`) | `tests/test_jsonl_writer.py` | **Verified Complete** | Atomic line append, parent directory auto-creation, path validation, file error handling. 13 tests pass. | None. |
| **REQ-OUT-03** | **Output Sink Abstraction & Fan-Out Routing** | `ARCHITECTURE.md` §7, `STAGE_6_2` | `simulation/output/sink.py` (`OutputSink`, `JsonlSink`, `KafkaSink`, `CompositeOutputSink`) | `tests/test_sinks.py` | **Verified Complete** | Single event reference passed without mutation; fail-fast error propagation; reverse-order cleanup. 30 tests pass. | None. |
| **REQ-KFK-01** | **Kafka Configuration & Credential Masking** | `STAGE_5_KAFKA_DESIGN.md` §3 | `simulation/kafka/kafka_config.py` (`KafkaConfig`) | `tests/test_kafka_config.py` | **Verified Complete** | Loads from environment or defaults, validates bootstrap servers and topic, masks SASL credentials in `__repr__`. 64 tests pass. | None. |
| **REQ-KFK-02** | **Kafka Producer & Partition Key Affinity** | `STAGE_5_KAFKA_DESIGN.md` §3.3 | `simulation/kafka/kafka_producer.py` (`SensorEventKafkaProducer`) | `tests/test_kafka_producer.py` | **Verified Complete** | UTF-8 `container_id` used as partition key ensuring partition affinity; `enable.idempotence=True`, `acks="all"`. 19 tests pass. | None. |
| **REQ-KFK-03** | **Asynchronous Delivery Callbacks & Metrics** | `STAGE_5_KAFKA_DESIGN.md` §4 | `simulation/kafka/delivery_report.py`, `simulation/kafka/kafka_producer.py` | `tests/test_kafka_producer.py` | **Verified Complete** | Distinguishes client enqueue (`attempted`) from partition commit (`delivered`); thread-safe `PublishingStats`. | None. |
| **REQ-KFK-04** | **Kafka Backpressure & Buffer Relief** | `STAGE_5_KAFKA_DESIGN.md` §4.3 | `simulation/kafka/kafka_producer.py`, `simulation/kafka/exceptions.py` | `tests/test_kafka_producer.py` | **Verified Complete** | Bounded polling relief when queue is full (`BufferError`); max retries before `KafkaBackpressureError`. | None. |
| **REQ-INT-01** | **Live Kafka Broker Integration Testing** | Master Prompt §9 | `simulation/kafka/kafka_producer.py`, `docker-compose.kafka.yml` | `tests/integration/test_kafka_integration.py`, `test_runner_integration.py` | **Implemented (Pending Live Broker)** | 1 negative test passes offline (`test_live_unreachable_broker_failure`); 4 live tests skip gracefully when broker is offline. | Start Docker Kafka container and run live integration tests in Week 2 (Nov 18). |
| **REQ-RUN-01** | **Unified Simulation Runner Orchestration** | `STAGE_6_2_RUNNER_DESIGN.md` | `simulation/runner.py` (`SimulationRunner`, `RunnerMetrics`) | `tests/test_runner.py`, `tests/test_runner_failures.py` | **Verified Complete** | Fail-fast error propagation, state transitions, honest metrics snapshot (`RunnerMetrics`). 34 tests pass. | None. |
| **REQ-CLI-01** | **Operational CLI & POSIX Exit Codes** | `STAGE_6_3_CLI_GUIDE.md` | `simulation/cli.py`, `simulation/__main__.py` | `tests/test_cli.py`, `tests/test_cli_lifecycle.py` | **Verified Complete** | Argument parsing, non-blocking `SignalContext` (SIGINT, SIGTERM, Windows SIGBREAK), exit codes 0, 1, 2, 130, 143. 25 tests pass. | None. |
| **REQ-DOC-01** | **Technical Documentation & Downstream Handover** | Master Prompt §10, §11, §14, §15 | `docs/` | Inspection | **Documentation Action Required** | Baseline documentation updated. Activity log and RTM initialized. Downstream guide and viva bank scheduled for Week 2. | Author `DOWNSTREAM_INTEGRATION_GUIDE.md` and viva bank across scheduled days. |

---

## 3. Subsystem Architectural Verification Summaries

### 3.1 Configuration Subsystem (`simulation/config/`)
- **Key Modules:** `config_loader.py`, `simulation_config.yaml`
- **Architectural Safeguards:** `UniqueKeySafeLoader` overrides YAML mapping construction to raise a `ConstructorError` upon encountering duplicate mapping keys. All configuration models (`AppConfig`, `SimulationSettings`, `SensorConfig`) are frozen dataclasses wrapping read-only mappings (`MappingProxyType`) to prevent runtime state mutations.
- **Verification Evidence:** `tests/test_configuration.py` executes 51 test cases covering missing sections, non-positive intervals, non-finite intervals (`NaN`, `Inf`), duplicate container IDs, blank container strings, and sensor limit boundary checks.

### 3.2 Virtual Sensors & PRNG Isolation (`simulation/sensors/`)
- **Key Modules:** `base_sensor.py`, `temperature_sensor.py`, `humidity_sensor.py`, `vibration_sensor.py`
- **Architectural Safeguards:** Each sensor utilizes a cryptographically stable SHA-256 seed derivation function (`BaseSensor.derive_sensor_seed`) that combines the master seed with container and sensor identifiers. This guarantees that independent sensor streams never collide or drift and eliminates Python's process-level hash randomization variance (`PYTHONHASHSEED`).
- **Statistical Fidelity:**
  - **Temperature:** Gaussian distribution ($\mu=4.0^\circ\text{C}, \sigma=1.5$), physical limits $[-50.0, 100.0]^\circ\text{C}$, 2 decimal precision.
  - **Humidity:** Bounded Gaussian ($\mu=85.0\%, \sigma=5.0$), physical clamping $[0.0, 100.0]\%$, 1 decimal precision.
  - **Vibration:** Rayleigh distribution ($\sigma=0.05\,\text{m/s}^2$), mathematical non-negative invariant ($x \ge 0$), 3 decimal precision.
- **Verification Evidence:** 32 unit tests pass across `test_temperature_sensor.py` (11), `test_humidity_sensor.py` (10), and `test_vibration_sensor.py` (11).

### 3.3 Event Engineering & Canonical Data Contract (`simulation/events/`, `simulation/validation/`)
- **Key Modules:** `event_schema.py`, `event_builder.py`, `event_serializer.py`, `event_validator.py`
- **Architectural Safeguards:** Implements **Option A multiplicity** (one top-level event per sensor reading). The canonical schema version 1 requires exactly seven fields:
  `schema_version` (int: 1), `event_id` (str: UUIDv4), `container_id` (str), `sensor_type` (str: `temperature|humidity|vibration`), `observed_at` (str: UTC ISO-8601 with millisecond precision ending in `'Z'`), `value` (float: finite), `unit` (str).
- **Deterministic Serialization:** Enforces strict alphabetical key sorting, compact delimiters `(',', ': ')`, `allow_nan=False`, and UTF-8 encoding.
- **Verification Evidence:** 91 unit tests pass across `test_event_builder.py` (15), `test_event_validator.py` (48), and `test_event_serializer.py` (28).

### 3.4 Multi-Sink Persistence & Fan-Out Architecture (`simulation/output/`)
- **Key Modules:** `sink.py`, `jsonl_writer.py`
- **Architectural Safeguards:** `OutputSink` interface defines `write_event`, `write_events`, `flush`, and `close`. `CompositeOutputSink` guarantees single event identity: the exact same immutable `SensorEvent` object reference is passed to `JsonlSink` and `KafkaSink`. Fail-fast dispatch executes in deterministic order (`jsonl` first, `kafka` second); teardown executes in reverse order.
- **Verification Evidence:** 43 unit tests pass across `test_jsonl_writer.py` (13) and `test_sinks.py` (30).

### 3.5 Apache Kafka Streaming Pipeline (`simulation/kafka/`)
- **Key Modules:** `kafka_config.py`, `kafka_producer.py`, `delivery_report.py`, `exceptions.py`
- **Architectural Safeguards:** Employs `confluent-kafka` (librdkafka C-engine). Partition keys are derived as UTF-8 encoded `container_id` bytes, ensuring container-level partition affinity. Configured with `enable.idempotence=True` and `acks="all"` for strict in-partition deduplication and ordering. Implements bounded backpressure relief with automatic poll draining upon queue saturation.
- **Verification Evidence:** 83 unit tests pass across `test_kafka_config.py` (64) and `test_kafka_producer.py` (19).

### 3.6 Unified Orchestrator & CLI Execution (`simulation/runner.py`, `simulation/cli.py`)
- **Key Modules:** `runner.py`, `cli.py`, `__main__.py`
- **Architectural Safeguards:** `SimulationRunner` bridges `SimulationEngine` and `OutputSink`. Enforces fail-fast boundaries, manages execution lifecycles, and distinguishes local client dispatch from broker partition commits. `cli.py` registers a non-blocking `SignalContext` for graceful termination across `SIGINT`, `SIGTERM`, and Windows `SIGBREAK`, mapping exits to standard POSIX status codes (`0`, `1`, `2`, `130`, `143`).
- **Verification Evidence:** 59 unit tests pass across `test_runner.py` (22), `test_runner_failures.py` (12), `test_cli.py` (14), and `test_cli_lifecycle.py` (11).

---

## 4. Test Suite Execution & Statement Coverage Ledger

### 4.1 Unit & Contract Test Execution Summary
- **Execution Date:** November 9–10, 2026
- **Test Command:** `python -m pytest -m "not integration" --cov=simulation`
- **Collected Test Items:** 377
- **Selected Test Items:** 372
- **Deselected Items:** 5 (Integration markers)
- **Passed Tests:** **372**
- **Failed Tests:** **0**
- **Execution Duration:** **2.11 seconds**

### 4.2 Statement Coverage Breakdown by Module

| Module Name | Statements | Misses | Coverage Percentage | Verification Scope |
| :--- | :---: | :---: | :---: | :--- |
| `simulation/__init__.py` | 1 | 0 | **100%** | Package root exports |
| `simulation/__main__.py` | 4 | 4 | 0% | CLI invocation wrapper |
| `simulation/cli.py` | 180 | 36 | **80%** | CLI parsing, signal handling, lifecycle |
| `simulation/config/__init__.py` | 2 | 0 | **100%** | Configuration exports |
| `simulation/config/config_loader.py` | 157 | 10 | **94%** | YAML parsing, validation, duplicate key checking |
| `simulation/events/__init__.py` | 4 | 0 | **100%** | Event subsystem exports |
| `simulation/events/event_builder.py` | 38 | 6 | **84%** | UUIDv4, UTC clock injection |
| `simulation/events/event_schema.py` | 16 | 0 | **100%** | Canonical 7-field SensorEvent contract |
| `simulation/events/event_serializer.py` | 59 | 1 | **98%** | Canonical JSON byte serialization |
| `simulation/kafka/__init__.py` | 5 | 0 | **100%** | Kafka package exports |
| `simulation/kafka/delivery_report.py` | 72 | 8 | **89%** | Thread-safe delivery metrics & callbacks |
| `simulation/kafka/exceptions.py` | 10 | 0 | **100%** | Kafka error hierarchy |
| `simulation/kafka/kafka_config.py` | 170 | 13 | **92%** | Producer configuration & credential masking |
| `simulation/kafka/kafka_producer.py` | 163 | 14 | **91%** | confluent-kafka client, backpressure, keying |
| `simulation/output/__init__.py` | 3 | 0 | **100%** | Output package exports |
| `simulation/output/jsonl_writer.py` | 83 | 9 | **89%** | Atomic append-only JSONL disk I/O |
| `simulation/output/sink.py` | 220 | 28 | **87%** | OutputSink, JsonlSink, KafkaSink, Composite |
| `simulation/runner.py` | 233 | 10 | **96%** | Lifecycle management & delivery accounting |
| `simulation/sensors/__init__.py` | 5 | 0 | **100%** | Sensor package exports |
| `simulation/sensors/base_sensor.py` | 70 | 8 | **89%** | SHA-256 PRNG stream derivation |
| `simulation/sensors/humidity_sensor.py` | 33 | 0 | **100%** | Bounded Gaussian humidity model |
| `simulation/sensors/temperature_sensor.py`| 33 | 0 | **100%** | Gaussian temperature model |
| `simulation/sensors/vibration_sensor.py` | 31 | 0 | **100%** | Rayleigh non-negative vibration model |
| `simulation/simulator/__init__.py` | 3 | 0 | **100%** | Simulator package exports |
| `simulation/simulator/container_simulator.py` | 63 | 3 | **95%** | Container cluster coordination |
| `simulation/simulator/simulation_engine.py` | 93 | 5 | **95%** | Multi-container iteration cycles |
| `simulation/validation/__init__.py` | 2 | 0 | **100%** | Validation package exports |
| `simulation/validation/event_validator.py` | 76 | 2 | **97%** | Strict Version-1 contract enforcement |
| **TOTAL** | **1,829** | **157** | **91%** | **Overall Statement Coverage** |

---

## 5. Technical Closure & Remaining Gap Action Plan

| Gap Description | Applicable Requirement | Target Resolution Date | Planned Engineering Action |
| :--- | :--- | :---: | :--- |
| **Live Kafka Broker Verification** | `REQ-INT-01` | **November 17–18, 2026** | Launch Docker Kafka container (`docker compose -f docker-compose.kafka.yml up -d`) and execute `tests/integration/test_kafka_integration.py` and `tests/integration/test_runner_integration.py`. |
| **Downstream Integration Handover Guide** | `REQ-DOC-01` | **November 20, 2026** | Author `docs/DOWNSTREAM_INTEGRATION_GUIDE.md` detailing Snowflake raw staging table DDL, Snowpipe streaming compatibility, dbt modeling advice, and sample payloads for Tasks B, C, and D. |
| **Mid-Review Demonstration & Viva Bank** | `REQ-DOC-01` | **November 21–22, 2026** | Author demonstration runbook and compile 40 technical viva questions and answers covering Python, PRNG isolation, Kafka idempotence, and system design. |
| **Weekly Reports** | `REQ-DOC-01` | **November 16 & 23, 2026** | Compile `docs/progress/WEEK_1_REPORT.md` and `docs/progress/WEEK_2_REPORT.md` summarizing verified progress for Infotact portal submission. |

