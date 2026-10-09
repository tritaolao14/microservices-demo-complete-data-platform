# Data Engineering Patterns — Trạng thái triển khai

Tài liệu này liệt kê **các data engineering pattern đã áp dụng thật trong repo** (kèm bằng chứng code/manifest), những pattern mới chỉ cấu hình, và những pattern còn thiếu theo roadmap. Dùng làm nguồn sự thật duy nhất về "đã làm được gì".

> **Quy tắc bắt buộc:** mỗi PR thay đổi hành vi data platform **phải cập nhật tài liệu này** trong cùng PR. Xem `AGENTS.md` → "Data Platform Documentation Rule".

Cập nhật lần cuối: 2026-10-09 (sau DPFMD-33; thêm Bronze Layer Writer MinIO Parquet cho DPFMD-34). Đối tác: `docs/data-platform.md`, `.agents/knowledge/data-engineering-design-patterns-index.md`.

## Chú giải trạng thái

- **Đang chạy** — đã code + deploy + verify trên cluster.
- **Cấu hình sẵn** — manifest/ConfigMap có nhưng chưa có workload vận hành.
- **Chưa làm** — nằm trong roadmap, chưa triển khai.

---

## Trạng thái tổng quan

| # | Pattern | Trạng thái | Bằng chứng |
|---|---------|-----------|------------|
| 1 | Event Streaming / Pub-Sub | Đang chạy | `src/checkoutservice/main.go:420`, `kubernetes-manifests/kafka.yaml` |
| 2 | Layered / Hexagonal (Ports & Adapters) | Đang chạy | `src/dataingestion/{domain,application,infrastructure,presentation}` |
| 3 | Stream ETL (Kafka → OLAP) | Đang chạy | `domain/transform.py`, `infrastructure/deserializer.py`, `infrastructure/postgres_writer.py` |
| 4 | Idempotent Writer (keyed idempotency) | Đang chạy | `infrastructure/postgres_writer.py:18` |
| 5 | Dead-Letter Queue (DLQ) | Đang chạy | `infrastructure/kafka_dlq_producer.py`, `application/consumer_service.py:92` |
| 6 | Retry + Circuit Breaker (in-memory backoff) | Đang chạy | `infrastructure/kafka_retry_producer.py`, `postgres_writer.py:87` |
| 7 | Change Data Capture (CDC) | Đang chạy | `gitops/overlays/dev/debezium.yaml`, `scripts/e2e-cdc.sh`, `docs/data-platform.md:39` |
| 8 | Schema Registry / Data Contract (Avro) | Đang chạy | `gitops/overlays/dev/schema-registry.yaml`, `src/debezium/` |
| 9 | Declarative Config Reconciliation (drift) | Đang chạy | `Job/debezium-register` trong `gitops/overlays/dev/debezium.yaml` |
| 10 | GitOps (ArgoCD + Kustomize overlays) | Đang chạy | `gitops/overlays/{dev,staging,production}` |
| 11 | Full / Incremental Batch Load + Upsert | Đang chạy | `src/data_processing/seed_database.py` |
| 12 | Dimensional Modeling (một phần) | Đang chạy | `analytics.order_items` trong `seed_database.py:67` |
| 13 | Object Storage / Data Lake | Đang chạy | `kubernetes-manifests/minio.yaml`, `src/data_processing/make_bucket_public.py`, `src/data_processing/lakehouse/sinks.py` |
| 14 | Lakehouse / Iceberg Table Format | Cấu hình sẵn | `kubernetes-manifests/spark-iceberg-config.yaml` |
| 14b | Bronze Layer Writer (Parquet + Manifest) | Đang chạy | `src/data_processing/lakehouse/` |
| 15 | Orchestration (Airflow) | Chưa làm | roadmap Phase 3 |
| 16 | Transformation (dbt / Star schema) | Chưa làm | roadmap Phase 3 |
| 17 | Serving / Query Layer (Trino/ClickHouse) | Chưa làm | roadmap Phase 3 |
| 18 | BI / Visualization | Chưa làm | roadmap Phase 4 |
| 19 | Security & Governance (secrets, auth) | Chưa làm | `TODO.md` Sprint 3 |

---

## 1. Event Streaming / Pub-Sub — Đang chạy

- `checkoutservice` là producer: publish order event JSON lên topic `orders` bằng `segmentio/kafka-go` (v0.4.51), key = `order_id`, payload gồm `order_id`, `shipping_*`, `items`, `timestamp` (`src/checkoutservice/main.go:420`).
- Retry publish 3 lần với backoff tuyến tính (`main.go:441`).
- Kafka chạy **KRaft mode** (không ZooKeeper); `auto.create.topics.enable=true` (`kubernetes-manifests/kafka.yaml`).

## 2. Layered / Hexagonal Architecture — Đang chạy

`dataingestion` tách tầng, phụ thuộc abstraction (DIP), không để tầng application import kafka-python:

| Tầng | Vai trò | File |
|------|---------|------|
| `domain/` | models, validation, transform | `domain/{models,transform,exceptions}.py` |
| `application/` | use case orchestration + routing lỗi | `application/consumer_service.py` |
| `infrastructure/` | adapter Kafka / PostgreSQL | `infrastructure/*` |
| `presentation/` | format output | `presentation/order_formatter.py` |

## 3. Stream ETL (Kafka → OLAP) — Đang chạy

- Luồng: consume `orders` → deserialize JSON (xử lý `omitempty` của Go) → validate → **flatten** → persist.
- `Order` lồng `items`/`Money` → `TransformedOrderItem` (1 dòng / line item) → bảng `analytics.order_items` (`domain/transform.py`, `infrastructure/deserializer.py`).
- `Money(units + nanos)` → `Decimal` (`domain/models.py:71`).

## 4. Idempotent Writer (Keyed Idempotency) — Đang chạy

- Ghi batch bằng `execute_values` với `INSERT ... ON CONFLICT (order_id, product_id) DO NOTHING` (`infrastructure/postgres_writer.py:18`).
- Composite PK `(order_id, product_id)` + at-least-once ⇒ hiệu ứng effectively-once.

## 5. Dead-Letter Queue — Đang chạy

- Message poison / invalid → topic `orders_dlq`, payload gồm `original`, `error`, `attempt`, `timestamp` (`infrastructure/kafka_dlq_producer.py`).
- Routing: `InvalidOrderError` và lỗi DB non-retryable đi thẳng DLQ (`application/consumer_service.py:92`).

## 6. Retry + Circuit Breaker — Đang chạy

- Phân loại lỗi: **retryable** (`OperationalError`, `InterfaceError`, `InternalError`) vs **non-retryable** (`ProgrammingError`, `DataError`).
- Lỗi tạm thời → topic `orders_retry` (wrapper `original`/`attempt`, header `x-retry-count`), rồi re-consume.
- Backoff lũy tiến + jitter trong `PostgresWriter`; quá `RETRY_MAX_ATTEMPT` → DLQ (`consumer_service.py:69`, `postgres_writer.py:87`).

## 7. Change Data Capture (CDC) — Đang chạy

- Debezium (Kafka Connect) dùng logical replication `pgoutput`, snapshot `initial` (`gitops/overlays/dev/debezium.yaml`).
- Hai connector, mỗi connector một slot + publication riêng:
  - `productcatalog-postgres` → `public.products` → slot/pub `debezium_productcatalog` → topic `cdc_product_changes`.
  - `orderitems-postgres` → `analytics.order_items` → slot/pub `debezium_orderitems` → topic `cdc_order_items`.
- SMT `RegexRouter` chuẩn hoá tên topic; `topic.prefix` **bắt buộc khác nhau** (JMX MBean collision — xem `docs/data-platform.md:167`).
- Yêu cầu nguồn: `wal_level=logical` (`kubernetes-manifests/postgresql.yaml`).
- E2E test: `scripts/e2e-cdc.sh` — INSERT rồi đọc lại event Avro (`op=c`) từ cả hai topic; chạy consumer trong pod ephemeral để không OOMKill `schema-registry` (xem `docs/data-platform.md` → "E2E test (DPFMD-33)").

## 8. Schema Registry / Data Contract (Avro) — Đang chạy

- `confluentinc/cp-schema-registry:7.6.1` + Avro converter đóng gói trong image `src/debezium` (`gitops/overlays/dev/schema-registry.yaml`).
- Subject `cdc_order_items-value` / `-key`, evolution theo version; wire format Confluent `magic 0x00 + schema id 4 byte big-endian`.
- Debezium envelope giữ `before`/`after`/`op`/`source`.

## 9. Declarative Config Reconciliation — Đang chạy

- `Job/debezium-register`: GET config thật → POST nếu chưa có → PUT nếu lệch git → chờ `RUNNING` → verify lại (`gitops/overlays/dev/debezium.yaml`).
- Chỉ PUT khi thật sự drift (tránh restart task liên tục); Job fail khi drift ⇒ đóng vai trò monitoring.

## 10. GitOps — Đang chạy

- ArgoCD `Application/dev` với `prune` + `selfHeal` + `CreateNamespace`; Kustomize overlays `dev` / `staging` / `production`.
- CI build kustomize cho cả 3 overlay (`.github/workflows/pre-pr.yaml`).

## 11. Full / Incremental Batch Load + Upsert — Đang chạy

- `src/data_processing/seed_database.py`: đọc `products.json` + CSV → upsert `products`/`categories`/`product_categories`.
- Batch 100 dòng; tải ảnh song song `ThreadPoolExecutor(max_workers=20)`; `ON CONFLICT DO UPDATE`.

## 12. Dimensional Modeling (một phần) — Đang chạy

- Schema `analytics` với bảng fact-like `order_items` (grain = order × product), PK `(order_id, product_id)`, index theo `order_id`, `product_id`, `event_timestamp` (`seed_database.py:67`).
- Catalog chuẩn hoá: `products` (dimension), `categories`, `product_categories` (bridge).

## 13. Object Storage / Data Lake — Đang chạy

- MinIO (S3-compatible) bucket `datalake-unstructured`, lưu ảnh sản phẩm (`kubernetes-manifests/minio.yaml`).
- Bucket policy public-read (`src/data_processing/make_bucket_public.py`).

## 14. Lakehouse / Iceberg — Cấu hình sẵn (chưa vận hành)

- `ConfigMap/spark-iceberg-config`: JDBC catalog trên Postgres, warehouse `s3a://iceberg-warehouse/`, `S3FileIO` tới MinIO.
- **Chưa có Spark deployment/job nào chạy** — mới dừng ở cấu hình.

## 14b. Bronze Layer Writer (MinIO Parquet) — Đang chạy

Package `src/data_processing/lakehouse/` land raw order event xuống `s3://lakehouse/bronze/orders/{yyyy}/{mm}/{dd}/` dưới dạng **Parquet snappy** + manifest (DPFMD-34). Đây là landing zone thô; bảng Iceberg (DPFMD-35) sẽ bọc lên trên.

Cấu trúc theo **Clean Architecture (Ports & Adapters)**: `domain/` (entity + logic thuần) ← `application/` (use case + ports) ← `infrastructure/` (adapter MinIO/Parquet/env); `runner.py` là composition root, `domain` không phụ thuộc ra ngoài.

| Pattern (knowledge index) | Bằng chứng |
|---------------------------|------------|
| Horizontal Partitioner (Ch.8) — `yyyy/mm/dd`, ngày chuẩn hoá **UTC** | `domain/partitioning.py:31`, `application/layout.py:16`, `application/writer.py:164` |
| Manifest (Ch.8) — reader dùng `_manifest.json` thay vì list S3 | `domain/manifest.py:47`, `domain/manifest.py:54`, `application/writer.py:196` |
| Transactional Writer (Ch.4) — ghi `_staging/` rồi commit atomic (copy) | `application/writer.py:190` |
| Proxy / Readiness Marker (Ch.6) — `_SUCCESS` sau khi commit manifest | `application/writer.py:213`, `application/layout.py:11` |
| Circuit Breaker / Retry (Ch.3) — backoff + jitter quanh I/O S3 | `application/retry.py:16`, `application/writer.py` `_call()` |
| Metadata Decorator / Wrapper (Ch.5) — cột `_ingested_at`, `_event_date`, `_source_topic`, `_kafka_partition`, `_kafka_offset`, `_schema_version`, payload gốc ở cột `raw` | `infrastructure/parquet_encoder.py:25`, `domain/records.py` |

- Ports (Dependency Inversion): `ObjectSink`, `RecordEncoder` (`application/ports.py:16`, `application/ports.py:39`); adapter `Boto3Sink`/`InMemorySink` (`infrastructure/sinks.py`) và `ParquetEncoder` (`infrastructure/parquet_encoder.py`).
- Idempotency: tên file `part-<sha256(content)[:16]>.parquet` + `Manifest.upsert` dedupe theo `path` ⇒ replay cùng batch không tạo file trùng (`application/writer.py:164`, `domain/manifest.py:54`).
- Rollover file: `BRONZE_FLUSH_ROWS` (mặc định 50k), `BRONZE_TARGET_FILE_BYTES` (mặc định 128 MiB), `BRONZE_FLUSH_INTERVAL_SECONDS` (mặc định 60s) — `application/rollover.py`, `application/options.py`, `infrastructure/config.py`.
- Đầu vào: Kafka topic `orders` (mặc định) hoặc file JSONL (`--input`) để dry-run; entrypoint `python -m lakehouse.runner` / `bronze_orders_writer.py` (`runner.py`).
- Đã verify E2E cục bộ: 3 event → 2 partition ngày (`2026/10/09`, `2026/10/10`), mỗi partition có Parquet + `_manifest.json` + `_SUCCESS`, đọc lại Parquet thấy đúng schema; 25 unit test (`pytest`, gồm roundtrip pyarrow) + ruff pass.
- Phụ thuộc: `boto3`, `kafka-python`, `pyarrow` (`src/data_processing/requirements.in`).

## 15–18. Chưa làm

- **Orchestration (Airflow)**, **Transformation (dbt + Star schema)**, **Query layer (Trino/ClickHouse)**, **BI (Superset/Metabase)** — thuộc Phase 3–4 trong `docs/data-platform.md`.
- **Security & Governance** (SealedSecret, Schema Registry auth, mã hoá config topic) — Sprint 3 trong `TODO.md`.

---

## Map sang knowledge index

Đối chiếu với `.agents/knowledge/data-engineering-design-patterns-index.md` (sách Konieczny):

| Nhóm chapter | Đã áp trong repo |
|--------------|------------------|
| Ch.2 Data Ingestion | Full Loader (`seed_database`), Incremental (order_items theo `event_timestamp`), CDC Replication (Debezium) |
| Ch.3 Error Management | Dead-Letter (DLQ), Circuit Breaker/Retry |
| Ch.4 Idempotency | Keyed Idempotency / Idempotent Writer (`ON CONFLICT DO NOTHING`), Transactional Writer (`lakehouse/application/writer.py`) |
| Ch.5 Data Value | Metadata Decorator (cột metadata Bronze — `lakehouse/infrastructure/parquet_encoder.py`) |
| Ch.6 Data Flow | Proxy / Readiness Marker (`_SUCCESS` — `lakehouse/application/writer.py`) |
| Ch.8 Data Storage | Horizontal Partitioner (date `yyyy/mm/dd`), Manifest (`_manifest.json`) — `src/data_processing/lakehouse/` |
| Ch.9 Data Quality | Schema Enforcer (Avro registry), một phần Gatekeeper (`validate_order`) |
| Ch.10 Observability | Offline Observer (reconcile Job = drift detection) |

**Gap chính:** Windowed Deduplicator, AWAP, Full Star schema (dbt), Hybrid Consumer, Z-Order.

---

## Cách cập nhật tài liệu

1. PR nào thay đổi pipeline/CDC/storage/manifest data platform → sửa bảng "Trạng thái tổng quan" và mục tương ứng.
2. Đổi trạng thái pattern (`Chưa làm` → `Cấu hình sẵn` → `Đang chạy`) kèm bằng chứng `file:line`.
3. Cập nhật dòng "Cập nhật lần cuối".
