# Data Engineering Patterns — Trạng thái triển khai

Tài liệu này liệt kê **các data engineering pattern đã áp dụng thật trong repo** (kèm bằng chứng code/manifest), những pattern mới chỉ cấu hình, và những pattern còn thiếu theo roadmap. Dùng làm nguồn sự thật duy nhất về "đã làm được gì".

> **Quy tắc bắt buộc:** mỗi PR thay đổi hành vi data platform **phải cập nhật tài liệu này** trong cùng PR. Xem `AGENTS.md` → "Data Platform Documentation Rule".



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
| 14 | Lakehouse / Iceberg Table Format | Đang chạy | `kubernetes-manifests/spark-iceberg-config.yaml`, `kubernetes-manifests/spark-bronze-orders.yaml`, `kubernetes-manifests/spark-bronze-orders.py` |
| 14b | Bronze Layer Writer (Parquet + Manifest) | Đang chạy | `src/data_processing/lakehouse/`, `gitops/overlays/dev/lakehouse-bronze-writer.yaml:23` |
| 14c | Iceberg Bronze Orders Bootstrap Job | Đang chạy | `kubernetes-manifests/spark-bronze-orders.yaml:15`, `kubernetes-manifests/spark-bronze-orders.py:67` |
| 14d | Iceberg Table Maintenance (expire_snapshots + compaction) | Đang chạy | `kubernetes-manifests/spark-iceberg-maintenance.py:76`, `kubernetes-manifests/spark-iceberg-maintenance.yaml:26` |
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

## 14. Lakehouse / Iceberg — Đang chạy

- `ConfigMap/spark-iceberg-config`: JDBC catalog trên Postgres, warehouse `s3a://iceberg-warehouse/`, `S3FileIO` tới MinIO.
- `Job/spark-bronze-orders`: bootstrap one-shot tạo namespace `bronze` và bảng `my_catalog.bronze.orders` (partition `days(_event_date)`), load dữ liệu từ Bronze Parquet landing zone `s3a://lakehouse/bronze/orders/` **chỉ khi bảng rỗng** (idempotent, an toàn khi re-run ArgoCD).
- Chạy trên `apache/spark:3.5.3` với `--packages` (Iceberg runtime, AWS bundle, Hadoop S3A, Postgres JDBC), đọc config từ `spark-iceberg-config` ConfigMap.

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
- **Deployment** (`gitops/overlays/dev/lakehouse-bronze-writer.yaml`): consumer chạy liên tục, `replicas: 1`, `strategy: Recreate` để không có 2 writer cùng ghi một partition ngày, `terminationGracePeriodSeconds: 60` để SIGTERM kịp flush buffer. Image `src/data_processing/Dockerfile` (python:3.12-slim, `requirements.txt` đã pin, chạy non-root uid 10001), build qua artifact `lakehouse` trong `skaffold.yaml`. **Chỉ dev overlay** — staging/production chưa bật vì các tầng phía sau còn đang hoàn thiện; image vẫn phải build trong `skaffold.yaml` để `scripts/ci-local.sh` nạp vào kind, đúng như tiền lệ `debezium`.
- **Offset commit sau khi flush** (DPFMD-47): consumer chạy `enable_auto_commit=False`. Trước đó offset tiến ngay khi message được consume, trong khi writer còn giữ nó trong buffer cho tới rollover (60s / 50k dòng / 128 MiB). Restart trong khoảng đó **mất hẳn message**: Kafka không redeliver vì group offset đã đi trước, còn Bronze thì chưa nhận. Nay chỉ commit sau khi flush trả về kết quả, đổi lại thành at-least-once — crash thì replay batch thay vì mất. Replay an toàn vì tên file theo content hash nên ghi lại rơi vào đúng object cũ.
- **Vòng consume poll thay vì iterate** (DPFMD-49): `flush_if_needed()` chỉ được gọi bên trong `add()`, nên `for message in consumer` **không bao giờ kiểm tra timer khi topic rảnh** — dữ liệu nằm trong RAM vô thời hạn và `BRONZE_FLUSH_INTERVAL_SECONDS` không ràng buộc gì. Nay dùng `consumer.poll(timeout_ms=1000)` và kiểm tra rollover sau **mỗi** lần poll, kể cả poll rỗng; offset cũng được commit sau flush đó (nếu không thì dữ liệu đã ghi mà Kafka vẫn replay).
- **Bắt SIGTERM để flush khi shutdown** (DPFMD-49): mặc định Python bị SIGTERM giết ngay, `finally` không chạy, `writer.close()` không flush — `terminationGracePeriodSeconds: 60` trong Deployment vô dụng. `main()` cài handler ném `SystemExit`, nên vòng lặp thoát bình thường qua `finally`: flush phần còn lại rồi commit offset.
- Đầu vào: Kafka topic `orders` (mặc định) hoặc file JSONL (`--input`) để dry-run; entrypoint `python -m lakehouse.runner` / `bronze_orders_writer.py` (`runner.py`).
- **Đã deploy (DPFMD-46).** Trước đó writer chỉ tồn tại dưới dạng code — DPFMD-34 không kèm Dockerfile/Deployment/skaffold artifact — nên không ai consume topic `orders`: Kafka có 1221 message, consumer group chỉ có `dataingestion`, Bronze zone rỗng. Verify E2E sau khi deploy: topic đạt offset 1305 → `lakehouse/bronze/orders/2026/10/09/` có 2 file Parquet (1300 + 5 rows) + `_manifest.json` (2 entry) + `_SUCCESS`.
- Đã verify E2E cục bộ: 3 event → 2 partition ngày (`2026/10/09`, `2026/10/10`), mỗi partition có Parquet + `_manifest.json` + `_SUCCESS`, đọc lại Parquet thấy đúng schema; 25 unit test (`pytest`, gồm roundtrip pyarrow) + ruff pass.
- Phụ thuộc: `boto3`, `kafka-python`, `pyarrow` (`src/data_processing/requirements.in`).

## 14c. Iceberg Bronze Orders Bootstrap Job — Đang chạy

Job `spark-bronze-orders` (DPFMD-35) bootstrap bảng Iceberg `my_catalog.bronze.orders` từ Bronze landing zone:

- **Manifest**: `kubernetes-manifests/spark-bronze-orders.yaml` (base, áp dụng cho mọi overlay).
- **Driver**: `kubernetes-manifests/spark-bronze-orders.py` (ConfigMap `spark-bronze-orders-script`).
- **Config**: đọc `spark-iceberg-config` ConfigMap (JDBC catalog `my_catalog` trên Postgres `product_catalog`, warehouse `s3a://iceberg-warehouse/`, `S3FileIO` MinIO, Hadoop `s3a` client cho Bronze source).
- **Logic idempotent**: `CREATE TABLE IF NOT EXISTS` → kiểm tra `count(*)` → `INSERT INTO ... SELECT` từ `s3a://lakehouse/bronze/orders/` **chỉ khi bảng rỗng**. Re-run (ArgoCD sync) không duplicate.
- **Bắt buộc `recursiveFileLookup=true`** khi đọc Bronze zone (`spark-bronze-orders.py:68`). Bronze partition theo `{yyyy}/{mm}/{dd}`; để Spark tự suy luận partition sẽ biến các segment số thành partition column, danh sách file thành rỗng và **fail `UNABLE_TO_INFER_SCHEMA` ở mọi thư mục cha** (chỉ leaf directory đọc được). Tắt suy luận thì cây thư mục thành tập file phẳng — không mất cột nào vì `_event_date` đã là cột thật bên trong Parquet. Trước khi sửa, lỗi này khiến Job **exit 0 với bảng rỗng** vì `try/except` nuốt exception — thành công giả, đã verify lại bằng data do chính writer DPFMD-34 sinh ra.
- **Packages**: Iceberg 1.6.1 (`iceberg-spark-runtime-3.5_2.12`, `iceberg-aws-bundle`), Postgres 42.7.3, Hadoop AWS 3.3.4, AWS SDK 1.12.262 (resolve via `--packages` runtime, ivy cache `/tmp/ivy`).
- **Verify E2E**: seed Bronze Parquet → run Job → Iceberg metadata trong Postgres (`iceberg_tables`) + data files trong `s3a://iceberg-warehouse/bronze/orders/` → `SELECT count(*) FROM my_catalog.bronze.orders` trả về số dòng đúng.

| Pattern (knowledge index) | Bằng chứng |
|---------------------------|------------|
| Full Loader (Ch.2) — bootstrap Iceberg table từ Parquet landing zone | `kubernetes-manifests/spark-bronze-orders.py:67` |
| Partitioner (Ch.8) — `days(_event_date)` partition | `kubernetes-manifests/spark-bronze-orders.py:81` |
| Idempotent Writer (Ch.4) — load only khi bảng rỗng | `kubernetes-manifests/spark-bronze-orders.py:73` |
| Declarative Config Reconciliation (Ch.10) — Job base + ConfigMapGenerator | `kubernetes-manifests/kustomization.yaml:35` |

> **Lỗi đã biết (DPFMD-35, phát hiện khi verify DPFMD-36):** `spark.read.parquet("s3a://lakehouse/bronze/orders/")` **không đọc được** — Spark trả `UNABLE_TO_INFER_SCHEMA`. Đo trực tiếp trên cluster: đọc leaf directory (`.../2026/10/08`) thành công, nhưng mọi thư mục cha (`.../2026/10`, `.../2026`, `.../orders`) đều fail. Do đó `try/except` ở `spark-bronze-orders.py:62` nuốt lỗi và **Job vẫn exit 0 với bảng rỗng**. Bronze Parquet do chính DPFMD-34 ghi ra (không phải dữ liệu dựng tay) cũng bị lỗi này. Sửa ở PR riêng.

## 14d. Iceberg Table Maintenance (expire_snapshots + compaction) — Đang chạy

CronJob `spark-iceberg-maintenance` (DPFMD-36) chạy bảo trì định kỳ cho `my_catalog.bronze.orders`.

Ticket DPFMD-36 viết bằng **ngôn ngữ Delta Lake**, nhưng DPFMD-35 tạo bảng **Iceberg** và Iceberg không có lệnh `VACUUM`. Bảng ánh xạ dùng trong driver:

| Delta Lake (ngôn ngữ ticket) | Apache Iceberg (thứ thực sự chạy) |
|-----------------------------|-------------------------------------|
| `VACUUM t RETAIN 7 DAYS` | `CALL my_catalog.system.expire_snapshots(older_than => now()-7d)` |
| `delta.deletedFileRetentionDuration` | `history.expire.max-snapshot-age-ms` |
| `delta.logRetentionDuration` | `history.expire.min-snapshots-to-keep` |
| `OPTIMIZE t ZORDER BY (c)` | `rewrite_data_files(strategy => 'binpack')` |
| *(không có tương đương)* | `remove_orphan_files` |

> **Cảnh báo khi đọc ticket:** copy nguyên cấu hình Delta sang Iceberg **không báo lỗi nhưng cũng không có tác dụng** — Iceberg âm thầm bỏ qua table property lạ (ví dụ `delta.retentionPeriod`, `delta.partitionColumns` đều không tồn tại trong Iceberg). Dạng "thành công giả" này nguy hiểm hơn lỗi rõ ràng.

- **Driver**: `kubernetes-manifests/spark-iceberg-maintenance.py` (ConfigMap `spark-iceberg-maintenance-script`).
- **CronJob**: `kubernetes-manifests/spark-iceberg-maintenance.yaml` — `schedule "23 4 * * *"`, `concurrencyPolicy: Forbid` (expire/rewrite đều commit cùng cấp metadata, không được chạy chồng).
- **Policy nằm trong table properties**, không nhúng trong script: mọi client đọc bảng đều thấy cùng policy. Hằng số trong Python là nguồn sự thật duy nhất, vừa ghi vào bảng vừa truyền cho procedure ⇒ không thể lệch nhau.
- Giá trị: `max-snapshot-age-ms=604800000` (7d, thay default 5d của Iceberg), `min-snapshots-to-keep=24` (thay default 1), compaction `min-input-files=2`, `target-file-size-bytes=128 MiB` (khớp `BRONZE_TARGET_FILE_BYTES` của DPFMD-34).
- **Ràng buộc cứng**: `older_than` của `remove_orphan_files` **phải ≥** `history.expire.max-snapshot-age-ms`. Nếu đặt nhỏ hơn, một file còn được snapshot sống tham chiếu có thể bị xoá → **mất dữ liệu không báo lỗi**. Giá trị hiện tại: 3d.
- `remove_orphan_files` chạy `dry_run=true` mặc định (env `ICEBERG_ORPHAN_DRY_RUN`), chỉ đổi sang `false` sau khi xem kết quả.
- Driver tự bỏ qua nếu bảng không dùng được (`table_exists`) — CronJob chạy trước bootstrap không được phép fail.

**Chưa làm — retention theo tuổi dữ liệu ("30d/90d" trong ticket).** Iceberg **không có** policy tự động xoá partition theo tuổi; phải `DROP PARTITION` thủ công. Quan trọng hơn: hiện Bronze chỉ sync vào Iceberg **một lần lúc bootstrap** (`spark-bronze-orders.py:53`), nên prune Bronze Parquet trước khi có sync định kỳ sẽ xoá dữ liệu chưa từng vào Iceberg. Cần một subtask sync định kỳ trước.

- **Verify E2E**: CronJob chạy exit 0; `SHOW TBLPROPERTIES` xác nhận đủ 4 property đã persist vào Iceberg metadata (`604800000`, `24`, `true`, `10`). *Giới hạn:* bảng chỉ có ≤1 snapshot nên `expire_snapshots` là no-op — test chứng minh procedure chạy đúng, **không** chứng minh việc xoá hoạt động.

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
