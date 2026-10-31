# Data Engineering Patterns — Trạng thái triển khai

Tài liệu này liệt kê **các data engineering pattern đã áp dụng thật trong repo** (kèm bằng chứng code/manifest), những pattern mới chỉ cấu hình, và những pattern còn thiếu theo roadmap. Dùng làm nguồn sự thật duy nhất về "đã làm được gì".

> **Quy tắc bắt buộc:** mỗi PR thay đổi hành vi data platform **phải cập nhật tài liệu này** trong cùng PR. Xem `AGENTS.md` → "Data Platform Documentation Rule".

Cập nhật lần cuối: 2026-10-31 (DPFMD-38: dbt project `src/data_processing/dbt` build staging → marts, `dbt build` PASS=26; `fact_orders_items` grain order-item reconcile với `analytics.order_items` window 2026-10-11..2026-10-15 = 67.013 dòng, diff=0; Trino thêm catalog `postgres` cho reconcile; trước đó DPFMD-37 deploy Trino + Iceberg JDBC catalog, DPFMD-28 chốt reconciliation contract theo grain order-item thay cho baseline 760).


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
| 9 | Declarative Config Reconciliation (drift) | Đang chạy | `initContainer/init-topics` trong `Deployment/debezium-connect`, `Job/debezium-register` (`gitops/overlays/dev/debezium.yaml`) |
| 10 | GitOps (ArgoCD + Kustomize overlays) | Đang chạy | `gitops/overlays/{dev,staging,production}` |
| 11 | Full / Incremental Batch Load + Upsert | Đang chạy | `src/data_processing/seed_database.py` |
| 12 | Dimensional Modeling (một phần) | Đang chạy | `analytics.order_items` trong `seed_database.py:67` |
| 13 | Object Storage / Data Lake | Đang chạy | `kubernetes-manifests/minio.yaml`, `src/data_processing/make_bucket_public.py`, `src/data_processing/lakehouse/sinks.py` |
| 14 | Lakehouse / Iceberg Table Format | Đang chạy | `kubernetes-manifests/spark-iceberg-config.yaml`, `kubernetes-manifests/spark-bronze-orders.yaml`, `kubernetes-manifests/spark-bronze-orders.py` |
| 14b | Bronze Layer Writer (Parquet + Manifest) | Đang chạy | `src/data_processing/lakehouse/`, `gitops/overlays/dev/lakehouse-bronze-writer.yaml:23` |
| 14c | Iceberg Bronze Incremental Sync (watermark `ingest_state`) | Đang chạy | `kubernetes-manifests/spark-bronze-orders.yaml:20`, `kubernetes-manifests/spark-bronze-orders.py:278` |
| 14d | Iceberg Maintenance + Retention (7d snapshot / 90d bronze / 30d raw) | Đang chạy | `kubernetes-manifests/spark-iceberg-maintenance.py:113`, `kubernetes-manifests/spark-iceberg-maintenance.py:140` |
| 15 | Orchestration (Airflow) | Chưa làm | roadmap Phase 3 |
| 16 | Transformation (dbt / Star schema — staging + fact) | Đang chạy | `src/data_processing/dbt/`, `models/staging/stg_orders_items.sql`, `models/marts/fact_orders_items.sql`, `tests/assert_fact_matches_postgres.sql` |
| 17 | Serving / Query Layer (Trino + Iceberg JDBC catalog) | Đang chạy | `kubernetes-manifests/trino.yaml`, `kubernetes-manifests/kustomization.yaml:33` |
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

- `initContainer init-topics` (DPFMD-49): tạo 3 topic nội bộ của Kafka Connect (`_debezium_connect_configs/_offsets/_statuses`) với `cleanup.policy=compact` và đúng số partition (1/25/5) **trước khi** container worker start. Broker chạy `auto.create.topics.enable=true` (cần cho `dataingestion`), nên ai hỏi topic trước cũng tạo được — nhưng broker tạo với `cleanup.policy=delete` và 1 partition, còn Connect thì **từ chối start**:
  `ConfigException: Topic '_debezium_connect_offsets' ... is required to have 'cleanup.policy=compact'`.
  Hậu quả khi thiếu: `/connectors/{name}/config` treo → `Job/debezium-register` timeout; CDC **dừng hẳn** (offset topic = 0 dù đã insert), generation Kafka group lên hơn 2,5 triệu (~14 rebalance/giây) trong khi pod vẫn `Running`. Init idempotent (tạo nếu thiếu, cảnh báo nếu topic tồn tại sai chuẩn), chạy trên image `confluentinc/cp-kafka` (image debezium không có CLI `kafka-topics`) và chờ broker bằng `kafka-broker-api-versions` tối đa 120s.
  Đây là initContainer **trong cùng pod** `Deployment/debezium-connect` (chạy trước container worker theo thứ tự Kubernetes), thay cho `Job/debezium-init-topics` rời trước đây — Job rời không có thứ tự với worker vì ArgoCD apply song song, nên trên cluster sạch worker có thể kịp tự tạo topic sai trước.
- `Job/debezium-register`: GET config thật → POST nếu chưa có → PUT nếu lệch git → chờ `RUNNING` → verify lại (`gitops/overlays/dev/debezium.yaml`).
- **Retry lỗi transport** (DPFMD-50): `request()` thử lại 5 lần với backoff cho `URLError` / `TimeoutError` / `OSError` / `HTTPException`, rồi mới ném `Failed`. HTTP error (404…) **vẫn là giá trị trả về, không retry** vì 404 là nhánh bình thường. Trước đó một response chậm là `TimeoutError` thô bay ra ngoài, giết pod; pod restart, gặp lại response chậm, hết `backoffLimit` ⇒ **reconcile âm thầm dừng hẳn**. Giờ connector đó được báo chưa reconcile và connector còn lại vẫn xử lý.
- Chỉ PUT khi thật sự drift (tránh restart task liên tục); Job fail khi drift ⇒ đóng vai trò monitoring.

## 10. GitOps — Đang chạy

- ArgoCD `Application/dev` với `prune` + `selfHeal` + `CreateNamespace`; Kustomize overlays `dev` / `staging` / `production`.
- CI build kustomize cho cả 3 overlay (`.github/workflows/pre-pr.yaml`).
- **Job trong overlay không được TTL đứng độc lập.** `ttlSecondsAfterFinished` xoá Job sau khi xong, còn ArgoCD vẫn muốn resource đó tồn tại ⇒ app `OutOfSync` trong khoảng ~60s mỗi chu kỳ TTL rồi tự tạo lại. Với 3 Job TTL 600s lệch pha trước đây, app `OutOfSync` ~27% thời gian — không bao giờ ổn định, dù `HEALTH=Healthy`.
  - `Job/debezium-register` và `Job/kafka-create-topic` dùng `argocd.argoproj.io/hook: PostSync` ⇒ ArgoCD không diff chúng, không còn vòng lặp TTL. Cả hai đều có vòng chờ riêng (`wait_for_api` trên REST API Connect; `kafka-broker-api-versions` trên broker) nên an toàn khi chạy PostSync.
  - `initContainer init-topics` **không dùng hook**: phải tạo topic nội bộ *trước khi* worker Kafka Connect khởi động (PostSync chạy sau ⇒ worker đã tự tạo topic sai với `cleanup.policy=delete`). Đã chuyển từ Job rời sang initContainer trong cùng pod `Deployment/debezium-connect`, nên thứ tự tuyệt đối được bảo đảm; file `gitops/overlays/dev/debezium-init-topics.yaml` đã xoá.
  - `CronJob/spark-bronze-orders` (DPFMD-36) thay `Job` TTL một lần ⇒ đối tượng trong git là CronJob ổn định, không còn OutOfSync mỗi giờ; Job con do CronJob sinh ra bị giới hạn `successfulJobsHistoryLimit` + TTL 1h nên không tích tụ.
  - `ttlSecondsAfterFinished` còn lại chỉ là backstop khi `kubectl apply` tay ngoài ArgoCD; hook thành công bị ArgoCD tự xoá.

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
- `CronJob/spark-bronze-orders`: mỗi giờ tạo/giữ bảng `my_catalog.bronze.orders` (partition `days(_event_date)` + `bucket(16, order_id)`) và sync các file Bronze Parquet mới từ `s3a://lakehouse/bronze/orders/` theo watermark `bronze.ingest_state`; merge theo `_row_id` nên an toàn khi chạy lại.
- Chạy trên `apache/spark:3.5.3` với `--packages` (Iceberg runtime, AWS bundle, Hadoop S3A, Postgres JDBC), đọc config từ `spark-iceberg-config` ConfigMap.

## 14b. Bronze Layer Writer (MinIO Parquet) — Đang chạy

Package `src/data_processing/lakehouse/` land raw order event xuống `s3://lakehouse/bronze/orders/{yyyy}/{mm}/{dd}/` dưới dạng **Parquet snappy** + manifest (DPFMD-34). Đây là landing zone thô; bảng Iceberg (DPFMD-35) sẽ bọc lên trên.

Cấu trúc theo **Clean Architecture (Ports & Adapters)**: `domain/` (entity + logic thuần) ← `application/` (use case + ports) ← `infrastructure/` (adapter MinIO/Parquet/env); `runner.py` là composition root, `domain` không phụ thuộc ra ngoài.

| Pattern (knowledge index) | Bằng chứng |
|---------------------------|------------|
| Horizontal Partitioner (Ch.8) — `yyyy/mm/dd`, ngày chuẩn hoá **UTC** | `domain/partitioning.py:31`, `application/layout.py:16`, `application/writer.py:164` |
| Manifest (Ch.8) — reader dùng `_manifest.json` thay vì list S3 | `domain/manifest.py:47`, `domain/manifest.py:54`, `application/writer.py:196` |
| Transactional Writer (Ch.4) — ghi `_staging/` rồi commit atomic (copy) | `application/writer.py:190` |
| Transactional Writer (Ch.4) — buffer chỉ clear khi mọi partition commit xong, fail thì giữ lại để retry | `application/writer.py:148` |
| Proxy / Readiness Marker (Ch.6) — `_SUCCESS` sau khi commit manifest | `application/writer.py:213`, `application/layout.py:11` |
| Circuit Breaker / Retry (Ch.3) — backoff + jitter quanh I/O S3 | `application/retry.py:16`, `application/writer.py` `_call()` |
| Metadata Decorator / Wrapper (Ch.5) — cột `_ingested_at`, `_event_date`, `_source_topic`, `_kafka_partition`, `_kafka_offset`, `_schema_version`, payload gốc ở cột `raw` | `infrastructure/parquet_encoder.py:25`, `domain/records.py` |

- Ports (Dependency Inversion): `ObjectSink`, `RecordEncoder` (`application/ports.py:16`, `application/ports.py:39`); adapter `Boto3Sink`/`InMemorySink` (`infrastructure/sinks.py`) và `ParquetEncoder` (`infrastructure/parquet_encoder.py`).
- Idempotency: tên file `part-<sha256(content)[:16]>.parquet` + `Manifest.upsert` dedupe theo `path` ⇒ replay cùng batch không tạo file trùng (`application/writer.py:164`, `domain/manifest.py:54`).
- Rollover file: `BRONZE_FLUSH_ROWS` (mặc định 50k), `BRONZE_TARGET_FILE_BYTES` (mặc định 128 MiB), `BRONZE_FLUSH_INTERVAL_SECONDS` (mặc định 60s) — `application/rollover.py`, `application/options.py`, `infrastructure/config.py`.
- **Deployment** (`gitops/overlays/dev/lakehouse-bronze-writer.yaml`): consumer chạy liên tục, `replicas: 1`, `strategy: Recreate` để không có 2 writer cùng ghi một partition ngày, `terminationGracePeriodSeconds: 60` để SIGTERM kịp flush buffer. Image `src/data_processing/Dockerfile` (python:3.12-slim, `requirements.txt` đã pin, chạy non-root uid 10001), build qua artifact `lakehouse` trong `skaffold.yaml`. **Chỉ dev overlay** — staging/production chưa bật vì các tầng phía sau còn đang hoàn thiện; image vẫn phải build trong `skaffold.yaml` để `scripts/ci-local.sh` nạp vào kind, đúng như tiền lệ `debezium`.
- **Offset commit sau khi flush** (DPFMD-47): consumer chạy `enable_auto_commit=False`. Trước đó offset tiến ngay khi message được consume, trong khi writer còn giữ nó trong buffer cho tới rollover (60s / 50k dòng / 128 MiB). Restart trong khoảng đó **mất hẳn message**: Kafka không redeliver vì group offset đã đi trước, còn Bronze thì chưa nhận. Nay chỉ commit sau khi flush trả về kết quả, đổi lại thành at-least-once — crash thì replay batch thay vì mất. Replay an toàn vì tên file theo content hash nên ghi lại rơi vào đúng object cũ.
- **Flush giữ buffer tới khi commit thành công** (DPFMD-48): buffer chỉ `clear()` **sau khi mọi partition đã commit**. Trước đó `flush()` drain buffer rồi mới commit — commit fail là mất sạch record trong RAM, không còn gì để retry, và module docstring tự claim "Transactional Writer (Ch.4)" trong khi hành vi thì không. Partition đã commit rồi sẽ bị ghi lại khi retry, nhưng vô hại: tên file theo content hash và manifest upsert dedupe theo path.
- **Vòng consume poll thay vì iterate** (DPFMD-49): `flush_if_needed()` chỉ được gọi bên trong `add()`, nên `for message in consumer` **không bao giờ kiểm tra timer khi topic rảnh** — dữ liệu nằm trong RAM vô thời hạn và `BRONZE_FLUSH_INTERVAL_SECONDS` không ràng buộc gì. Nay dùng `consumer.poll(timeout_ms=1000)` và kiểm tra rollover sau **mỗi** lần poll, kể cả poll rỗng; offset cũng được commit sau flush đó (nếu không thì dữ liệu đã ghi mà Kafka vẫn replay).
- **Bắt SIGTERM để flush khi shutdown** (DPFMD-49): mặc định Python bị SIGTERM giết ngay, `finally` không chạy, `writer.close()` không flush — `terminationGracePeriodSeconds: 60` trong Deployment vô dụng. `main()` cài handler ném `SystemExit`, nên vòng lặp thoát bình thường qua `finally`: flush phần còn lại rồi commit offset.
- Đầu vào: Kafka topic `orders` (mặc định) hoặc file JSONL (`--input`) để dry-run; entrypoint `python -m lakehouse.runner` / `bronze_orders_writer.py` (`runner.py`).
- **Đã deploy (DPFMD-46).** Trước đó writer chỉ tồn tại dưới dạng code — DPFMD-34 không kèm Dockerfile/Deployment/skaffold artifact — nên không ai consume topic `orders`: Kafka có 1221 message, consumer group chỉ có `dataingestion`, Bronze zone rỗng. Verify E2E sau khi deploy: topic đạt offset 1305 → `lakehouse/bronze/orders/2026/10/09/` có 2 file Parquet (1300 + 5 rows) + `_manifest.json` (2 entry) + `_SUCCESS`.
- Đã verify E2E cục bộ: 3 event → 2 partition ngày (`2026/10/09`, `2026/10/10`), mỗi partition có Parquet + `_manifest.json` + `_SUCCESS`, đọc lại Parquet thấy đúng schema; 25 unit test (`pytest`, gồm roundtrip pyarrow) + ruff pass.
- Phụ thuộc: `boto3`, `kafka-python`, `pyarrow` (`src/data_processing/requirements.in`).

## 14c. Iceberg Bronze Incremental Sync (CronJob + watermark) — Đang chạy

CronJob `spark-bronze-orders` (DPFMD-36, nối tiếp DPFMD-35) sync bảng Iceberg `my_catalog.bronze.orders` từ Bronze landing zone mỗi giờ (`37 * * * *`, `concurrencyPolicy: Forbid`):

- **Manifest**: `kubernetes-manifests/spark-bronze-orders.yaml` (base, áp dụng cho mọi overlay). Trước đây là `Job` TTL — chỉ chạy khi ArgoCD sync, nên bảng chỉ mới bằng lần sync gần nhất và `ttlSecondsAfterFinished` sinh OutOfSync mỗi giờ.
- **Driver**: `kubernetes-manifests/spark-bronze-orders.py` (ConfigMap `spark-bronze-orders-script`). `BRONZE_SYNC_MODE=incremental` (mặc định, env trong CronJob); `bootstrap` chỉ còn là escape hatch chạy tay khi bảng bị xoá.
- **Watermark**: bảng `my_catalog.bronze.ingest_state` (`source_path`, `rows`, `file_created_at`, `synced_at`) ghi tập file đã nạp. Mỗi vòng đọc `_manifest.json` của các partition ngày (không list toàn bucket), anti-join với state, sắp xếp theo path (cũ nhất trước) và giới hạn `BRONZE_SYNC_MAX_FILES` (mặc định 2000) để một backlog lớn được drain qua nhiều vòng thay vì một commit dài.
- **Idempotency 3 lớp**: (1) file đã có trong state thì bỏ qua; (2) mỗi dòng có `_row_id = sha256(raw)` — `raw` chứa `order_id` do checkout sinh nên unique, replay Kafka ở offset mới vẫn trùng id và được dedupe; (3) `MERGE INTO ... ON _row_id` xử lý trường hợp crash giữa commit và ghi state. State chỉ được ghi **sau** khi merge commit.
- **Migration DPFMD-35**: bảng cũ có 350 dòng với `_row_id = NULL`. Driver backfill `order_id = get_json_object(raw,'$.order_id')` và `_row_id` trước lần sync đầu, nên 350 dòng cũ được match thay vì insert lại; toàn bộ backlog chưa từng nạp vẫn được đưa vào.
- **Bucket 16**: ticket ghi "Bucket 16 on user_id" nhưng event `orders` không có `user_id`, nên dùng khóa cardinality cao tự nhiên là `order_id`: `PARTITIONED BY (days(_event_date), bucket(16, order_id))`. Bảng cũ được `ALTER TABLE ... ADD PARTITION FIELD` (metadata-only, dữ liệu cũ giữ spec cũ).
- **Bắt buộc `recursiveFileLookup=true`** ở chế độ bootstrap (`spark-bronze-orders.py:260`). Bronze partition theo `{yyyy}/{mm}/{dd}`; để Spark tự suy luận partition sẽ biến các segment số thành partition column, danh sách file thành rỗng và **fail `UNABLE_TO_INFER_SCHEMA` ở mọi thư mục cha**. Tắt suy luận thì cây thư mục thành tập file phẳng — không mất cột nào vì `_event_date` đã là cột thật bên trong Parquet. Trước khi sửa, lỗi này khiến Job **exit 0 với bảng rỗng** vì `try/except` nuốt exception — thành công giả.
- **Packages**: Iceberg 1.6.1 (`iceberg-spark-runtime-3.5_2.12`, `iceberg-aws-bundle`), Postgres 42.7.3, Hadoop AWS 3.3.4, AWS SDK 1.12.262 (resolve via `--packages` runtime, ivy cache `/tmp/ivy`).
- **Verify E2E (kind-ci-local)**: bảng 350 dòng (bootstrap DPFMD-35) → sync 3 file/13 dòng → 363; sync tiếp 3 file → 377 dòng / 6 file trong `ingest_state`; xoá 3 state row để mô phỏng crash giữa MERGE và ghi state → sync lại thêm **0 dòng** (MERGE match `_row_id`), state trở lại 6. Kết quả cuối: `rows=377`, `distinct_row_ids=377`, `null_row_ids=0`, `duplicate_keys=0`; maintenance compaction sau đó không đổi số dòng.

| Pattern (knowledge index) | Bằng chứng |
|---------------------------|------------|
| Incremental Loader / watermark (Ch.2) — `ingest_state` anti-join | `kubernetes-manifests/spark-bronze-orders.py:278` |
| Idempotent Writer (Ch.4) — `_row_id` + `MERGE` + state sau commit | `kubernetes-manifests/spark-bronze-orders.py:278` |
| Partitioner (Ch.8) — `days(_event_date)` + `bucket(16, order_id)` | `kubernetes-manifests/spark-bronze-orders.py:98` |
| Full Loader (Ch.2) — bootstrap thủ công khi bảng rỗng | `kubernetes-manifests/spark-bronze-orders.py:240` |
| Declarative Config Reconciliation (Ch.10) — CronJob base + ConfigMapGenerator | `kubernetes-manifests/kustomization.yaml:35` |

## 14d. Iceberg Maintenance + Data Retention — Đang chạy

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
- **`remove_orphan_files`**: `older_than` (3d) chỉ cần lớn hơn thời gian một write đang chạy — file đang upload chưa có trong metadata nên trông như orphan. 3d an toàn tuyệt đối cho một Spark batch; **không** liên quan tới snapshot window (snapshot sống luôn được đối chiếu qua metadata trước khi xoá).
- `remove_orphan_files` chạy `dry_run=true` mặc định (env `ICEBERG_ORPHAN_DRY_RUN`), chỉ đổi sang `false` sau khi xem kết quả.
- Driver tự bỏ qua nếu bảng không dùng được (`table_exists`) — CronJob chạy trước bootstrap không được phép fail.

**Retention theo tuổi dữ liệu ("30d/90d" trong ticket) — đã làm.** Ticket viết theo Delta Lake ("bronze 30d, silver 90d"); repo chỉ có raw và bronze, và `governance.md` quy định raw 30d / bronze 90d. Ánh xạ:

| Tầng | Thời gian | Cơ chế |
|------|-----------|--------|
| raw — topic Kafka `orders` | 30d | `retention.ms=2592000000` trong `Job/kafka-create-topic` (`gitops/overlays/dev/schema-registry.yaml`), alter cả topic đã tồn tại |
| raw — Parquet landing zone | 30d | `prune_landing_zone()` xoá object đã có trong `bronze.ingest_state` và `file_created_at` quá hạn (`spark-iceberg-maintenance.py:140`); file chưa sync **không bao giờ** bị xoá |
| bronze — bảng Iceberg | 90d | `DELETE FROM ... WHERE _event_date < current_date - 90d` (`spark-iceberg-maintenance.py:113`); Iceberg không có policy tự động nên đây là DELETE tường minh |

Hai env `RAW_RETENTION_DAYS` / `BRONZE_RETENTION_DAYS` trong CronJob manifest cho phép chỉnh window; `RAW_PRUNE_DRY_RUN=true` để chỉ báo candidate mà không xoá.

- **Verify E2E (kind-ci-local)**: driver chạy exit 0; `SHOW TBLPROPERTIES` xác nhận đủ 4 property đã persist (`604800000`, `24`, `true`, `10`). Với dữ liệu 2026-10-09/10 và hôm nay 2026-10-31, cutoff 90d = 2026-08-02 và 30d = 2026-10-01 → cả hai no-op đúng như log (`nothing to drop`, `nothing to prune`), chứng minh nhánh tính cutoff + query chạy đúng; `orders` retention.ms xác nhận `2592000000` trên cluster. Compaction, `expire_snapshots` và `remove_orphan_files` (dry-run) đều exit 0.

## 14e. Reconciliation contract — Bronze ↔ `analytics.order_items` (chuẩn cho dbt) — Đang áp dụng

AC ban đầu của DPFMD-28 ("Trino `count(*) iceberg.bronze.orders` == Postgres `analytics.order_items`, 760 rows baseline") có hai lỗi; từ 2026-10-31 dùng contract dưới đây.

- **Lỗi grain**: `bronze.orders` là 1 dòng / order event (1 Kafka message, `items` lồng trong `raw`), còn `analytics.order_items` là 1 dòng / `(order_id, product_id)`. Hai count không cùng đơn vị. Phải explode `raw.items` rồi dedupe `(order_id, product_id)` — đúng PK và `ON CONFLICT DO NOTHING` của `PostgresWriter` (`src/dataingestion/infrastructure/postgres_writer.py:19`).
- **Lỗi baseline/window**: `760` chỉ là số item của ngày seed đầu tiên `2026-08-27`, không phải tổng. Postgres có dữ liệu từ 27/08; Kafka `orders` giữ 30d nên Bronze chỉ bắt đầu từ 09/10 và còn backlog sau 16/10 ⇒ so tổng luôn lệch. Chỉ so trong **complete window** mà cả hai phía đã đủ.
- **AC mới**: window `2026-10-11` → `2026-10-15`; `fact_orders_items` (grain order item) = **67.013** dòng, bằng `count(*) analytics.order_items` cùng window (diff = 0). Breakdown: 16.544 / 14.915 / 15.325 / 13.209 / 7.020. Khi Bronze drain hết backlog thì mở rộng window; `760` chỉ còn là smoke test ngày đầu.
- **Bằng chứng**: raw Bronze trong window = 27.137 order events (không phải 67.013) — minh hoạ lỗi grain; explode + dedupe `(order_id, product_id)` = 67.013, khớp Postgres từng ngày.
- **Cấu trúc raw**: `items[].item.product_id`, `items[].item.quantity`, `items[].cost.{currency_code,units,nanos}`; `unit_price = units + nanos/1e9`, `total_price = quantity * unit_price`, `event_timestamp = raw.timestamp`; payload không có `status` nên dùng `SUCCEED` như `deserializer.py:30`.
- **Vai trò**: dbt build từ Iceberg (nguồn sự thật của event history); Postgres `analytics.order_items` chỉ là reference để reconcile, không phải dependency runtime.
- **Reconcile SQL** (Trino):
  ```sql
  SELECT count(*) FROM (
    SELECT order_id,
           json_extract_scalar(t.item, '$.item.product_id') AS product_id
    FROM iceberg.bronze.orders
    CROSS JOIN UNNEST(CAST(json_extract(raw, '$.items') AS array(json))) AS t(item)
    WHERE _event_date BETWEEN DATE '2026-10-11' AND DATE '2026-10-15'
    GROUP BY 1, 2
  );
  ```
  Kỳ vọng `67013`; đối chiếu `SELECT count(*) FROM analytics.order_items WHERE event_timestamp::date BETWEEN '2026-10-11' AND '2026-10-15';`.
- **Thực thi trong dbt**: `src/data_processing/dbt/tests/assert_fact_matches_postgres.sql` hiện thực hoá contract này — `dbt build` fail nếu `item_count`, `unit_count` hoặc `revenue` lệch `postgres.analytics.order_items`, hoặc `item_count ≠ 67013`. Lần verify 2026-10-31 test PASS.

## 16. Transformation (dbt) — Đang chạy (staging + fact; chưa full star schema)

dbt project `src/data_processing/dbt` (dbt-core 1.10.23 + dbt-trino 1.10.6) biến đổi Bronze Iceberg thành Silver/Gold và chạy test data quality trong cùng một lệnh `dbt build`:

- **Project**: `dbt_project.yml` (staging `+schema: silver`, marts `+schema: gold`, materialized `table`), `profiles.yml` (Trino catalog `iceberg`, host `trino:8080`, method `none`), macro `macros/generate_schema_name.sql` để `+schema` thắng `target.schema`.
- **Staging** (`models/staging/stg_orders_items.sql`): `source('bronze','orders')` → `CROSS JOIN UNNEST(CAST(json_extract(raw, '$.items') AS array(json)))` để hạ grain từ order event xuống order item; parse `$.item.product_id`, `$.item.quantity`, `$.cost.{currency_code,units,nanos}`, `raw.timestamp`; dedupe `(order_id, product_id)` bằng `row_number()` theo `_ingested_at, _kafka_partition, _kafka_offset` (giữ bản ghi mới nhất), khớp PK `analytics.order_items`.
- **Mart** (`models/marts/fact_orders_items.sql`): fact grain `(order_id, product_id)`, `unit_price = units + nanos/1e9`, `total_price = quantity * unit_price`, kèm `event_timestamp`, `event_date`, `status = 'SUCCEED'`.
- **Data tests**: `not_null` + `accepted_values` trong `models/{staging,marts}/schema.yml`; `tests/assert_fact_orders_items_unique.sql` (không có cặp `(order_id, product_id)` trùng); `tests/assert_fact_matches_postgres.sql` reconcile với `postgres.analytics.order_items` trong window `2026-10-11..2026-10-15`.
- **Chạy**: `CronJob/dbt-build` (`gitops/overlays/dev/dbt.yaml`) chạy `dbt build` lúc `50 * * * *`, sau Bronze sync `:37`; đọc Trino trên cluster, ghi `iceberg.silver` / `iceberg.gold`. Image `src/data_processing/Dockerfile` cài dbt trong venv riêng `/opt/dbt` để không xung đột pin của writer.
- **Verify (kind-ci-local, 2026-10-31)**: `dbt build` → **PASS=26** (2 models + 24 tests), 0 error; `iceberg.gold.fact_orders_items` = 84.600 dòng tổng, window `2026-10-11..2026-10-15` = **67.013** (khớp Postgres, diff=0); `sum(quantity)=451.674`, `sum(total_price)=336.524.180,88`.
- **Giới hạn**: mới có grain order-item; chưa có bảng dimension / full star schema và chưa có orchestration upstream (drain Bronze backlog) — thuộc Phase 3–4.

## 17. Serving / Query Layer — Trino + Iceberg JDBC catalog — Đang chạy

Trino 476 single-node đọc trực tiếp bảng Iceberg do Spark ghi, không cần Hive Metastore hay REST catalog:

- **Manifest**: `kubernetes-manifests/trino.yaml` — ConfigMap `trino-config` (`config.properties`, `node.properties`, `jvm.config`), ConfigMap `trino-catalog` (`iceberg.properties`, `postgres.properties`), `Deployment/trino` và `Service/trino:8080`; khai báo trong base `kubernetes-manifests/kustomization.yaml:33` nên mọi overlay đều có.
- **Catalog**: `iceberg.catalog.type=jdbc` trỏ vào Postgres `product_catalog` — đúng database Spark đang dùng. `iceberg.jdbc-catalog.catalog-name=my_catalog` **bắt buộc** khớp `iceberg_tables.catalog_name` do Spark ghi; đổi tên sẽ khiến mọi bảng Spark tạo trở nên vô hình. `default-warehouse-dir=s3a://iceberg-warehouse/` giữ nguyên scheme `s3a` mà Spark đã ghi trong metadata.
- **MinIO**: `fs.native-s3.enabled=true` + `s3.endpoint=http://minio:9000`, path-style; Trino native S3 đọc được location `s3a://` của bảng nên không cần Hadoop S3A.
- **JDBC catalog schema**: `iceberg_tables` do Iceberg 1.6.1 (Spark) tạo tương thích với schema-version mặc định V1 của Trino; không cần migrate.
- **Verify E2E (kind-ci-local)**: pod `1/1 Running`, `SHOW CATALOGS` → `iceberg`, `postgres`, `system`; `SHOW SCHEMAS FROM iceberg` → `bronze`; `SHOW TABLES FROM iceberg.bronze` → `ingest_state`, `orders`; `SELECT count(*) FROM iceberg.bronze.orders` = **34.294** (đúng số dòng Iceberg tại thời điểm verify DPFMD-37); `SELECT count(*) ... WHERE _event_date = DATE '2026-10-10'` = **4.728**, khớp kết quả Spark; `SHOW CREATE TABLE` trả về `partitioning = ARRAY['day(_event_date)','bucket(order_id, 16)']`, `location = 's3a://iceberg-warehouse/bronze/orders'`.
- **Catalog `postgres`**: `postgres.properties` trỏ thẳng vào `product_catalog` (cùng instance, user `boutique`) để dbt và truy vấn tay đọc được `postgres.analytics.order_items`; đây là reference cho reconcile ở mục 14e, không phải dependency runtime của pipeline.
- **Giới hạn hiện tại**: 1 coordinator, chưa HA; credentials nằm trong ConfigMap (cùng pattern với Postgres/Spark, xử lý ở Sprint 3).

## 15, 18. Chưa làm

- **Orchestration (Airflow)**, **BI (Superset/Metabase)** — thuộc Phase 3–4 trong `docs/data-platform.md`.
- **Transformation**: đã có staging + fact ở mục 16; full star schema (dimension, bridge) vẫn còn thiếu.
- **Security & Governance** (SealedSecret, Schema Registry auth, mã hoá config topic) — Sprint 3 trong `TODO.md`.

---

## Map sang knowledge index

Đối chiếu với `.agents/knowledge/data-engineering-design-patterns-index.md` (sách Konieczny):

| Nhóm chapter | Đã áp trong repo |
|--------------|------------------|
| Ch.2 Data Ingestion | Full Loader (`seed_database`), Incremental (order_items theo `event_timestamp`), Incremental Loader/watermark (`spark-bronze-orders` + `ingest_state`), CDC Replication (Debezium) |
| Ch.3 Error Management | Dead-Letter (DLQ), Circuit Breaker/Retry |
| Ch.4 Idempotency | Keyed Idempotency / Idempotent Writer (`ON CONFLICT DO NOTHING`), Transactional Writer (`lakehouse/application/writer.py`) |
| Ch.5 Data Value | Metadata Decorator (cột metadata Bronze — `lakehouse/infrastructure/parquet_encoder.py`) |
| Ch.6 Data Flow | Proxy / Readiness Marker (`_SUCCESS` — `lakehouse/application/writer.py`) |
| Ch.8 Data Storage | Horizontal Partitioner (date `yyyy/mm/dd`), Manifest (`_manifest.json`) — `src/data_processing/lakehouse/`; Iceberg `days(_event_date)` + `bucket(16, order_id)` |
| Ch.9 Data Quality | Schema Enforcer (Avro registry), một phần Gatekeeper (`validate_order`), dbt data tests + reconcile test (`src/data_processing/dbt/tests/`) |
| Ch.10 Observability | Offline Observer (reconcile Job = drift detection) |

**Gap chính:** Windowed Deduplicator, AWAP, full Star schema (dbt mới có staging + fact), Hybrid Consumer, Z-Order.

---

## Cách cập nhật tài liệu

1. PR nào thay đổi pipeline/CDC/storage/manifest data platform → sửa bảng "Trạng thái tổng quan" và mục tương ứng.
2. Đổi trạng thái pattern (`Chưa làm` → `Cấu hình sẵn` → `Đang chạy`) kèm bằng chứng `file:line`.
3. Cập nhật dòng "Cập nhật lần cuối".
