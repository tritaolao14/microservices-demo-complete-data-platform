# Data Platform Integration Architecture

Tài liệu này mô tả kiến trúc và lộ trình tích hợp Data Platform vào dự án Online Boutique (microservices-demo). Việc tích hợp giúp thu thập, xử lý và trực quan hoá dữ liệu hành vi người dùng và dữ liệu giao dịch từ các microservices.

## 1. Tổng quan Kiến trúc (Architecture Overview)

Dự án hiện tại bao gồm các microservices phục vụ giao dịch trực tuyến (OLTP). Để thêm Data Platform (OLAP), chúng ta sử dụng kiến trúc **Event-Driven Data Pipeline**:

- **Nguồn Dữ Liệu (Data Sources):** Các microservices (ví dụ: `checkoutservice`, `cartservice`) đóng vai trò là Producer, sinh ra các sự kiện (events).
- **Trục Dữ Liệu Trung Tâm (Event Streaming):** Sử dụng **Apache Kafka** để tiếp nhận, lưu trữ tạm thời và phân phối các events theo thời gian thực.
- **Hút Dữ Liệu (Data Ingestion):** Một service độc lập (thường viết bằng Python) đóng vai trò Consumer, lấy dữ liệu từ Kafka và ghi xuống Data Lake/Data Warehouse.
- **Phân Tích & Biến Đổi (Data Processing):** Các công cụ như Apache Spark, dbt, hoặc Airflow xử lý dữ liệu thô thành dữ liệu có cấu trúc.

---

## 2. Lộ trình Triển khai (Roadmap)

### Phase 1: Nền tảng Event Streaming (Real-time Checkout Tracking)
*Mục tiêu: Đưa dữ liệu đơn hàng từ hệ thống giao dịch sang hệ thống dữ liệu theo thời gian thực.*

1.  **Triển khai Kafka (Kubernetes):**
    - Cài đặt một cluster Kafka gọn nhẹ (Sử dụng KRaft mode, ví dụ hình ảnh từ `bitnami/kafka`).
    - Khởi tạo topic `orders`.
2.  **Tích hợp Kafka Producer vào `checkoutservice`:**
    - Cập nhật mã nguồn `checkoutservice` (Golang).
    - Thêm thư viện `github.com/segmentio/kafka-go`.
    - Sau khi hàm `PlaceOrder` xử lý thanh toán thành công, định dạng hoá thông tin đơn hàng sang JSON.
    - Bắn JSON payload vào topic `orders`.
3.  **Xây dựng `data-ingestion-service` (Python):**
    - Tạo service Python chạy liên tục để `consume` messages từ topic `orders`.
    - Dịch vụ này có thể in ra log, ghi ra file cục bộ hoặc đẩy vào cơ sở dữ liệu (PostgreSQL/SQLite) làm bước đệm cho Data Warehouse.

### Phase 2: Chụp thay đổi Dữ liệu Cơ sở dữ liệu (CDC - Change Data Capture)
*Mục tiêu: Theo dõi mọi thay đổi trên Product Catalog và Cart mà không ảnh hưởng code.*

-   Thay thế lưu trữ `products.json` tĩnh bằng PostgreSQL.
-   Sử dụng **Debezium** kết nối tới PostgreSQL để tự động capture các lệnh `INSERT`/`UPDATE` trên bảng sản phẩm và bắn thẳng vào Kafka.

#### Đã triển khai (DPFMD-31)

Debezium chạy dưới dạng Kafka Connect worker, chỉ trong overlay dev
(`gitops/overlays/dev/debezium.yaml`) để không ảnh hưởng staging/production:

| Thành phần | Giá trị |
| --- | --- |
| Worker | Image riêng `ghcr.io/tritaolao14/debezium` (FROM `quay.io/debezium/connect:2.7.3.Final` + plugin Avro), REST trên cổng `8083` |
| Connector | `productcatalog-postgres`, `orderitems-postgres` (`io.debezium.connector.postgresql.PostgresConnector`) |
| Bảng nguồn | `public.products`, `analytics.order_items` (`table.include.list`) |
| Plugin | `pgoutput`, snapshot `initial` |
| Slot / publication | `debezium_productcatalog` / `debezium_orderitems`, mỗi cặp một publication riêng (`publication.autocreate.mode=filtered`) |
| Topic output | `cdc_product_changes` (2 partitions), `cdc_order_items` (4 partitions) — xem DPFMD-32 |
| Đăng ký connector | `Job/debezium-register` (reconcile **mọi** connector, không chỉ POST một lần) — xem DPFMD-32 |

Điểm vận hành đáng lưu ý:

-   `postgresql.yaml` phải bật `wal_level=logical`, nếu không connector sẽ không
    validate được.
-   Password nằm trong `Secret/debezium-db`, không nằm trong `ConfigMap`.
    `Job/debezium-register` thay placeholder `__DB_PASSWORD__` bằng giá trị từ
    Secret trước khi apply. Kafka Connect **không** resolve `${file:...}` trong
    connector config, nên không thể dùng `FileConfigProvider` cho `database.password`.
-   ⚠️ `Secret/debezium-db` đang chứa password dạng plain text, và
    `kubernetes-manifests/postgresql.yaml` cũng vậy. Đây là ngoại lệ tạm cho môi
    trường dev: cluster `kind-ci-local` là ephemeral (`scripts/ci-local.sh`), nên
    SealedSecret đã commit sẽ không decrypt được sau `kind delete cluster`.
    Phải chuyển sang SealedSecret/External Secrets trước khi dùng lại pattern này
    cho staging hay production. Xem TODO.md → Sprint 3 → Security.
-   Kafka Connect lưu config của connector trong topic `_debezium_connect_configs`
    dạng **plain text**, nên `database.password` cũng nằm trong đó. Ngoại lệ tạm
    cho dev, giống trên. Xem TODO.md → Sprint 3 → Security.

```bash
# Trạng thái connector
kubectl -n onlineboutique-dev exec deploy/debezium-connect -- \
  curl -sS http://localhost:8083/connectors/productcatalog-postgres/status

# Đọc event (offset cao nhất)
kubectl -n onlineboutique-dev exec deploy/kafka -- \
  kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic cdc_product_changes --partition 0 --offset LAST --max-messages 1

# CDC của đơn hàng: topic này có event liên tục khi load generator đang chạy
kubectl -n onlineboutique-dev exec deploy/kafka -- \
  kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server kafka:9092 \
  --topic cdc_order_items --time -1

# Slot phải active; nếu active=false thì connector chưa stream (xem cảnh báo JMX)
kubectl -n onlineboutique-dev exec deploy/postgres -- psql -U boutique \
  -d product_catalog -c \
  "select slot_name, active from pg_replication_slots order by slot_name;"
```

#### Đã triển khai (DPFMD-32)

Chuẩn hoá output của CDC thành **Avro** trên topic cố định
`cdc_product_changes`, và sửa `Job/debezium-register` để thực sự *reconcile*
config thay vì chỉ đăng ký một lần.

| Thành phần | Giá trị |
| --- | --- |
| Topic | `cdc_product_changes`, 2 partitions, replication factor 1 (`Job/kafka-create-topic`) |
| Định tuyến | SMT `RegexRouter`: `dbserver1\.public\.products` → `cdc_product_changes` |
| Serializer | `io.confluent.connect.avro.AvroConverter` cho cả key và value |
| Schema Registry | `Deployment/schema-registry` (`confluentinc/cp-schema-registry:7.6.1`), `http://schema-registry:8081` |
| Schema history | Vẫn dùng JSON converter (`internal.*.converter`), nên topic `_schema-changes.product_catalog` không đổi format |
| Plugin Avro | Đóng gói trong image riêng `src/debezium` (`kafka-connect-avro-converter:7.6.1`) |
| Reconcile | `Job/debezium-register`: đọc config thật, PUT nếu lệch, chờ `RUNNING`, verify lại |

Vì sao cần `RegexRouter`: Debezium tự đặt tên topic theo
`<topic.prefix>.<schema>.<table>`, tức `dbserver1.public.products`. `topic.routing`
**không** phải property của Debezium (đó là connector option của Kafka Connect
framework, không có trong `PostgresConnector`), nên phải dùng SMT `RegexRouter`
để ép tên topic. Giữ `topic.prefix=dbserver1` vì connector vẫn cần prefix này
cho schema history và tên schema.

Về Avro trên wire: Confluent dùng **magic 1 byte + schema id 4 byte** big-endian
(`00` + `00 00 00 02`), không phải 5 byte như header Avro gốc. Đọc record để kiểm
tra phải dùng đúng độ dài này.

`Job/debezium-register` viết bằng Python (image không có `jq`) và theo thứ tự:

1.  Chờ REST API của worker sẵn sàng.
2.  Đọc config đang chạy thật qua `GET /connectors/<name>/config`.
3.  Nếu connector chưa có → `POST /connectors`.
4.  Nếu có mà lệch với git → `PUT /connectors/<name>/config`.
5.  Chờ `connector.state` và **task** đều `RUNNING`.
6.  Đọc lại config và fail nếu vẫn còn lệch.

Hai điểm cố ý:

-   **Chỉ PUT khi thật sự lệch.** `PUT` luôn restart task, kể cả khi config không
    đổi. Vì ArgoCD chạy lại Job này (`ttlSecondsAfterFinished` hết hạn Job đã xong
    → ArgoCD tạo lại), nếu PUT vô điều kiện thì task restart ~144 lần/ngày.
-   **Chỉ so sánh các key có trong git.** `GET /config` của Kafka Connect trả
    thêm `name` và có thể thêm default của phiên bản sau; đó không phải drift.
    Giá trị `database.password` được so sánh nhưng không bao giờ in ra log.

Job này cũng chính là "monitoring" của pipeline: drift giờ làm Job fail thay vì
bị bỏ qua âm thầm. Trước DPFMD-32, Job chỉ `POST` và coi `409` là thành công, nên
mọi thay đổi config trong git bị bỏ qua nếu connector đã tồn tại.

#### Connector thứ hai: `analytics.order_items`

`public.products` gần như **không đổi** khi app chạy: load generator chỉ tạo
đơn hàng, không sửa catalog. Topic `cdc_product_changes` vì thế gần như trống, và
một connector trên bảng đó không chứng minh được điều gì.

Traffic thật nằm ở `analytics.order_items`, schema `analytics` trong cùng database
`product_catalog`. Luồng: load generator → topic `orders` → `dataingestion` →
`INSERT ... ON CONFLICT DO NOTHING` vào bảng này. Bảng có composite PK
`(order_id, product_id)`, nên Debezium dựng được message key thật thay vì key rỗng.

| Thành phần | Giá trị |
| --- | --- |
| Connector | `orderitems-postgres` |
| Bảng nguồn | `analytics.order_items` |
| `topic.prefix` | `orderdb` (khác `dbserver1`, xem ràng buộc JMX bên dưới) |
| Slot / publication | `debezium_orderitems` / `debezium_orderitems` |
| Schema history | `_schema-changes.order_items` |
| Topic | `cdc_order_items`, 4 partitions, Avro |
| Định tuyến | `RegexRouter`: `orderdb\.analytics\.order_items` → `cdc_order_items` |

Mỗi connector cần `slot.name`, `publication.name` và schema-history topic riêng.
Dùng chung slot sẽ khiến hai connector tranh nhau một replication stream và mất
event.

⚠️ **`topic.prefix` phải khác nhau giữa các connector.** Debezium đặt tên MBean
JMX theo `topic.prefix`, nên hai connector cùng prefix sẽ đăng ký trùng MBean.
Connector thứ hai kẹt trong vòng retry `Unable to register metrics ... retrying`
(12 lần rồi lặp lại vô hạn), task báo `RUNNING` nhưng **luồng streaming không bao
giờ khởi động**: replication slot vẫn `active=false` và topic không có event nào.
Đây là kiểu lỗi nguy hiểm vì mọi thứ "đều xanh" trên status. Đã tái hiện và sửa
bằng cách đặt `topic.prefix=orderdb` cho connector thứ hai.

#### Thêm connector mới

`ConfigMap/debezium-connector-config` dùng **mỗi key là một connector**, và
`Job/debezium-register` duyệt toàn bộ `*.json` trong thư mục mount. Thêm connector
mới chỉ cần:

1.  Thêm một key mới vào ConfigMap, file là `<connector-name>.json` với
    `{"name": ..., "config": {...}}`.
2.  Thêm topic tương ứng vào `CDC_TOPICS` của `Job/kafka-create-topic` theo định
    dạng `"<topic>:<partitions>"`.

Job không dừng ở connector đầu tiên hỏng: nó ghi nhận lỗi, kiểm tra nốt các
connector còn lại, rồi `exit 1` kèm danh sách connector chưa reconcile được. Nhờ
vậy một config hỏng không che mất trạng thái của những connector khác.

Ba điểm vận hành của image `cp-schema-registry:7.6.1`, đều đã tốn thời gian
debug và đều có comment trong manifest:

-   Phải đặt `enableServiceLinks: false`. Nếu không, kubelet inject env
    `SCHEMA_REGISTRY_PORT` từ Service `schema-registry`, và
    `/etc/confluent/docker/configure` coi đó là biến `PORT` deprecated rồi
    `exit 1`. `kubernetes-manifests/kafka.yaml` đã bật cờ này với lý do tương tự.
-   Cần `runAsUser: 1000` (`appuser`) **và** `fsGroup: 994` (`confluent`).
    `/etc/schema-registry` là `775 appuser:root` nên entrypoint bắt buộc uid 1000,
    còn `/var/log/confluent` là `770 cp-schema-registry:confluent` và log4j ghi
    file vào đó. Không user nào đơn lẻ đáp ứng cả hai, nên thêm group thay vì
    chạy bằng root.
-   Job phải tự khai báo `imagePullSecrets`, vì patch `imagePullSecrets` của overlay
    chỉ target `kind: Deployment`, không áp cho `kind: Job`.

Ngoại lệ bảo mật tạm cho dev: Schema Registry không có authentication (chỉ gắn
ClusterIP, không expose ra ngoài cluster). Phải thêm auth và network policy trước
khi dùng lại pattern cho staging/production. Xem TODO.md → Sprint 3 → Security.

```bash
# Schema Registry: các subject đã đăng ký
kubectl -n onlineboutique-dev exec deploy/schema-registry -- \
  curl -sS http://localhost:8081/subjects

# Log reconcile (phát hiện drift, apply, verify)
kubectl -n onlineboutique-dev logs job/debezium-register
```

### Phase 3: Kho Dữ Liệu Trung Tâm (Data Warehouse / Lakehouse)
*Mục tiêu: Lưu trữ dữ liệu tập trung phục vụ phân tích dài hạn.*

-   Triển khai **MinIO** (cho S3-compatible Object Storage) làm Data Lake để chứa raw JSON từ Kafka.
-   Triển khai **Trino** hoặc **ClickHouse** để truy vấn trực tiếp dữ liệu thô này bằng SQL.
-   Thiết lập **Airflow** để điều phối các job chuyển đổi dữ liệu thô thành các mô hình (Star schema) chuẩn cho báo cáo (dùng **dbt**).

### Phase 4: Trực Quan Hoá Dữ Liệu (BI Dashboard)
*Mục tiêu: Báo cáo kinh doanh.*

-   Triển khai **Apache Superset** hoặc **Metabase**.
-   Kết nối với Data Warehouse ở Phase 3.
-   Tạo các Dashboard:
    -   *Tổng doanh thu theo giờ (Real-time)*
    -   *Các sản phẩm bán chạy nhất*
    -   *Sản phẩm có tỷ lệ đưa vào giỏ hàng nhưng không thanh toán cao nhất.*

---

## 3. Tech Stack Gợi ý (Cho môi trường Local/Minikube)
- **Event Bus:** Kafka (Bitnami)
- **Ingestion/Orchestration:** Python, Apache Airflow
- **Data Transformation:** dbt (Data Build Tool)
- **Data Storage:** PostgreSQL (Warehouse nhỏ) / MinIO (Data Lake)
- **BI / Analytics:** Metabase
