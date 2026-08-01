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
