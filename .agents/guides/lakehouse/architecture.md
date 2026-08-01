# Lakehouse Architecture Guide

## Mục tiêu
Hướng dẫn kiến trúc lakehouse cho toàn bộ nền tảng dữ liệu.

## Tổng quan kiến trúc
Lakehouse là sự kết hợp giữa data lake (lưu trữ rẻ) và data warehouse (table format, engine, catalog).

### Thành phần chính:
1. **Storage**: Object store (S3/GCS/ADLS)
2. **Table Format**: Delta/Iceberg/Hudi
3. **Engine**: Spark/Trino/Flink
4. **Catalog**: Hive, Glue, Unity, Nessie

### Kiến trúc phân tầng:
- **Bronze**: Raw data, không thay đổi schema
- **Silver**: Staging/Conformed data, chuẩn hóa
- **Gold**: Curated/Analytics data, phục vụ BI/ML

## Nguyên tắc thiết kế
1. **Storage rẻ**: Dữ liệu được lưu trên object store
2. **Table format**: Sử dụng Delta/Iceberg/Hudi để quản lý schema và versioning
3. **Engine linh hoạt**: Spark/Trino/Flink cho xử lý batch/streaming
4. **Catalog quản lý metadata**: Hive, Glue, Unity

## Tính năng nổi bật
- Hỗ trợ cả batch và streaming trên cùng một nền tảng
- Schema evolution tự động
- Data quality kiểm soát
- Truy xuất lineage dễ dàng

## Các yêu cầu kỹ thuật
1. Mọi dataset quan trọng phải:
   - Nằm trên object store (S3/GCS/ADLS)
   - Dùng table format (Delta/Iceberg/Hudi)
   - Có catalog/metadata (Hive, Glue, Unity)

2. Chính sách quản lý:
   - Partitioning (theo ngày, event_type…)
   - Retention (xóa/archival theo thời gian)
   - Compaction/optimize định kỳ

## Không được làm
- Không lưu dữ liệu production chỉ dưới dạng file rời (no schema, no version)
- Không bypass table format để đọc/ghi trực tiếp file Parquet/ORC

## Ví dụ cấu trúc thư mục
```
lakehouse/
├── raw/
│   ├── user_events/
│   │   ├── 2023-01-01/
│   │   └── 2023-01-02/
│   └── orders/
├── bronze/
│   ├── user_events/
│   └── orders/
├── silver/
│   ├── user_events/
│   └── orders/
└── gold/
    ├── user_analytics/
    └── order_reports/
```