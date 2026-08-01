# Lakehouse Storage Layout

## Mục tiêu
Hướng dẫn cấu trúc lưu trữ cho lakehouse.

## Cấu trúc thư mục chuẩn
```
lakehouse/
├── raw/                 # Dữ liệu thô từ nguồn
│   ├── {dataset_name}/  # Mỗi dataset có thư mục riêng
│   │   ├── {year}/      # Partition theo năm
│   │   │   ├── {month}/  # Partition theo tháng
│   │   │   │   └── {day}/ # Partition theo ngày
│   │   │   └── {date}.parquet # File dữ liệu
│   │   └── {date}.parquet # File dữ liệu không partitioned
│   └── {other_dataset}/
├── bronze/              # Dữ liệu thô đã được xử lý
│   ├── {dataset_name}/
│   └── {other_dataset}/
├── silver/              # Dữ liệu chuẩn hóa
│   ├── {dataset_name}/
│   └── {other_dataset}/
├── gold/                # Dữ liệu curated cho BI/ML
│   ├── {dataset_name}/
│   └── {other_dataset}/
└── metadata/            # Metadata và catalog files
    ├── tables/
    └── schemas/
```

## Partitioning Strategy

### Partition by Date
- Ngày: `2023-01-01`
- Tháng: `2023-01`
- Năm: `2023`

### Partition by Event Type
- `user_events/`
- `order_events/`
- `payment_events/`

### Partition by Region
- `us/`
- `eu/`
- `apac/`

## File Format
1. **Parquet** - Format chính cho dữ liệu
2. **Delta** - Format table cho Delta Lake
3. **Iceberg** - Format table cho Iceberg
4. **Hudi** - Format table cho Hudi

## Naming Convention
- Dataset name: `snake_case`
- File name: `{dataset_name}_{date}.parquet`
- Folder name: `{dataset_name}/`

## Ví dụ cụ thể
```
lakehouse/
├── raw/
│   ├── user_events/
│   │   ├── 2023/
│   │   │   ├── 01/
│   │   │   │   └── 01/
│   │   │   │       └── user_events_2023-01-01.parquet
│   │   │   └── 02/
│   │   │       └── user_events_2023-01-02.parquet
│   │   └── 2023-01-03.parquet
│   └── orders/
├── bronze/
│   ├── user_events/
│   └── orders/
└── silver/
    ├── user_events/
    └── orders/
```

## Chính sách lưu trữ
1. **Retention Policy**: 30 ngày cho raw data, 90 ngày cho bronze
2. **Archival**: Dữ liệu cũ được chuyển đến long-term storage
3. **Compaction**: Tự động merge file nhỏ thành file lớn định kỳ

## Ví dụ cấu hình partitioning
```yaml
partitioning:
  enabled: true
  columns:
    - date
    - region
  strategy: "date"
  format: "yyyy-MM-dd"
```