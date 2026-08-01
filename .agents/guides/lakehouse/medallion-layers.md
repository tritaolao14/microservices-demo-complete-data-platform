# Medallion Layers Architecture

## Mục tiêu
Hướng dẫn kiến trúc phân tầng medallion cho lakehouse.

## Tổng quan kiến trúc
Medallion architecture là mô hình phân tầng dữ liệu phổ biến trong lakehouse, chia dữ liệu thành 3 tầng:

1. **Bronze** (Raw) - Dữ liệu thô từ nguồn
2. **Silver** (Staging/Conformed) - Dữ liệu đã được chuẩn hóa
3. **Gold** (Curated/Analytics) - Dữ liệu đã được xử lý cho mục đích phân tích

## Tầng Bronze (Raw)

### Đặc điểm:
- Dữ liệu thô từ nguồn, không thay đổi schema
- Giữ nguyên bản gốc từ nguồn dữ liệu
- Không có business logic

### Ví dụ:
```
lakehouse/
├── bronze/
│   ├── user_events/
│   │   ├── 2023-01-01/
│   │   │   └── user_events_2023-01-01.json
│   │   └── 2023-01-02/
│   │       └── user_events_2023-01-02.json
│   └── orders/
└── raw/
    ├── user_events/
    └── orders/
```

### Chính sách:
- Không thay đổi dữ liệu
- Lưu trữ theo ngày/tháng/năm
- Metadata về nguồn dữ liệu

### Ví dụ cấu hình:
```yaml
bronze:
  storage: "s3a://bucket/raw"
  retention: "30 days"
  partitioning: "daily"
```

## Tầng Silver (Staging/Conformed)

### Đặc điểm:
- Dữ liệu đã được chuẩn hóa
- Schema được định nghĩa rõ ràng
- Business rules được áp dụng

### Ví dụ:
```
lakehouse/
├── silver/
│   ├── user_events/
│   │   ├── 2023-01-01/
│   │   │   └── user_events_2023-01-01.parquet
│   │   └── 2023-01-02/
│   │       └── user_events_2023-01-02.parquet
│   └── orders/
└── staging/
    ├── user_events/
    └── orders/
```

### Chính sách:
- Áp dụng business rules
- Validation schema
- Metadata về dữ liệu đã xử lý

### Ví dụ cấu hình:
```yaml
silver:
  storage: "s3a://bucket/silver"
  retention: "90 days"
  partitioning: "daily"
  validation:
    - schema_check
    - null_check
```

## Tầng Gold (Curated/Analytics)

### Đặc điểm:
- Dữ liệu đã được xử lý cho mục đích phân tích
- Được tối ưu cho BI/ML
- Có thể có các tính toán phức tạp

### Ví dụ:
```
lakehouse/
├── gold/
│   ├── user_analytics/
│   │   ├── 2023-01-01/
│   │   │   └── user_analytics_2023-01-01.parquet
│   │   └── 2023-01-02/
│   │       └── user_analytics_2023-01-02.parquet
│   └── order_reports/
└── curated/
    ├── user_analytics/
    └── order_reports/
```

### Chính sách:
- Tối ưu cho truy vấn
- Có thể có các tính toán phức tạp
- Metadata về mục đích sử dụng

### Ví dụ cấu hình:
```yaml
gold:
  storage: "s3a://bucket/gold"
  retention: "1 year"
  partitioning: "monthly"
  optimization:
    - statistics_update
    - compaction
```

## Quy trình chuyển đổi giữa các tầng

### Bronze → Silver:
1. Validate schema
2. Apply basic transformations
3. Add metadata

### Silver → Gold:
1. Apply business logic
2. Aggregate data
3. Optimize for queries

## Ví dụ pipeline chuyển đổi

### Pipeline Bronze → Silver:
```python
# Extract raw data from source
raw_df = spark.read.json("s3a://bucket/raw/user_events/")

# Transform to silver layer
silver_df = raw_df.withColumn("processed_at", current_timestamp())

# Write to silver table with proper schema
silver_df.write.format("delta").mode("overwrite").save(
    "s3a://bucket/silver/user_events"
)
```

### Pipeline Silver → Gold:
```python
# Read from silver layer
silver_df = spark.read.format("delta").load(
    "s3a://bucket/silver/user_events"
)

# Apply business logic
gold_df = silver_df.groupBy("user_id").agg(
    count("*").alias("event_count"),
    max("timestamp").alias("last_event")
)

# Write to gold layer
gold_df.write.format("delta").mode("overwrite").save(
    "s3a://bucket/gold/user_analytics"
)
```

## Chính sách quản lý dữ liệu

### Versioning:
- Mỗi tầng có version riêng
- Schema version được theo dõi

### Metadata:
- Mỗi dataset có metadata
- Thông tin về nguồn, xử lý, mục đích

### Truy xuất:
- Có thể truy vết từ gold → silver → bronze
- Lineage được theo dõi

## Ví dụ cấu hình toàn diện

```yaml
medallion_layers:
  bronze:
    name: "bronze"
    storage: "s3a://bucket/bronze"
    retention: "30 days"
    partitioning:
      enabled: true
      columns: ["date"]
      format: "yyyy-MM-dd"
    
  silver:
    name: "silver"
    storage: "s3a://bucket/silver"
    retention: "90 days"
    partitioning:
      enabled: true
      columns: ["date"]
      format: "yyyy-MM-dd"
    validation:
      - schema_validation
      - null_check
    
  gold:
    name: "gold"
    storage: "s3a://bucket/gold"
    retention: "1 year"
    partitioning:
      enabled: true
      columns: ["month"]
      format: "yyyy-MM"
    optimization:
      - statistics_update
      - compaction
```

## Tốt nhất nên làm

1. **Tách biệt các tầng**: Không cho phép join trực tiếp Bronze → Gold
2. **Kiểm soát metadata**: Mỗi tầng có metadata rõ ràng
3. **Áp dụng business rules**: Chỉ áp dụng logic nghiệp vụ ở Silver/Gold
4. **Theo dõi lineage**: Có thể truy vết từ bất kỳ tầng nào đến nguồn
5. **Quản lý version**: Mỗi thay đổi schema đều có versioning