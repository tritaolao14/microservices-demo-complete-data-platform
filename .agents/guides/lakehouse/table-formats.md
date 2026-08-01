# Lakehouse Table Formats

## Mục tiêu
Hướng dẫn sử dụng các table format trong lakehouse.

## Các table format chính

### 1. Delta Lake
**Đặc điểm:**
- Table format của Delta Lake
- Schema versioning tự động
- ACID transaction
- Data quality kiểm soát

**Ưu điểm:**
- Tốt cho ETL/ELT
- Hỗ trợ streaming và batch
- Metadata quản lý tốt

**Ví dụ sử dụng:**
```python
# Tạo table Delta
df.write.format("delta").save("s3a://bucket/delta_table")

# Truy vấn table Delta
df = spark.read.format("delta").load("s3a://bucket/delta_table")
```

### 2. Iceberg
**Đặc điểm:**
- Format table tiêu chuẩn cho lakehouse
- Hỗ trợ nhiều engine (Spark, Trino, Flink)
- Schema evolution mạnh mẽ

**Ưu điểm:**
- Tương thích đa nền tảng
- Hỗ trợ transaction ACID
- Metadata quản lý tốt

**Ví dụ sử dụng:**
```sql
-- Tạo table Iceberg
CREATE TABLE lakehouse.user_events (
    id STRING,
    name STRING,
    created_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (date);
```

### 3. Hudi
**Đặc điểm:**
- Format table tối ưu cho upserts và streaming
- Hỗ trợ write-once-read-many (WORM)
- Schema versioning

**Ưu điểm:**
- Tốt cho streaming data
- Hỗ trợ upserts hiệu quả
- Metadata quản lý tốt

**Ví dụ sử dụng:**
```yaml
# Hudi table config
hoodie.datasource.write.recordkey.field: "id"
hoodie.datasource.write.partitionpath.field: "date"
hoodie.datasource.write.table.name: "user_events"
```

## So sánh table formats

| Feature | Delta | Iceberg | Hudi |
|---------|-------|---------|------|
| Schema Evolution | ✅ | ✅ | ✅ |
| ACID Transactions | ✅ | ✅ | ✅ |
| Multi-engine Support | ✅ | ✅ | ✅ |
| Streaming Support | ✅ | ✅ | ✅ |
| Partitioning | ✅ | ✅ | ✅ |
| Compaction | ✅ | ✅ | ✅ |

## Khi nào nên dùng table format nào?

### Dùng Delta khi:
- Bạn đang sử dụng Spark
- Cần ETL/ELT pipeline đơn giản
- Muốn dễ dàng quản lý version schema

### Dùng Iceberg khi:
- Bạn cần tương thích đa nền tảng
- Muốn sử dụng với nhiều engine (Spark, Trino, FaaS)
- Cần tiêu chuẩn hóa format

### Dùng Hudi khi:
- Xử lý streaming data
- Cần hiệu quả cho upserts
- Muốn tối ưu cho write-once-read-many

## Ví dụ cấu hình table format

### Delta Table Configuration
```yaml
table_format: "delta"
partitioning:
  enabled: true
  columns: ["date"]
  strategy: "daily"
compaction:
  enabled: true
  interval_days: 7
```

### Iceberg Table Configuration
```yaml
table_format: "iceberg"
partitioning:
  enabled: true
  columns: ["date", "region"]
  strategy: "date"
retention:
  enabled: true
  days: 30
```

### Hudi Table Configuration
```yaml
table_format: "hudi"
write:
  operation: "upsert"
  payload_class: "org.apache.hudi.payload.AvroPayload"
compaction:
  enabled: true
  strategy: "major"
```

## Tối ưu hóa table format

### Delta Table Optimization
```python
# Optimize table
spark.sql("OPTIMIZE delta_table")

# Z-order for better query performance
spark.sql("OPTIMIZE delta_table ZORDER BY (id)")
```

### Iceberg Table Optimization
```sql
-- Compaction
MSCK REPAIR TABLE lakehouse.user_events;

-- Statistics update
ANALYZE TABLE lakehouse.user_events COMPUTE STATISTICS;
```

### Hudi Table Optimization
```bash
# Hoodie compaction
spark-submit \
  --class org.apache.hudi.utilities.HoodieCompactor \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  hoodie-utilities-bundle_2.12-0.13.0.jar \
  --source-path s3a://bucket/hudi_table \
  --target-path s3a://bucket/hudi_table_compacted
```