# Lakehouse Performance Tuning

## Mục tiêu
Hướng dẫn tối ưu hiệu suất cho lakehouse.

## Tổng quan tối ưu

### Tối ưu hiệu suất là quá trình cải thiện tốc độ xử lý, giảm chi phí và tăng khả năng mở rộng của hệ thống lakehouse.

## Partitioning Strategy

### Tối ưu partitioning:
1. **Partition size**: Mỗi partition nên có kích thước từ 100MB - 1GB
2. **Partition count**: Tránh quá nhiều partition nhỏ (dưới 100MB)
3. **Partition key**: Chọn key phù hợp với truy vấn

### Ví dụ cấu hình:
```yaml
partitioning:
  enabled: true
  key: "date"
  strategy: "daily"
  size_limit: "1GB"
  
  # Optimize for query patterns
  query_optimization:
    - field: "date"
      order: "ascending"
    - field: "region"
      order: "ascending"
```

### Ví dụ code partitioning:
```python
# Tạo table với partitioning tối ưu
df = spark.read.parquet("s3a://bucket/data")

# Partition by date and region for optimal query performance
df.write.format("delta") \
    .mode("overwrite") \
    .option("path", "s3a://bucket/output") \
    .saveAsTable("lakehouse.user_events")

# Cấu hình partitioning trong table
spark.sql("""
    ALTER TABLE lakehouse.user_events 
    SET TBLPROPERTIES (
        'delta.partitionColumns' = 'date,region'
    )
""")
```

## File Format Optimization

### Tối ưu file format:
1. **Parquet**: Format tốt cho truy vấn
2. **Delta**: Format tốt cho ETL/ELT
3. **Iceberg**: Format tiêu chuẩn cho lakehouse

### Ví dụ cấu hình:
```yaml
file_format:
  primary: "parquet"
  compression: "snappy"
  row_group_size: "128KB"
  
  # Delta format specific
  delta:
    enable_optimize: true
    auto_compact: true
```

### Ví dụ tối ưu file:
```python
# Tối ưu file size cho truy vấn
df.write.format("delta") \
    .option("compression", "snappy") \
    .option("maxRecordsPerFile", 100000) \
    .mode("overwrite") \
    .save("s3a://bucket/output")

# Sử dụng Z-order cho truy vấn hiệu quả
spark.sql("""
    OPTIMIZE lakehouse.user_events 
    ZORDER BY (user_id, event_type)
""")
```

## Memory Management

### Tối ưu memory:
1. **Spark configuration**: Cấu hình bộ nhớ phù hợp
2. **Caching**: Cache dữ liệu thường dùng
3. **Broadcast join**: Dùng broadcast join cho table nhỏ

### Ví dụ cấu hình Spark:
```yaml
spark_config:
  memory:
    driver: "4g"
    executor: "8g"
    executor_memory_fraction: 0.8
    
  shuffle:
    partitions: 200
    file_buffer_size: "64k"
    
  adaptive:
    enabled: true
    coalesce_partitions: true
```

### Ví dụ code memory management:
```python
# Cấu hình Spark session tối ưu
spark = SparkSession.builder \
    .appName("Optimized Processing") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Cache dữ liệu thường dùng
cached_df = df.cache()
```

## Query Optimization

### Tối ưu truy vấn:
1. **Predicate pushdown**: Giảm lượng dữ liệu đọc
2. **Column pruning**: Chỉ đọc các cột cần thiết
3. **Partition pruning**: Chỉ đọc partition phù hợp

### Ví dụ truy vấn tối ưu:
```sql
-- Truy vấn tối ưu với predicate pushdown
SELECT user_id, event_type, count(*) as event_count
FROM lakehouse.user_events 
WHERE date >= '2023-01-01' 
  AND region = 'us'
GROUP BY user_id, event_type
ORDER BY event_count DESC
LIMIT 1000;

-- Sử dụng partition pruning
SELECT * FROM lakehouse.user_events 
WHERE date BETWEEN '2023-01-01' AND '2023-01-31'
  AND region = 'us';
```

### Ví dụ code truy vấn tối ưu:
```python
# Tối ưu truy vấn với filter early
df_filtered = df.filter(df.date >= "2023-01-01") \
    .filter(df.region == "us")

# Chỉ chọn cột cần thiết
df_optimized = df_filtered.select("user_id", "event_type", "timestamp")

# Group by và aggregate
result = df_optimized.groupBy("user_id", "event_type") \
    .agg(count("*").alias("event_count")) \
    .orderBy(desc("event_count")) \
    .limit(1000)
```

## Compaction Strategy

### Tối ưu compaction:
1. **Delta table**: Auto-compact và optimize
2. **Iceberg table**: Định kỳ compaction
3. **Hudi table**: Major compaction

### Ví dụ cấu hình compaction:
```yaml
compaction:
  delta:
    auto_optimize: true
    auto_compact: true
    min_file_size: "10MB"
    
  iceberg:
    enabled: true
    strategy: "minor"
    interval_days: 7
    
  hudi:
    enabled: true
    strategy: "major"
    interval_days: 14
```

### Ví dụ code compaction:
```python
# Tối ưu Delta table
spark.sql("OPTIMIZE lakehouse.user_events")

# Compaction với Z-order
spark.sql("""
    OPTIMIZE lakehouse.user_events 
    ZORDER BY (user_id, event_type)
""")

# Compaction định kỳ
spark.sql("""
    MSCK REPAIR TABLE lakehouse.user_events
""")
```

## Data Skew Handling

### Xử lý skew dữ liệu:
1. **Bucketing**: Phân vùng dữ liệu để giảm skew
2. **Salting**: Thêm salt vào key để phân bố đều

### Ví dụ bucketing:
```python
# Tạo table với bucketing
df.write.format("delta") \
    .mode("overwrite") \
    .option("path", "s3a://bucket/output") \
    .saveAsTable("lakehouse.user_events")

# Cấu hình bucketing
spark.sql("""
    ALTER TABLE lakehouse.user_events 
    SET TBLPROPERTIES (
        'delta.bucketing.numBuckets' = '16',
        'delta.bucketing.bucketColumn' = 'user_id'
    )
""")
```

### Ví dụ salting:
```python
# Thêm salt cho key để giảm skew
from pyspark.sql.functions import *

def add_salt(df, column_name, salt_count=16):
    """Add salt to column to reduce skew."""
    salt = (hash(column_name) % salt_count).cast("int")
    return df.withColumn(f"{column_name}_salted", 
                        concat(col(column_name), lit("_"), salt))

# Sử dụng salt cho join
df_with_salt = add_salt(df1, "user_id")
```

## Caching Strategy

### Tối ưu caching:
1. **Cache frequent queries**: Cache dữ liệu thường dùng
2. **Eviction policy**: Cấu hình chính sách xóa cache

### Ví dụ cấu hình caching:
```yaml
caching:
  enabled: true
  cache_type: "memory"
  eviction_policy: "LRU"
  max_memory_percent: 40
  
  # Cache policy for specific queries
  query_cache:
    - name: "user_summary"
      query: "SELECT user_id, count(*) FROM lakehouse.user_events GROUP BY user_id"
      cache_duration: "1 hour"
```

### Ví dụ code caching:
```python
# Cache dữ liệu thường dùng
user_events_cached = spark.read.format("delta").load(
    "s3a://bucket/user_events"
).cache()

# Sử dụng cache cho các truy vấn
summary_df = user_events_cached.groupBy("user_id").count()
```

## Monitoring and Metrics

### Theo dõi hiệu suất:
1. **Metrics**: Theo dõi các chỉ số quan trọng
2. **Alerts**: Cảnh báo khi có vấn đề hiệu suất

### Ví dụ cấu hình monitoring:
```yaml
monitoring:
  metrics:
    - name: "query_duration"
      description: "Duration of queries in seconds"
      type: "gauge"
      
    - name: "memory_usage"
      description: "Memory usage percentage"
      type: "gauge"
      
    - name: "file_count"
      description: "Number of files processed"
      type: "counter"
      
  alerts:
    - name: "high_query_time"
      condition: "query_duration > 300 seconds"
      severity: "warning"
      
    - name: "memory_exhaustion"
      condition: "memory_usage > 80%"
      severity: "critical"
```

## Ví dụ toàn bộ cấu hình tối ưu:

```yaml
performance_tuning:
  partitioning:
    enabled: true
    key: "date"
    strategy: "daily"
    size_limit: "1GB"
    
  file_format:
    primary: "parquet"
    compression: "snappy"
    row_group_size: "128KB"
    
  memory:
    driver: "4g"
    executor: "8g"
    executor_memory_fraction: 0.8
    
  query_optimization:
    predicate_pushdown: true
    column_pruning: true
    partition_pruning: true
    
  compaction:
    delta:
      auto_optimize: true
      auto_compact: true
      min_file_size: "10MB"
      
  caching:
    enabled: true
    cache_type: "memory"
    max_memory_percent: 40
    
  monitoring:
    enabled: true
    metrics:
      - name: "query_duration"
        type: "gauge"
      - name: "memory_usage"
        type: "gauge"
```

## Ví dụ kiểm tra hiệu suất:

```python
import time

def measure_query_performance(query, spark):
    """Measure query execution time."""
    
    start_time = time.time()
    
    # Thực hiện truy vấn
    result = spark.sql(query)
    
    end_time = time.time()
    
    execution_time = end_time - start_time
    
    print(f"Query executed in {execution_time} seconds")
    
    return result

# Kiểm tra hiệu suất truy vấn
query = """
SELECT user_id, count(*) as event_count 
FROM lakehouse.user_events 
WHERE date >= '2023-01-01' 
  AND region = 'us'
GROUP BY user_id
ORDER BY event_count DESC
LIMIT 1000
"""

result = measure_query_performance(query, spark)
```

## Kết luận

Tối ưu hiệu suất trong lakehouse là một quá trình liên tục bao gồm:

1. **Partitioning**: Tối ưu phân vùng dữ liệu
2. **File format**: Chọn đúng định dạng file
3. **Memory management**: Cấu hình bộ nhớ phù hợp
4. **Query optimization**: Tối ưu truy vấn hiệu quả
5. **Compaction**: Quản lý file định kỳ
6. **Caching**: Cache dữ liệu thường dùng
7. **Monitoring**: Theo dõi hiệu suất

Việc áp dụng các kỹ thuật tối ưu này giúp hệ thống lakehouse của bạn hoạt động hiệu quả và tiết kiệm chi phí.