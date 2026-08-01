# Scaffold Lakehouse Table Workflow

## Mô tả
Quy trình tạo table trong lakehouse tự động.

## Mục tiêu
- Tự động tạo table Delta/Iceberg/Hudi trong lakehouse.
- Hỗ trợ các pattern: partitioning, retention, compaction.

## Các bước thực hiện

### 1. Nhận yêu cầu
- Xác định table format (Delta/Iceberg/Hudi)
- Xác định cấu trúc dữ liệu
- Xác định yêu cầu kỹ thuật (partitioning, retention, etc.)

### 2. Tạo cấu trúc thư mục table
- `tables/` - chứa định nghĩa table
- `scripts/` - script tạo table
- `config/` - cấu hình table

### 3. Tạo file mẫu table
- Delta table Python script
- Iceberg SQL table
- Hudi YAML configuration

### 4. Cấu hình table
- Tạo file config cho table
- Cấu hình partitioning và retention
- Thiết lập compaction policy

### 5. Kiểm tra và hoàn thiện
- Validate cấu trúc table
- Kiểm tra table có thể tạo và sử dụng
- Tạo tài liệu hướng dẫn sử dụng

## Ví dụ sử dụng

```
Tạo một Delta table cho user_events:
1. Tạo table với partitioning theo ngày
2. Cấu hình retention 30 ngày
3. Tạo compaction policy
4. Có logging cấu trúc (JSON)
```

## Mẫu cấu trúc table

```
user_events_lakehouse_table/
├── tables/
│   ├── delta_table_creation.py
│   ├── iceberg_table.sql
│   └── hudi_table.yml
├── config/
│   └── table_config.yaml
└── scripts/
    └── run_table_creation.sh
```

## Các yêu cầu kỹ thuật

### 1. Delta Lake:
- Sử dụng Delta table format
- Có cấu hình cho partitioning

### 2. Iceberg table:
- Hỗ trợ Iceberg table format
- Có cấu hình cho metadata

### 3. Hudi table:
- Hỗ trợ Hudi table format
- Có cấu hình cho upserts

### 4. Partitioning:
- Có cấu hình partitioning theo ngày
- Tối ưu hiệu suất truy vấn

### 5. Retention:
- Có chính sách retention cho dữ liệu
- Có cấu hình archival

### 6. Compaction:
- Có chính sách compaction định kỳ
- Tối ưu hiệu suất đọc

## Ví dụ code table creation

### Delta table:
```python
"""Delta table creation script for user_events."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging


def create_delta_table(spark, table_name, location, schema):
    """Create Delta table with proper configuration."""
    
    # Create the table with Delta format
    df = spark.createDataFrame([], schema)
    
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("path", location) \
        .saveAsTable(table_name)
    
    # Configure table properties
    spark.sql(f"""
        ALTER TABLE {table_name} 
        SET TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true',
            'delta.enableChangeDataFeed' = 'true'
        )
    """)
    
    logging.info(f"Created Delta table {table_name} at {location}")


def setup_partitioning(spark, table_name, partition_column):
    """Setup partitioning for the table."""
    
    # Add partitioning configuration
    spark.sql(f"""
        ALTER TABLE {table_name} 
        SET TBLPROPERTIES (
            'delta.partitionColumns' = '{partition_column}'
        )
    """)
    
    logging.info(f"Configured partitioning on {table_name} by {partition_column}")


def setup_retention(spark, table_name, retention_days):
    """Setup data retention policy."""
    
    # Configure retention (this is a simplified example)
    spark.sql(f"""
        ALTER TABLE {table_name} 
        SET TBLPROPERTIES (
            'delta.retentionPeriod' = '{retention_days} days'
        )
    """)
    
    logging.info(f"Configured {retention_days}-day retention for {table_name}")


def main():
    """Main function to create Delta table."""
    spark = SparkSession.builder \
        .appName("Create user_events Delta Table") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        # Table configuration
        table_name = "lakehouse.user_events"
        location = f"s3a://your-bucket/lakehouse/user_events"
        partition_column = "date"
        retention_days = 30
        
        # Define schema (customize based on your data)
        schema = [
            ("id", "string"),
            ("name", "string"),
            ("created_at", "timestamp"),
            ("date", "date")
        ]
        
        # Create table
        create_delta_table(spark, table_name, location, schema)
        
        # Setup partitioning
        setup_partitioning(spark, table_name, partition_column)
        
        # Setup retention
        setup_retention(spark, table_name, retention_days)
        
        logging.info("Delta table creation completed successfully")
        
    except Exception as e:
        logging.error(f"Error creating Delta table: {str(e)}")
        raise
    
    spark.stop()


if __name__ == "__main__":
    main()
```

### Iceberg table:
```sql
-- Iceberg table creation for user_events

-- Create Iceberg table with proper configuration
CREATE TABLE IF NOT EXISTS lakehouse.user_events (
    id STRING,
    name STRING,
    created_at TIMESTAMP,
    date DATE
)
USING iceberg
PARTITIONED BY (date)
LOCATION 's3a://your-bucket/lakehouse/user_events'
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.partition.overwrite' = 'true',
    'write.compaction.strategy' = 'major',
    'write.compaction.enabled' = 'true'
);

-- Configure retention policy (example)
ALTER TABLE lakehouse.user_events
SET TBLPROPERTIES (
    'retention.days' = 30
);

-- Configure partitioning
ALTER TABLE lakehouse.user_events
SET TBLPROPERTIES (
    'partitioning' = 'date'
);

-- Example query to verify table creation
SELECT * FROM lakehouse.user_events LIMIT 10;
```

### Hudi table:
```yaml
# Hudi table configuration for user_events

version: "1.0"
name: "user_events Hudi Table Configuration"
description: "Configuration for Hudi table in lakehouse"

table:
  name: "lakehouse.user_events"
  location: "s3a://your-bucket/lakehouse/user_events"
  format: "hudi"
  
partitioning:
  enabled: true
  column: "date"
  type: "daily"
  
storage:
  file_format: "parquet"
  compression: "snappy"
  
compaction:
  enabled: true
  strategy: "major"
  interval_days: 7
  
retention:
  enabled: true
  days: 30
  
write:
  operation: "upsert"
  payload_class: "org.apache.hudi.payload.AvroPayload"
  
read:
  consistency_check: true
  snapshot_query: true
  
properties:
  # Hudi specific properties
  hoodie.datasource.write.recordkey.field: "id"
  hoodie.datasource.write.partitionpath.field: "date"
  hoodie.datasource.write.table.name: "user_events"
  hoodie.datasource.write.keygenerator.class: "org.apache.hudi.keygen.SimpleKeyGenerator"
```

## Tài liệu hướng dẫn

### 1. Cấu trúc thư mục:
- `tables/` - chứa định nghĩa table
- `scripts/` - script tạo table
- `config/` - cấu hình table

### 2. Cách chạy table creation:
```bash
# Cài đặt dependencies
pip install pyspark

# Chạy script tạo Delta table
python tables/delta_table_creation.py

# Hoặc chạy SQL trực tiếp với Spark SQL
spark-sql -f tables/iceberg_table.sql

# Hoặc chạy Hudi table config
hudi-table-config --config tables/hudi_table.yml
```

### 3. Cấu hình table:
```yaml
# config/table_config.yaml
table:
  name: "user_events"
  location: "s3a://your-bucket/lakehouse/user_events"
  format: "delta"
  
partitioning:
  enabled: true
  column: "date"
  format: "yyyy-MM-dd"
  
retention:
  enabled: true
  days: 30
  
compaction:
  enabled: true
  strategy: "major"
  interval_days: 7
  
logging:
  level: "INFO"
  format: "json"
```

## Kiểm tra chất lượng

### 1. Unit test:
- Test từng component độc lập
- Test edge cases và error conditions

### 2. Integration test:
- Test table tạo và sử dụng đúng cách
- Test end-to-end flow

### 3. Performance test:
- Kiểm tra thời gian tạo table
- Kiểm tra hiệu suất truy vấn

## Cảnh báo và xử lý lỗi

### 1. Retry logic:
- Có cấu hình retry cho các table fail
- Log chi tiết về lỗi

### 2. Monitoring:
- Theo dõi thời gian tạo table
- Theo dõi hiệu suất truy vấn

### 3. Alerting:
- Cảnh báo khi table creation fail liên tiếp
- Có routing alert theo domain/team

## Tối ưu hóa hiệu suất

### 1. Partitioning:
- Sử dụng partitioning theo ngày/tháng
- Tối ưu hiệu suất truy vấn

### 2. Memory management:
- Cấu hình bộ nhớ Spark phù hợp
- Sử dụng cache cho dữ liệu thường dùng

### 3. Compaction:
- Định kỳ compaction table
- Tối ưu hiệu suất đọc

## Kết luận

Quy trình scaffold lakehouse table giúp tạo cấu trúc chuẩn cho các table trong lakehouse, đảm bảo:
1. Tính nhất quán trong cấu trúc table
2. Khả năng tái sử dụng các component
3. Dễ dàng kiểm thử và bảo trì
4. Tuân thủ các best practices trong lakehouse architecture