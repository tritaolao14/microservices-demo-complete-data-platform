# Scaffold Spark Job Workflow

## Mô tả
Quy trình tạo job Spark tự động dựa trên yêu cầu xử lý dữ liệu.

## Mục tiêu
- Tự động tạo Spark job với cấu trúc chuẩn.
- Hỗ trợ các pattern: Delta Lake, Iceberg table, streaming.

## Các bước thực hiện

### 1. Nhận yêu cầu
- Xác định loại xử lý (batch/streaming)
- Xác định table format (Delta/Iceberg/Hudi)
- Xác định yêu cầu kỹ thuật (partitioning, retention, etc.)

### 2. Tạo cấu trúc thư mục Spark job
- `jobs/` - chứa các Spark job files
- `config/` - file cấu hình YAML
- `tables/` - định nghĩa table (Delta/Iceberg/Hudi)
- `scripts/` - script xử lý chính

### 3. Tạo file mẫu Spark job
- Spark job Python (PySpark)
- Configuration file YAML
- Delta table creation
- Iceberg table setup

### 4. Cấu hình Spark job
- Tạo file config cho job
- Cấu hình logging và observability
- Thiết lập partitioning và compaction

### 5. Kiểm tra và hoàn thiện
- Validate cấu trúc Spark job
- Kiểm tra job có thể chạy độc lập
- Tạo tài liệu hướng dẫn sử dụng

## Ví dụ sử dụng

```
Tạo một Spark job xử lý user_events:
1. Sử dụng Delta table format
2. Xử lý batch theo ngày
3. Có cấu hình cho partitioning và compaction
4. Có logging cấu trúc (JSON)
```

## Mẫu cấu trúc Spark job

```
user_events_spark_job/
├── jobs/
│   └── user_events_processing.py
├── config/
│   └── spark_config.yaml
├── tables/
│   └── delta_table_creation.py
└── scripts/
    └── run_job.sh
```

## Các yêu cầu kỹ thuật

### 1. PySpark API:
- Sử dụng PySpark API cho xử lý dữ liệu
- Có cấu hình cho Spark session

### 2. Delta Lake:
- Sử dụng Delta table format
- Có cấu hình cho partitioning

### 3. Iceberg table:
- Hỗ trợ Iceberg table format
- Có cấu hình cho metadata

### 4. Partitioning:
- Có cấu hình partitioning theo ngày
- Tối ưu hiệu suất truy vấn

### 5. Retention:
- Có chính sách retention cho dữ liệu
- Có cấu hình archival

### 6. Compaction:
- Có chính sách compaction định kỳ
- Tối ưu hiệu suất đọc

## Ví dụ code Spark job

### Spark job chính:
```python
"""Spark job for user_events processing."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging


def create_spark_session():
    """Create Spark session with proper configuration."""
    spark = SparkSession.builder \
        .appName("user_events_processing") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    return spark


def process_user_events_data(spark, input_path, output_path):
    """Process user_events data."""
    
    # Read input data
    df = spark.read.format("delta").load(input_path)
    
    # Process data (example transformation)
    processed_df = df.withColumn("processed_at", current_timestamp())
    
    # Write to output (Delta table)
    processed_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(output_path)
    
    logging.info("Processed user_events data successfully")


def main():
    """Main function to run the Spark job."""
    spark = create_spark_session()
    
    # Configuration
    input_path = "s3a://your-bucket/raw/user_events"
    output_path = "s3a://your-bucket/processed/user_events"
    
    try:
        process_user_events_data(spark, input_path, output_path)
        logging.info("Spark job completed successfully")
    except Exception as e:
        logging.error(f"Error in Spark job: {str(e)}")
        raise
    
    spark.stop()


if __name__ == "__main__":
    main()
```

### Configuration file:
```yaml
# config/spark_config.yaml
spark:
  app_name: "user_events_processing"
  master: "local[*]"
  config:
    spark.sql.adaptive.enabled: "true"
    spark.sql.adaptive.coalescePartitions.enabled: "true"
    spark.sql.execution.arrow.pyspark.enabled: "true"
    spark.sql.shuffle.partitions: "200"
    
storage:
  input_path: "s3a://your-bucket/raw/user_events"
  output_path: "s3a://your-bucket/processed/user_events"
  checkpoint_path: "s3a://your-bucket/checkpoint/user_events"
  
partitioning:
  enabled: true
  column: "date"
  format: "yyyy-MM-dd"
  
retention:
  enabled: true
  days: 30
  
compaction:
  enabled: true
  interval_days: 7

logging:
  level: "INFO"
  format: "json"
```

### Delta table creation:
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

## Tài liệu hướng dẫn

### 1. Cấu trúc thư mục:
- `jobs/` - chứa các Spark job files
- `config/` - file cấu hình YAML
- `tables/` - định nghĩa table (Delta/Iceberg/Hudi)
- `scripts/` - script xử lý chính

### 2. Cách chạy Spark job:
```bash
# Cài đặt dependencies
pip install pyspark

# Chạy Spark job
spark-submit \
  --master local[*] \
  --class com.example.UserEventsProcessor \
  user_events_processing.py

# Hoặc chạy trực tiếp với Python
python jobs/user_events_processing.py
```

### 3. Cấu hình Spark:
```yaml
# config/spark_config.yaml
spark:
  app_name: "user_events_processing"
  master: "local[*]"
  config:
    spark.sql.adaptive.enabled: "true"
    spark.sql.adaptive.coalescePartitions.enabled: "true"
    spark.sql.execution.arrow.pyspark.enabled: "true"
    spark.sql.shuffle.partitions: "200"
    
storage:
  input_path: "s3a://your-bucket/raw/user_events"
  output_path: "s3a://your-bucket/processed/user_events"
  
partitioning:
  enabled: true
  column: "date"
  format: "yyyy-MM-dd"
  
retention:
  enabled: true
  days: 30
  
compaction:
  enabled: true
  interval_days: 7
```

## Kiểm tra chất lượng

### 1. Unit test:
- Test từng component độc lập
- Test edge cases và error conditions

### 2. Integration test:
- Test job chạy trên dữ liệu mẫu
- Test end-to-end flow

### 3. Performance test:
- Kiểm tra thời gian chạy job
- Kiểm tra hiệu suất xử lý

## Cảnh báo và xử lý lỗi

### 1. Retry logic:
- Có cấu hình retry cho các job fail
- Log chi tiết về lỗi

### 2. Monitoring:
- Theo dõi thời gian chạy job
- Theo dõi hiệu suất xử lý

### 3. Alerting:
- Cảnh báo khi job fail liên tiếp
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

Quy trình scaffold Spark job giúp tạo cấu trúc chuẩn cho các job Spark, đảm bảo:
1. Tính nhất quán trong cấu trúc code
2. Khả năng tái sử dụng các component
3. Dễ dàng kiểm thử và bảo trì
4. Tuân thủ các best practices trong Spark processing