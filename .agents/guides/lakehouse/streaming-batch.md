# Streaming vs Batch Processing

## Mục tiêu
Hướng dẫn lựa chọn giữa streaming và batch processing trong lakehouse.

## Tổng quan

### Batch Processing
- Xử lý dữ liệu theo khoảng thời gian cố định (giờ, ngày)
- Thường được sử dụng cho ETL/ELT
- Dữ liệu được xử lý theo chu kỳ

### Streaming Processing
- Xử lý dữ liệu theo thời gian thực (real-time)
- Dữ liệu được xử lý khi có sẵn
- Thường được sử dụng cho các use case cần phản hồi nhanh

## Đặc điểm của từng loại

### Batch Processing

#### Ưu điểm:
- Hiệu quả cao cho xử lý lượng lớn dữ liệu
- Dễ kiểm soát và quản lý trạng thái
- Tốt cho các pipeline ETL/ELT
- Có thể xử lý dữ liệu không có sẵn

#### Nhược điểm:
- Không phản hồi nhanh
- Cần thiết lập lịch chạy
- Có thể bị trễ dữ liệu

#### Ví dụ:
```python
# Batch pipeline xử lý theo ngày
spark.read.parquet("s3a://bucket/data/2023-01-01") \
    .filter("timestamp >= '2023-01-01'") \
    .groupBy("user_id") \
    .agg(count("*").alias("event_count")) \
    .write.format("delta").mode("overwrite").save("s3a://bucket/output")
```

### Streaming Processing

#### Ưu điểm:
- Phản hồi nhanh (real-time)
- Xử lý dữ liệu khi có sẵn
- Tốt cho các use case cần cập nhật tức thì

#### Nhược điểm:
- Khó kiểm soát trạng thái
- Yêu cầu hệ thống ổn định
- Có thể gặp vấn đề về consistency

#### Ví dụ:
```python
# Streaming pipeline xử lý dữ liệu theo thời gian thực
streaming_df = spark \
    .readStream \
    .format("kafka") \
    .option("subscribe", "user_events") \
    .load()

processed_streaming_df = streaming_df \
    .withColumn("processed_at", current_timestamp())

processed_streaming_df \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start("s3a://bucket/streaming_output")
```

## Khi nào nên dùng batch?

### Dùng batch khi:
1. **Xử lý dữ liệu lớn**: Batch xử lý hiệu quả hơn streaming cho lượng lớn dữ liệu
2. **Cần kiểm soát thời gian**: Bạn cần chạy pipeline theo lịch cố định (giờ, ngày)
3. **Xử lý dữ liệu không có sẵn**: Dữ liệu chỉ có khi được thu thập hoàn chỉnh
4. **Cần tính toán phức tạp**: Batch xử lý tốt cho các phép tính phức tạp

### Ví dụ use case:
- Báo cáo hàng ngày
- Tính toán tổng hợp theo tháng
- Pipeline ETL/ELT

## Khi nào nên dùng streaming?

### Dùng streaming khi:
1. **Cần phản hồi nhanh**: Bạn cần cập nhật dữ liệu ngay khi có sự kiện mới
2. **Dữ liệu liên tục**: Dữ liệu được tạo liên tục và cần xử lý ngay lập tức
3. **Cần cảnh báo thực thời**: Bạn muốn nhận cảnh báo khi có sự kiện đặc biệt
4. **Xử lý dữ liệu theo thời gian thực**: Bạn cần xử lý dữ liệu theo thời gian thực

### Ví dụ use case:
- Cảnh báo gian lận
- Dashboard theo thời gian thực
- Hệ thống cảnh báo sự kiện

## Hybrid Approach

### Kết hợp batch và streaming:
1. **Batch**: Xử lý dữ liệu lớn theo chu kỳ
2. **Streaming**: Xử lý dữ liệu mới theo thời gian thực

### Ví dụ hybrid:
```python
# Batch pipeline cho dữ liệu lịch sử
batch_df = spark.read.parquet("s3a://bucket/history") \
    .filter("date < '2023-01-01'")

# Streaming pipeline cho dữ liệu mới
streaming_df = spark \
    .readStream \
    .format("kafka") \
    .option("subscribe", "user_events") \
    .load()

# Kết hợp cả hai
combined_df = batch_df.union(streaming_df)

# Ghi kết quả vào lakehouse
combined_df.write.format("delta").mode("overwrite").save(
    "s3a://bucket/combined_data"
)
```

## Công cụ hỗ trợ

### Batch Processing Tools:
1. **Spark**: Xử lý batch hiệu quả
2. **Airflow**: Orchestrator cho batch pipeline
3. **dbt**: Transformation cho batch data

### Streaming Processing Tools:
1. **Spark Structured Streaming**: Streaming với Spark
2. **Flink**: Framework streaming mạnh mẽ
3. **Kafka Streams**: Streaming với Kafka

## Ví dụ cấu hình hybrid pipeline

### Batch Pipeline:
```yaml
batch_pipeline:
  name: "user_events_batch"
  schedule: "daily"
  source: "s3a://bucket/raw/user_events"
  destination: "s3a://bucket/silver/user_events"
  processing:
    - transform_schema
    - validate_data
```

### Streaming Pipeline:
```yaml
streaming_pipeline:
  name: "user_events_streaming"
  source: "kafka://user_events_topic"
  destination: "s3a://bucket/streaming/user_events"
  processing:
    - real_time_validation
    - enrich_data
  trigger: "1 minute"
```

## Tối ưu hóa hiệu suất

### Batch Optimization:
1. **Partitioning**: Phân vùng dữ liệu theo ngày/tháng
2. **Compaction**: Gộp file nhỏ thành file lớn
3. **Caching**: Cache dữ liệu thường dùng

### Streaming Optimization:
1. **Windowing**: Sử dụng window để xử lý dữ liệu theo khoảng thời gian
2. **Checkpointing**: Lưu trạng thái để phục hồi khi có lỗi
3. **Backpressure**: Kiểm soát tốc độ xử lý

## Ví dụ cấu hình chi tiết

### Batch Configuration:
```yaml
pipeline:
  name: "user_events_batch"
  type: "batch"
  schedule:
    interval: "daily"
    time: "02:00"
  source:
    type: "s3"
    path: "s3a://bucket/raw/user_events"
  processing:
    - name: "schema_validation"
      type: "validation"
    - name: "data_transformation"
      type: "transform"
  destination:
    type: "delta"
    path: "s3a://bucket/silver/user_events"
```

### Streaming Configuration:
```yaml
pipeline:
  name: "user_events_streaming"
  type: "streaming"
  source:
    type: "kafka"
    topic: "user_events"
  processing:
    - name: "real_time_validation"
      type: "validation"
    - name: "enrichment"
      type: "transform"
  destination:
    type: "delta"
    path: "s3a://bucket/streaming/user_events"
  trigger:
    interval: "1 minute"
  checkpoint:
    path: "s3a://bucket/checkpoint/user_events"
```

## Quản lý trạng thái

### Batch:
- Trạng thái được lưu theo lịch trình
- Không cần quản lý trạng thái liên tục

### Streaming:
- Cần quản lý trạng thái (checkpoint)
- Cần xử lý lỗi và phục hồi

## Ví dụ kiểm tra hiệu suất

### Batch Performance:
```python
# Kiểm tra thời gian xử lý batch
start_time = time.time()

spark.read.parquet("s3a://bucket/data") \
    .groupBy("user_id") \
    .agg(count("*").alias("event_count")) \
    .write.format("delta").mode("overwrite").save("s3a://bucket/output")

end_time = time.time()
print(f"Batch processing took {end_time - start_time} seconds")
```

### Streaming Performance:
```python
# Kiểm tra hiệu suất streaming
start_time = time.time()

streaming_df = spark \
    .readStream \
    .format("kafka") \
    .option("subscribe", "user_events") \
    .load()

processed_df = streaming_df \
    .withColumn("processed_at", current_timestamp())

processed_df \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start("s3a://bucket/streaming_output")

end_time = time.time()
print(f"Streaming processing took {end_time - start_time} seconds")
```

## Kết luận

Việc lựa chọn giữa batch và streaming phụ thuộc vào yêu cầu cụ thể của use case:

1. **Batch**: Tốt cho xử lý lượng lớn dữ liệu, kiểm soát thời gian
2. **Streaming**: Tốt cho phản hồi nhanh, xử lý dữ liệu theo thời gian thực
3. **Hybrid**: Kết hợp cả hai để tối ưu hiệu suất và đáp ứng nhu cầu khác nhau