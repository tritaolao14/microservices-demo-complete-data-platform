# Generate Spark Job Skill

## Mô tả
Kỹ năng tạo job Spark tự động dựa trên yêu cầu xử lý dữ liệu.

## Mục tiêu
- Tự động tạo Spark job với cấu trúc chuẩn.
- Hỗ trợ các pattern: Delta Lake, Iceberg table, streaming.

## Cách sử dụng
Khi yêu cầu tạo Spark job:
1. Xác định loại xử lý (batch/streaming)
2. Xác định table format (Delta/Iceberg/Hudi)
3. Gọi kỹ năng để tạo job

## Các mẫu được hỗ trợ
- Spark job Python (PySpark)
- Configuration file YAML
- Delta table creation
- Iceberg table setup

## Ví dụ sử dụng
```
Tạo một Spark job xử lý user_events:
1. Sử dụng Delta table format
2. Xử lý batch theo ngày
3. Có cấu hình cho partitioning và compaction
```

## Cấu trúc job được tạo
- `jobs/` - chứa các Spark job files
- `config/` - file cấu hình YAML
- `tables/` - định nghĩa table (Delta/Iceberg/Hudi)
- `scripts/` - script xử lý chính

## Các yêu cầu kỹ thuật
- Sử dụng PySpark API
- Có cấu hình cho partitioning (theo ngày)
- Có chính sách retention và compaction
- Sử dụng Delta Lake hoặc Iceberg table format
- Có logging cấu trúc (JSON)