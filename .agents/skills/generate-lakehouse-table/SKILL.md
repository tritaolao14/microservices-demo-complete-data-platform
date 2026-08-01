# Generate Lakehouse Table Skill

## Mô tả
Kỹ năng tạo table trong lakehouse tự động.

## Mục tiêu
- Tự động tạo table Delta/Iceberg/Hudi trong lakehouse.
- Hỗ trợ các pattern: partitioning, retention, compaction.

## Cách sử dụng
Khi yêu cầu tạo table lakehouse:
1. Xác định table format (Delta/Iceberg/Hudi)
2. Xác định cấu trúc dữ liệu
3. Gọi kỹ năng để tạo table

## Các mẫu được hỗ trợ
- Delta table Python
- Iceberg SQL table
- Hudi YAML configuration

## Ví dụ sử dụng
```
Tạo một Delta table cho user_events:
1. Tạo table với partitioning theo ngày
2. Cấu hình retention 30 ngày
3. Tạo compaction policy
```

## Cấu trúc table được tạo
- `tables/` - chứa định nghĩa table
- `scripts/` - script tạo table
- `config/` - cấu hình table

## Các yêu cầu kỹ thuật
- Sử dụng Delta Lake hoặc Iceberg table format
- Có partitioning theo ngày hoặc event_type
- Có retention policy và compaction
- Có metadata store (Hive, Glue)