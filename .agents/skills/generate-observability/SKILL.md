# Generate Observability Skill

## Mô tả
Kỹ năng tạo cấu hình observability tự động cho pipeline.

## Mục tiêu
- Tự động tạo các metric, logging, alert cho pipeline.
- Hỗ trợ cấu hình observability theo best practices.

## Cách sử dụng
Khi yêu cầu tạo observability:
1. Xác định pipeline cần theo dõi
2. Xác định loại metric cần theo dõi
3. Gọi kỹ năng để tạo cấu hình

## Các mẫu được hỗ trợ
- Metrics YAML file
- Alert configuration
- Logging structure

## Ví dụ sử dụng
```
Tạo cấu hình observability cho user_events pipeline:
1. Tạo metric cho số bản ghi qua từng stage
2. Tạo alert khi tỷ lệ lỗi vượt ngưỡng
3. Cấu hình logging cấu trúc
```

## Cấu trúc observability được tạo
- `metrics/` - file cấu hình metric
- `alerts/` - cấu hình cảnh báo
- `logging/` - cấu hình logging

## Các yêu cầu kỹ thuật
- Sử dụng structured logging (JSON format)
- Có các metric chính: record count, duration, error rate
- Có alert khi pipeline fail liên tiếp 3 lần
- Có freshness metric cho dataset