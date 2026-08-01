# Generate Data Quality Check Skill

## Mô tả
Kỹ năng tạo kiểm tra chất lượng dữ liệu tự động.

## Mục tiêu
- Tự động tạo các kiểm tra chất lượng dữ liệu (DQ rules).
- Hỗ trợ các loại kiểm tra: schema, completeness, uniqueness.

## Cách sử dụng
Khi yêu cầu tạo kiểm tra chất lượng:
1. Xác định dataset cần kiểm tra
2. Xác định loại kiểm tra (schema, completeness, etc.)
3. Gọi kỹ năng để tạo kiểm tra

## Các mẫu được hỗ trợ
- DQ rules YAML file
- SQL test queries
- Alert configuration

## Ví dụ sử dụng
```
Tạo kiểm tra chất lượng cho user_events:
1. Kiểm tra schema (type, nullability)
2. Kiểm tra completeness (không thiếu bản ghi quan trọng)
3. Tạo alert khi vượt ngưỡng lỗi
```

## Cấu trúc kiểm tra được tạo
- `dq_rules/` - file quy tắc kiểm tra chất lượng
- `dq_tests/` - các test SQL cho kiểm tra
- `alerts/` - cấu hình cảnh báo

## Các yêu cầu kỹ thuật
- Sử dụng SQL để tạo test queries
- Có cấu hình alert khi vượt ngưỡng
- Có logging cho kết quả kiểm tra
- Kiểm tra định kỳ theo SLA