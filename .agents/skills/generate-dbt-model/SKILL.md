# Generate dbt Model Skill

## Mô tả
Kỹ năng tạo model dbt tự động dựa trên yêu cầu và cấu trúc dữ liệu.

## Mục tiêu
- Tự động tạo model SQL dbt với cấu trúc chuẩn.
- Hỗ trợ các pattern: staging → intermediate → final.
- Tạo schema.yml và test.sql cho model.

## Cách sử dụng
Khi yêu cầu tạo dbt model:
1. Xác định loại model (staging, intermediate, final)
2. Xác định nguồn dữ liệu và cấu trúc
3. Gọi kỹ năng để tạo model dbt

## Các mẫu được hỗ trợ
- Model SQL (staging/intermediate/final)
- Schema YAML file
- Test SQL

## Ví dụ sử dụng
```
Tạo một model dbt staging cho user_events từ table raw_user_events:
1. Tạo model staging trong models/staging/
2. Tạo schema.yml cho model
3. Tạo test.sql cho validation
```

## Cấu trúc model được tạo
- `models/staging/` - model staging cho dữ liệu thô
- `models/intermediate/` - model trung gian
- `models/final/` - model cuối cùng cho BI/ML
- `tests/` - test cases cho model

## Các yêu cầu kỹ thuật
- Sử dụng naming convention snake_case
- Có comment cho logic phức tạp
- Tách các CTE nếu quá dài (dưới 300 dòng)
- Có schema.yml định nghĩa field
- Có test.sql cho validation