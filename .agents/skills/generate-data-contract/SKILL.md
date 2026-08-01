# Generate Data Contract Skill

## Mô tả
Kỹ năng tạo data contract tự động dựa trên yêu cầu và cấu trúc dữ liệu.

## Mục tiêu
- Tự động tạo data contract với định nghĩa field, type, nullability.
- Hỗ trợ các format: YAML, JSON, Avro.

## Cách sử dụng
Khi yêu cầu tạo data contract:
1. Xác định dataset cần tạo contract
2. Xác định cấu trúc dữ liệu
3. Gọi kỹ năng để tạo contract

## Các mẫu được hỗ trợ
- Data contract YAML
- Schema JSON
- Expectations YAML

## Ví dụ sử dụng
```
Tạo một data contract cho user_events:
1. Định nghĩa các field: id, name, email, created_at
2. Kiểm tra nullability và type
3. Tạo expectations cho validation
```

## Cấu trúc contract được tạo
- `contracts/` - chứa các file contract
- `schemas/` - định nghĩa schema (JSON/Avro)
- `expectations/` - bộ quy tắc validation

## Các yêu cầu kỹ thuật
- Sử dụng Pydantic hoặc JSON Schema để định nghĩa contract
- Có versioning cho contract
- Có backward compatibility khi thay đổi schema
- Có expectations cho validation (data quality rules)