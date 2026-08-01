# Generate Pipeline Skill

## Mô tả
Kỹ năng tạo pipeline ETL/ELT tự động dựa trên yêu cầu và mẫu.

## Mục tiêu
- Tự động tạo pipeline Python với cấu trúc chuẩn.
- Hỗ trợ các pattern: generator, functional stage, protocol-based.
- Tạo cấu trúc thư mục phù hợp với quy chuẩn.

## Cách sử dụng
Khi yêu cầu tạo pipeline mới:
1. Xác định loại pipeline (batch, streaming, hybrid)
2. Xác định domain/data source/destination
3. Gọi kỹ năng để tạo cấu trúc pipeline

## Các mẫu được hỗ trợ
- Extract → Transform → Load (ETL)
- Data contract
- Test cases

## Ví dụ sử dụng
```
Tạo một pipeline ETL cho user_events từ Kafka đến PostgreSQL với các bước:
1. Extract từ Kafka topic user_events
2. Transform: thêm timestamp, validate schema
3. Load vào table user_events trong PostgreSQL
```

## Cấu trúc pipeline được tạo
- `extract/` - module extract từ nguồn
- `transform/` - module transform dữ liệu
- `load/` - module load vào đích
- `contracts/` - định nghĩa data contract
- `tests/` - test cases cho từng component

## Các yêu cầu kỹ thuật
- Sử dụng generator-based pipeline design
- Functional stages (pure functions)
- Protocol-based components (Source, Sink, Transformer)
- Configuration-driven behavior
- Error handling & retry logic
- Type safety (Pydantic/dataclass)
- Observability (structured logging, metrics)