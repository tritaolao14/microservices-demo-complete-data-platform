# Feature Pipeline Template

## Mô tả
Mẫu pipeline ETL/ELT cho feature data trong hệ thống.

## Cấu trúc thư mục
```
feature_pipeline/
├── extract/
│   ├── __init__.py
│   └── feature_extractor.py
├── transform/
│   ├── __init__.py
│   └── feature_transformer.py
├── load/
│   ├── __init__.py
│   └── feature_loader.py
├── contracts/
│   ├── __init__.py
│   └── feature_contract.py
├── tests/
│   ├── __init__.py
│   └── test_feature_pipeline.py
├── config/
│   └── pipeline_config.yaml
└── main.py
```

## Đặc điểm

### 1. Generator-based pipeline design:
- Extract → Transform → Load đều dùng generator
- Chain các stage bằng iterator

### 2. Functional pipeline stages:
- Mỗi stage là pure function
- Dễ test, dễ reuse

### 3. Protocol-based components:
- Định nghĩa protocol/interface: Source, Sink, Transformer
- Nhiều implementation (file, DB, API)

### 4. Configuration management:
- Dùng dataclass / Pydantic cho config
- Config theo cấp: global → pipeline → stage

### 5. Error handling & recovery:
- Exception hierarchy rõ ràng
- Retry với backoff
- Dead-letter queue

### 6. State management:
- PipelineState: status, counters, timestamps
- Persist state để resume

### 7. Type safety & validation:
- Type hint everywhere
- Pydantic / dataclass validation

### 8. Performance optimization:
- Generator, batching, parallel (concurrent.futures, asyncio)

### 9. Observability & monitoring:
- Structured logging
- Metrics: count, duration, error rate

### 10. Testing pipeline components:
- Unit test pure functions
- Integration test với fake source/sink

## Ví dụ sử dụng

```
Tạo một pipeline feature cho user_events từ Kafka đến PostgreSQL:
1. Extract từ Kafka topic user_events
2. Transform: thêm timestamp, validate schema
3. Load vào table user_events trong PostgreSQL
4. Tạo test cases cho từng component
5. Cấu hình logging và observability
```

## Tài liệu hướng dẫn

### 1. Cách chạy pipeline:
```bash
# Cài đặt dependencies
pip install -r requirements.txt

# Chạy pipeline
python main.py

# Chạy test cases
python -m pytest tests/
```

### 2. Cấu hình pipeline:
```yaml
# config/pipeline_config.yaml
pipeline:
  name: "feature_pipeline"
  source:
    type: "kafka"
    topic: "user_events"
  destination:
    type: "postgresql"
    table: "user_events"
  logging:
    level: "INFO"
    format: "json"
```

### 3. Kiểm tra chất lượng:
- Test từng component độc lập
- Test end-to-end flow
- Validate schema cho dữ liệu đầu vào
```