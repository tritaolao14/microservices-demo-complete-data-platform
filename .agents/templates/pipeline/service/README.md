# Service Pipeline Template

## Mô tả
Mẫu pipeline ETL/ELT cho service data trong hệ thống.

## Cấu trúc thư mục
```
service_pipeline/
├── extract/
│   ├── __init__.py
│   └── service_extractor.py
├── transform/
│   ├── __init__.py
│   └── service_transformer.py
├── load/
│   ├── __init__.py
│   └── service_loader.py
├── contracts/
│   ├── __init__.py
│   └── service_contract.py
├── tests/
│   ├── __init__.py
│   └── test_service_pipeline.py
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
Tạo một pipeline service cho order_events từ API đến Delta table:
1. Extract từ API endpoint orders
2. Transform: thêm metadata, validate schema
3. Load vào Delta table order_events
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
  name: "service_pipeline"
  source:
    type: "api"
    endpoint: "/orders"
  destination:
    type: "delta"
    table: "order_events"
  logging:
    level: "INFO"
    format: "json"
```

### 3. Kiểm tra chất lượng:
- Test từng component độc lập
- Test end-to-end flow
- Validate schema cho dữ liệu đầu vào
```