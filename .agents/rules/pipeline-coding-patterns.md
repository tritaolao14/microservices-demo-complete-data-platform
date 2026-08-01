# Pipeline Coding Patterns (18 Best Practices)

Tài liệu này tổng hợp 18 pattern từ “18 skill pipeline” gốc, dùng làm reference khi viết code pipeline Python.

## 1. Generator-Based Pipeline Design

- Mục đích: xử lý dữ liệu lớn mà không load hết vào memory.
- Pattern:
  - Extract → Transform → Load đều dùng generator.
  - Chain các stage bằng iterator.

```python
def extract(source):
    for row in source:
        yield row

def transform(records):
    for r in records:
        yield {**r, "processed": True}

def load(records, sink):
    for r in records:
        sink.write(r)
```

## 2. Functional Pipeline Stages

- Mỗi stage là pure function.
- Dễ test, dễ reuse.

## 3. Protocol-Based Components

- Định nghĩa protocol/interface: Source, Sink, Transformer.
- Nhiều implementation (file, DB, API).

## 4. Configuration Management

- Dùng dataclass / Pydantic cho config.
- Config theo cấp: global → pipeline → stage.

## 5. Error Handling & Recovery

- Exception hierarchy rõ ràng.
- Retry với backoff.
- Dead-letter queue.

## 6. State Management

- PipelineState: status, counters, timestamps.
- Persist state để resume.

## 7. Type Safety & Data Validation

- Type hint everywhere.
- Pydantic / dataclass validation.

## 8. Performance Optimization

- Generator, batching, parallel (concurrent.futures, asyncio).

## 9. Observability & Monitoring

- Structured logging.
- Metrics: count, duration, error rate.

## 10. Testing Pipeline Components

- Unit test pure functions.
- Integration test với fake source/sink.

## 11. CQRS for Monitoring

- Write model: pipeline state.
- Read model: metrics, dashboard.

## 12. Fluent Pipeline Builder

- Builder pattern để dựng pipeline.

## 13. Descriptor-Based Validation

- Custom descriptor cho field validation.

## 14. Pattern Matching

- `match/case` để routing logic.

## 15. Policy Pattern

- Function-based policies cho validation rules.

## 16. Modern Python Features

- `@cache`, `pairwise`, `ExitStack`, `contextvars`, `:=`.

## 17. DRY Principles

- Extract common logic, tránh copy-paste.

## 18. API Design for Pipeline Services

- Pydantic request/response.
- Separated DTOs.