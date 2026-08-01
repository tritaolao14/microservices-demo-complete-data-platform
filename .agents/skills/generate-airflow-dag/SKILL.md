# Generate Airflow DAG Skill

## Mô tả
Kỹ năng tạo DAG Airflow tự động dựa trên yêu cầu pipeline.

## Mục tiêu
- Tự động tạo DAG Airflow với cấu trúc chuẩn.
- Hỗ trợ các pattern: task group, retry logic, alerting.

## Cách sử dụng
Khi yêu cầu tạo DAG Airflow:
1. Xác định pipeline cần chạy (ETL/ELT)
2. Xác định lịch chạy và SLA
3. Gọi kỹ năng để tạo DAG

## Các mẫu được hỗ trợ
- DAG Python với các task chính
- Task group cho tổ chức logic
- Retry và timeout configuration

## Ví dụ sử dụng
```
Tạo một DAG Airflow cho pipeline user_events:
1. DAG chạy mỗi ngày
2. Task extract → transform → load
3. Có retry logic và alerting
```

## Cấu trúc DAG được tạo
- `dags/` - chứa các DAG files
- Mỗi DAG có cấu trúc:
  - Task group cho từng stage (extract, transform, load)
  - Retry logic với backoff
  - Alert configuration
  - SLA check

## Các yêu cầu kỹ thuật
- Sử dụng PythonOperator cho các task
- Có retry với backoff (5 attempts, exponential backoff)
- Có timeout cho mỗi task (30 phút)
- Có alert khi pipeline fail liên tiếp 3 lần
- Có logging cấu trúc (JSON)