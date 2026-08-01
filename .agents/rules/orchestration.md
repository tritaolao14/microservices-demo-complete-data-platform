# Orchestration Rules

## Mục tiêu
- Chuẩn hóa cách định nghĩa, chạy, và giám sát pipeline.

## Công cụ khuyến nghị
- Airflow, Dagster, Prefect (tùy stack).

## Nguyên tắc DAG
- Mỗi DAG = 1 business process.
- Task nhỏ, rõ nhiệm vụ.
- Có retry, timeout, alert.
- Có dependency rõ ràng giữa các task.

## Lịch chạy
- Dựa theo SLA của dataset.
- Tránh chạy quá tải vào cùng khung giờ.
- Có lịch backfill rõ ràng.

## Quản lý môi trường
- Tách rõ: dev / staging / prod.
- Không dùng chung connection, secret giữa các môi trường.
- Có quy trình promote DAG từ dev → prod.