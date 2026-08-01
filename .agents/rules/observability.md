# Observability Rules

## Mục tiêu
- Đảm bảo pipeline và dữ liệu có thể quan sát, debug, và cảnh báo.

## Logging
- Log có cấu trúc (JSON).
- Bao gồm: pipeline_id, run_id, stage, record_count, duration, error.

## Metrics
- Theo dõi:
  - Số bản ghi qua mỗi stage.
  - Thời gian chạy từng stage.
  - Tỷ lệ lỗi, số bản ghi dead-letter.
  - Freshness của dataset.

## Alerting
- Alert khi:
  - Pipeline fail liên tiếp N lần.
  - Dữ liệu trễ hơn SLA.
  - Tỷ lệ lỗi DQ vượt ngưỡng.
- Có routing alert theo domain/team.

## Tracing & Lineage
- Có thể truy vết:
  - Từ dashboard → model → pipeline → nguồn.
- Có metadata store (catalog) để tra lineage.