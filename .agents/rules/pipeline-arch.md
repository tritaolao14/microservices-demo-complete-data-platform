# Pipeline Architecture Rules

## Mục tiêu
- Chuẩn hóa kiến trúc pipeline ETL/ELT.

## Nguyên tắc thiết kế
- Pipeline = Extract → Transform → Load.
- Mỗi stage:
  - Pure function càng nhiều càng tốt.
  - Có contract rõ ràng (input/output schema).
  - Có thể test độc lập.

## Kiểu pipeline
- **Batch**: chạy theo lịch (hourly, daily).
- **Streaming**: xử lý sự kiện theo thời gian thực.
- **Hybrid**: batch + streaming cho cùng một domain.

## Yêu cầu
- Idempotent: chạy lại không làm hỏng dữ liệu.
- Có checkpoint/resume cho pipeline dài.
- Có retry với backoff cho các bước I/O.
- Có dead-letter cho bản ghi lỗi.

## Tổ chức code
- Tách rõ:
  - `extract/`
  - `transform/`
  - `load/`
  - `contracts/`
  - `tests/`
- Không gọi API trực tiếp trong UI/dashboard; chỉ qua service layer.

## Pattern áp dụng
- Generator-based pipeline.
- Functional stages.
- Protocol-based components.
- Configuration-driven behavior.
- Error handling & retry.
- State tracking.
- Type safety & validation.
- Performance optimization.
- Observability & monitoring.
- Testing từng thành phần.