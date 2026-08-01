# Testing Rules (Data Engineering)

## Mục tiêu
- Đảm bảo pipeline, model, và dữ liệu đúng như mong đợi.

## Cấp độ test
- **Unit**: hàm transform, logic nghiệp vụ.
- **Integration**: pipeline chạy trên dữ liệu mẫu.
- **End-to-end**: pipeline đầy đủ từ source → sink.
- **Data quality**: expectation, schema, rule.

## Nguyên tắc
- Test phải:
  - Nhanh, ổn định, không phụ thuộc môi trường thật.
  - Có dữ liệu mẫu nhỏ, cố định.
- Mọi thay đổi logic chính phải có test đi kèm.
- CI phải chạy:
  - Lint, type check.
  - Unit test.
  - Một số integration test quan trọng.

## Data test
- Test schema: column, type, nullability.
- Test rule: uniqueness, range, relationship.
- Test backfill: chạy lại không thay đổi kết quả.