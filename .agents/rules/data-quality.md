# Data Quality Rules

## Mục tiêu
- Đảm bảo dữ liệu đúng, đủ, tin cậy.

## Loại kiểm tra
- **Schema**: type, nullability, format.
- **Completeness**: không thiếu bản ghi quan trọng.
- **Uniqueness**: không trùng khóa chính.
- **Consistency**: nhất quán giữa các bảng/hệ thống.
- **Timeliness**: đúng SLA freshness.
- **Validity**: giá trị nằm trong miền cho phép.

## Triển khai
- Mỗi dataset chính phải có:
  - Data contract.
  - Bộ rule chất lượng (expectations).
  - Test tự động (SQL/Python).
- Kết quả DQ phải:
  - Được log, metric hóa.
  - Có alert khi vượt ngưỡng.

## Xử lý lỗi
- Phân loại:
  - Blocking: dừng pipeline, alert ngay.
  - Warning: ghi log, gửi cảnh báo, vẫn chạy.
- Có quy trình hotfix dữ liệu khi phát hiện lỗi nghiêm trọng.