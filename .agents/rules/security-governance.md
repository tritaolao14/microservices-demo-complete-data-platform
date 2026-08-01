# Security & Governance Rules

## Mục tiêu
- Bảo vệ dữ liệu, tuân thủ chính sách và quy định.

## Phân loại dữ liệu
- Public, Internal, Confidential, PII, Sensitive.
- Mỗi loại có:
  - Chính sách truy cập.
  - Chính sách lưu trữ.
  - Chính sách chia sẻ.

## Access control
- Nguyên tắc least privilege.
- Role-based access (RBAC).
- Có audit log truy cập dữ liệu nhạy cảm.

## Bảo mật
- Mã hóa ở rest và in transit.
- Quản lý secret qua vault/secret manager.
- Không hardcode credential trong code.

## Governance
- Có data owner cho mỗi domain/dataset.
- Có quy trình:
  - Xin truy cập.
  - Thay đổi schema.
  - Xóa/dữ liệu hết hạn.