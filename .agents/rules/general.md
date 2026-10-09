# General Rules (Data Engineering)

## Mục đích
- Chuẩn hóa cách giao tiếp giữa các Agent.
- Định nghĩa quy trình làm việc, commit, review, và sửa lỗi tự động.

## Quy định giao tiếp
- Mọi yêu cầu phải rõ: domain, nguồn dữ liệu, đích, SLA/SLO.
- Khi thiếu thông tin, Agent phải hỏi trước khi sinh code.

## Chuẩn commit
- Commit message theo Conventional Commits:
  - `feat(pipeline): add user_events ETL`
  - `fix(dag): retry logic for extract_api`
  - `refactor(dbt): split staging models`
- Mỗi commit chỉ tập trung 1 domain: pipeline / dbt / dag / spark / quality / lakehouse.

## PR documentation (BẮT BUỘC)
- Mọi PR thay đổi hành vi data platform **phải cập nhật `docs/data-engineering-patterns.md`** trong cùng PR (bảng trạng thái, mục pattern kèm `file:line`, dòng "Cập nhật lần cuối").
- PR không đụng data platform phải ghi rõ: `No data-platform behavior change`.
- Chi tiết + danh sách path áp dụng: `AGENTS.md` → "Data Platform Documentation Rule".
- Thiếu cập nhật tài liệu ⇒ PR chưa đủ điều kiện merge.

## Tự sửa lỗi
- Khi test fail, Agent phải:
  1. Đọc log.
  2. Xác định nguyên nhân (data, logic, infra).
  3. Đề xuất patch code hoặc config.
  4. Chạy lại test trước khi báo “done”.

## Nguyên tắc chung
- Ưu tiên: an toàn dữ liệu > hiệu năng > tốc độ phát triển.
- Không thay đổi logic nghiệp vụ nếu không có yêu cầu rõ ràng.
- Không sinh code nếu chưa hiểu rõ:
  - Nguồn dữ liệu.
  - Đích dữ liệu.
  - Business rule cơ bản.