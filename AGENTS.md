# AGENTS.md — Hướng dẫn cho AI agent trong repo này

## Data Platform Documentation Rule (BẮT BUỘC)

**Mọi PR thay đổi hành vi của data platform phải cập nhật `docs/data-engineering-patterns.md` trong cùng PR đó.**

Cụ thể:

1. PR đụng tới bất kỳ phần nào sau đây được coi là "thay đổi data platform":
   - `src/dataingestion/**`, `src/data_processing/**`, `src/debezium/**`
   - `kubernetes-manifests/{kafka,minio,postgresql,spark-iceberg-config,dataingestion}.yaml`
   - `gitops/overlays/**` (Debezium, Schema Registry, CDC, topics)
   - `src/checkoutservice/**` khi thay đổi event producer
2. PR đó phải sửa `docs/data-engineering-patterns.md`: cập nhật bảng "Trạng thái tổng quan", mục pattern liên quan (kèm bằng chứng `file:line`), và dòng "Cập nhật lần cuối".
3. Nếu PR **không** thay đổi data platform, ghi rõ trong PR: `No data-platform behavior change`.
4. Checklist bắt buộc này nằm trong `.github/pull_request_template.md`.

Thiếu cập nhật tài liệu ⇒ PR chưa đủ điều kiện merge.

## Quy ước chung

- Ngôn ngữ tài liệu: tiếng Việt, giữ tên pattern/thuật ngữ tiếng Anh.
- Commit theo Conventional Commits (`feat(pipeline): ...`, `fix(dag): ...`).
- Trước khi code story mới: tra `.agents/knowledge/data-engineering-design-patterns-index.md` để chọn pattern.
- Không commit file local: `jira-sprint-2.json`, `.agents/knowledge/` (trừ khi được yêu cầu).
