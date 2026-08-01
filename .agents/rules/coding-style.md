# Coding Style Rules (Data Engineering)

## Ngôn ngữ
- Python cho pipeline, Spark, utility.
- SQL cho transformation (dbt, warehouse).
- YAML cho config, contract, DQ rules.

## Python
- Type hint cho mọi hàm public.
- Docstring ngắn gọn: mục đích, input, output.
- Ưu tiên hàm pure, dễ test.
- Sử dụng generator/stream cho dữ liệu lớn.
- Không load toàn bộ dataset vào memory.

## SQL
- Tên cột, bảng: snake_case.
- Mô hình: staging → intermediate → final.
- Có comment cho logic phức tạp.
- Không viết query quá 300 dòng; tách thành CTE/model nhỏ.

## Config & Contract
- YAML rõ ràng, có comment.
- Tên field, dataset: snake_case.
- Có version cho contract khi thay đổi.