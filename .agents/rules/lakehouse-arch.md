# Lakehouse Architecture Rules

## Mục tiêu
- Chuẩn hóa kiến trúc data lakehouse cho toàn bộ nền tảng dữ liệu.

## Nguyên tắc chung
- Lakehouse = Storage rẻ (object store) + Table format (Delta/Iceberg/Hudi) + Engine (Spark/Trino/Flink) + Catalog.
- Hỗ trợ cả batch và streaming trên cùng một nền tảng lưu trữ.
- Dữ liệu được tổ chức theo medallion: Bronze → Silver → Gold.

## Yêu cầu
- Mọi dataset quan trọng phải:
  - Nằm trên object store (S3/GCS/ADLS).
  - Dùng table format (Delta/Iceberg/Hudi).
  - Có catalog/metadata (Hive, Glue, Unity, Nessie…).
- Có chính sách:
  - Partitioning (theo ngày, event_type…).
  - Retention (xóa/archival theo thời gian).
  - Compaction/optimize định kỳ.

## Không được làm
- Không lưu dữ liệu production chỉ dưới dạng file rời (no schema, no version).
- Không bypass table format để đọc/ghi trực tiếp file Parquet/ORC.