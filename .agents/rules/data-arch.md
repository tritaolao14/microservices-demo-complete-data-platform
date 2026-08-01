# Data Architecture Rules

## Mục tiêu
- Đảm bảo kiến trúc dữ liệu nhất quán, dễ mở rộng, dễ kiểm soát chất lượng.

## Phân tầng dữ liệu
- **Raw**: giữ nguyên bản từ nguồn, không sửa schema.
- **Staging**: chuẩn hóa schema, thêm metadata cơ bản.
- **Conformed/Clean**: áp dụng business rules, data quality.
- **Curated/Analytics**: phục vụ báo cáo, ML, BI.

## Nguyên tắc
- Mỗi tầng có:
  - Schema rõ ràng (data contract).
  - Owner (team/domain).
  - SLA/SLO (freshness, completeness).
- Không cho phép join trực tiếp Raw → Curated.
- Mọi transformation phải qua Staging hoặc Conformed.

## Schema & Contract
- Mọi dataset phải có:
  - Schema (JSON/Avro/Parquet).
  - Data contract (field, type, nullability, expectation).
- Thay đổi schema phải:
  - Có version.
  - Có migration plan.
  - Có backward compatibility hoặc deprecation plan.

## Lakehouse mapping
- Raw ↔ Bronze.
- Staging/Conformed ↔ Silver.
- Curated/Analytics ↔ Gold.