# dbt Project Template

## Mô tả
Mẫu project dbt cho hệ thống data warehouse.

## Cấu trúc thư mục
```
dbt_project/
├── models/
│   ├── staging/
│   │   └── stg_user_events.sql
│   ├── intermediate/
│   │   └── int_user_events.sql
│   └── final/
│       └── fct_user_events.sql
├── tests/
│   ├── test_user_events_schema.sql
│   └── test_user_events_uniqueness.sql
├── seeds/
│   └── user_events_sample.csv
├── snapshots/
│   └── snapshot_user_events.sql
├── macros/
│   └── date_functions.sql
├── dbt_project.yml
└── packages.yml
```

## Đặc điểm

### 1. Naming convention:
- Tên cột, bảng: snake_case
- Mô hình: staging → intermediate → final

### 2. Comment cho logic phức tạp:
- Mỗi model có comment giải thích logic
- Có comment cho các CTE nếu quá dài

### 3. Tách CTE:
- Không viết query quá 300 dòng
- Tách thành CTE/model nhỏ

### 4. Schema YAML:
- Định nghĩa field cho model
- Có test cho từng field

### 5. Test SQL:
- Có test schema: column, type, nullability
- Có test rule: uniqueness, range, relationship

## Ví dụ sử dụng

```
Tạo một project dbt cho user_events:
1. Tạo model staging từ table raw_user_events
2. Tạo model intermediate cho xử lý dữ liệu
3. Tạo model final cho BI/ML
4. Tạo schema.yml và test.sql cho từng model
5. Cấu hình project dbt với connection đúng
```

## Tài liệu hướng dẫn

### 1. Cách chạy project:
```bash
# Cài đặt dependencies
pip install dbt

# Chạy project dbt
dbt run

# Chạy test cases
dbt test

# Generate documentation
dbt docs generate
```

### 2. Cấu hình project:
```yaml
# dbt_project.yml
name: 'user_events_dbt_project'
version: '1.0.0'
config-version: 2

# Configure your target
target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

# Define sources
sources:
  raw:
    tables:
      user_events:
        name: "raw_user_events"
        schema: "raw"

# Define models
models:
  user_events_dbt_project:
    staging:
      +materialized: table
    intermediate:
      +materialized: table
    final:
      +materialized: table
```

### 3. Kiểm tra chất lượng:
- Test schema cho từng field
- Test uniqueness cho primary key
- Test range cho các field số
```