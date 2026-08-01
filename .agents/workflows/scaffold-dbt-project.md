# Scaffold dbt Project Workflow

## Mô tả
Quy trình tạo project dbt tự động dựa trên yêu cầu.

## Mục tiêu
- Tự động tạo cấu trúc project dbt theo chuẩn.
- Hỗ trợ các pattern: staging → intermediate → final.

## Các bước thực hiện

### 1. Nhận yêu cầu
- Xác định loại model (staging, intermediate, final)
- Xác định nguồn dữ liệu và cấu trúc
- Xác định yêu cầu kỹ thuật (SLA, data quality, etc.)

### 2. Tạo cấu trúc thư mục dbt
- `models/` - chứa các model SQL
- `tests/` - chứa các test SQL
- `seeds/` - dữ liệu mẫu (nếu cần)
- `snapshots/` - snapshot model (nếu cần)
- `macros/` - macro dùng chung (nếu cần)

### 3. Tạo các file mẫu dbt
- Model SQL cho từng dataset
- Schema YAML file cho model
- Test SQL cho validation

### 4. Cấu hình project dbt
- Tạo file `dbt_project.yml`
- Cấu hình connection và target
- Thiết lập các setting cho project

### 5. Kiểm tra và hoàn thiện
- Validate cấu trúc project dbt
- Kiểm tra các model có thể chạy độc lập
- Tạo tài liệu hướng dẫn sử dụng

## Ví dụ sử dụng

```
Tạo một project dbt cho user_events:
1. Tạo model staging từ table raw_user_events
2. Tạo model intermediate cho xử lý dữ liệu
3. Tạo model final cho BI/ML
4. Tạo schema.yml và test.sql cho từng model
5. Cấu hình project dbt với connection đúng
```

## Mẫu cấu trúc project dbt

```
user_events_dbt_project/
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

## Các yêu cầu kỹ thuật

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

## Ví dụ code model dbt

### Staging model:
```sql
-- models/staging/stg_user_events.sql

{{ config(
    materialized='table',
    schema='staging'
) }}

-- Staging model for user_events data

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'user_events') }}
),

renamed AS (
    SELECT
        id as user_id,
        name as user_name,
        created_at as user_created_at,
        *
    FROM source
)

SELECT * FROM renamed
```

### Intermediate model:
```sql
-- models/intermediate/int_user_events.sql

{{ config(
    materialized='table',
    schema='intermediate'
) }}

-- Intermediate model for user_events data

WITH stg_events AS (
    SELECT *
    FROM {{ ref('stg_user_events') }}
),

processed_events AS (
    SELECT
        user_id,
        user_name,
        user_created_at,
        -- Add processing logic here
        current_timestamp() as processed_at
    FROM stg_events
)

SELECT * FROM processed_events
```

### Final model:
```sql
-- models/final/fct_user_events.sql

{{ config(
    materialized='table',
    schema='final'
) }}

-- Final model for user_events data

WITH int_events AS (
    SELECT *
    FROM {{ ref('int_user_events') }}
)

SELECT
    user_id,
    user_name,
    user_created_at,
    processed_at,
    -- Add any final aggregations here
    count(*) as event_count
FROM int_events
GROUP BY user_id, user_name, user_created_at, processed_at
```

## Tài liệu hướng dẫn

### 1. Cấu trúc thư mục:
- `models/staging/`: Model staging cho dữ liệu thô
- `models/intermediate/`: Model trung gian
- `models/final/`: Model cuối cùng cho BI/ML
- `tests/`: Test cases cho model

### 2. Cách chạy project:
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

### 3. Cấu hình project:
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

## Kiểm tra chất lượng

### 1. Schema test:
- Test field type, nullability
- Test field name và description

### 2. Data quality test:
- Test uniqueness cho primary key
- Test range cho các field số
- Test relationship giữa các bảng

### 3. Performance test:
- Kiểm tra thời gian chạy model
- Kiểm tra hiệu suất truy vấn

## Cảnh báo và xử lý lỗi

### 1. Alerting:
- Cảnh báo khi test quality fail
- Cảnh báo khi model chạy quá lâu

### 2. Retry logic:
- Có thể cấu hình retry cho các model fail
- Log chi tiết về lỗi

### 3. Monitoring:
- Theo dõi thời gian chạy model
- Theo dõi số lượng bản ghi

## Tối ưu hóa hiệu suất

### 1. Partitioning:
- Sử dụng partitioning cho các table lớn
- Tối ưu query với filter trên partition key

### 2. CTE optimization:
- Tách các CTE phức tạp thành model nhỏ
- Sử dụng materialized CTE khi cần

### 3. Indexing:
- Tạo index cho các field thường dùng trong WHERE clause
- Sử dụng clustering khi hỗ trợ

## Kết luận

Quy trình scaffold dbt project giúp tạo cấu trúc chuẩn cho các project dbt, đảm bảo:
1. Tính nhất quán trong cấu trúc code
2. Khả năng tái sử dụng các model
3. Dễ dàng kiểm thử và bảo trì
4. Tuân thủ các best practices trong data modeling