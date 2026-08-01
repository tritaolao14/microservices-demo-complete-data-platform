# Lakehouse Governance

## Mục tiêu
Hướng dẫn quản lý dữ liệu trong lakehouse.

## Tổng quan quản lý

### Quản lý dữ liệu là quá trình kiểm soát và đảm bảo chất lượng dữ liệu trong toàn bộ hệ sinh thái.

## Phân loại dữ liệu

### Theo mức độ nhạy cảm:
1. **Public**: Dữ liệu công khai, không có giới hạn truy cập
2. **Internal**: Dữ liệu nội bộ, chỉ dùng cho các thành phần trong tổ chức
3. **Confidential**: Dữ liệu nhạy cảm, cần kiểm soát truy cập nghiêm ngặt
4. **PII**: Dữ liệu cá nhân (Personal Identifiable Information)
5. **Sensitive**: Dữ liệu nhạy cảm, cần bảo vệ đặc biệt

### Ví dụ phân loại:
```yaml
data_classification:
  public:
    description: "Dữ liệu công khai"
    access_control: "open"
    retention: "infinite"
    
  internal:
    description: "Dữ liệu nội bộ"
    access_control: "role-based"
    retention: "1 year"
    
  confidential:
    description: "Dữ liệu nhạy cảm"
    access_control: "role-based + encryption"
    retention: "3 years"
    
  pii:
    description: "Dữ liệu cá nhân"
    access_control: "strict + encryption"
    retention: "5 years"
    
  sensitive:
    description: "Dữ liệu nhạy cảm đặc biệt"
    access_control: "strict + encryption + audit"
    retention: "10 years"
```

## Chính sách truy cập

### Nguyên tắc least privilege:
- Chỉ cấp quyền truy cập tối thiểu cần thiết
- Mỗi người dùng chỉ có quyền truy cập dữ liệu mà họ cần

### RBAC (Role-Based Access Control):
```yaml
access_control:
  roles:
    - name: "data_engineer"
      permissions:
        - read: "bronze/*"
        - write: "silver/*"
        - execute: "spark_jobs/*"
    
    - name: "analyst"
      permissions:
        - read: "gold/*"
        - query: "analytics/*"
    
    - name: "admin"
      permissions:
        - read: "*"
        - write: "*"
        - manage: "metadata/*"
```

### Audit log:
```yaml
audit_logging:
  enabled: true
  fields:
    - user_id
    - action
    - timestamp
    - resource_accessed
    - ip_address
  retention: "1 year"
```

## Bảo mật dữ liệu

### Mã hóa:
1. **At rest**: Mã hóa dữ liệu khi lưu trữ
2. **In transit**: Mã hóa dữ liệu khi truyền tải

### Ví dụ cấu hình mã hóa:
```yaml
encryption:
  at_rest:
    enabled: true
    algorithm: "AES-256"
    
  in_transit:
    enabled: true
    protocol: "TLS 1.3"
    
  key_management:
    provider: "AWS KMS"
    rotation_period: "90 days"
```

### Quản lý secret:
```yaml
secret_management:
  provider: "AWS Secrets Manager"
  rotation_policy:
    enabled: true
    interval: "90 days"
    
  access_control:
    - service_account: "spark-service-account"
      permissions: "read"
    - service_account: "airflow-service-account"
      permissions: "read/write"
```

## Chính sách quản lý dữ liệu

### Data Owner:
- Mỗi dataset có một owner
- Owner chịu trách nhiệm về chất lượng dữ liệu

### Ví dụ cấu hình:
```yaml
data_owner:
  dataset: "user_events"
  owner: "data-engineering-team@company.com"
  contact: "data-engineering-team@company.com"
  responsibilities:
    - data_quality
    - schema_evolution
    - retention_policy
```

### Data Lineage:
```yaml
lineage_tracking:
  enabled: true
  metadata_store: "glue_catalog"
  update_frequency: "real_time"
  
  relationships:
    - source: "kafka/user_events"
      transformation: "spark_job"
      destination: "delta/user_events"
```

## Chính sách retention

### Retention policy:
1. **Raw data**: 30 ngày
2. **Bronze data**: 90 ngày
3. **Silver data**: 1 năm
4. **Gold data**: 3 năm

### Ví dụ cấu hình:
```yaml
retention_policy:
  raw:
    enabled: true
    days: 30
    archive_to: "s3://archive/raw"
    
  bronze:
    enabled: true
    days: 90
    archive_to: "s3://archive/bronze"
    
  silver:
    enabled: true
    days: 365
    archive_to: "s3://archive/silver"
    
  gold:
    enabled: true
    days: 1095
    archive_to: "s3://archive/gold"
```

## Chính sách compaction

### Compaction:
1. **Delta tables**: Tự động hoặc thủ công
2. **Iceberg tables**: Định kỳ
3. **Hudi tables**: Theo chiến lược

### Ví dụ cấu hình:
```yaml
compaction_policy:
  delta_tables:
    enabled: true
    strategy: "major"
    interval_days: 7
    
  iceberg_tables:
    enabled: true
    strategy: "minor"
    interval_days: 30
    
  hudi_tables:
    enabled: true
    strategy: "major"
    interval_days: 14
```

## Chính sách versioning

### Schema versioning:
```yaml
schema_versioning:
  enabled: true
  format: "semantic_versioning"
  migration_plan:
    - version: "1.0.0"
      changes: "Initial schema"
    - version: "1.1.0"
      changes: "Added timestamp field"
      
  backward_compatibility:
    enabled: true
    policy: "allow"
```

## Chính sách kiểm tra chất lượng

### Data quality:
```yaml
data_quality:
  enabled: true
  rules:
    - name: "schema_validation"
      severity: "error"
      threshold: 0.95
      
    - name: "completeness_check"
      severity: "warning"
      threshold: 0.99
      
    - name: "uniqueness_check"
      severity: "error"
      threshold: 1.0
      
  alerts:
    - name: "high_error_rate"
      condition: "error_count > 100"
      severity: "critical"
```

## Ví dụ quy trình quản lý dữ liệu

### Tạo dataset mới:
1. Xác định phân loại dữ liệu
2. Thiết lập chính sách truy cập
3. Cấu hình retention policy
4. Gán data owner

### Ví dụ:
```yaml
new_dataset_process:
  dataset_name: "user_events"
  classification: "confidential"
  access_control:
    - role: "data_engineer"
      permissions: "read/write"
    - role: "analyst"
      permissions: "read"
  retention_policy:
    raw: 30 days
    bronze: 90 days
    silver: 1 year
    gold: 3 years
  owner: "data-engineering-team@company.com"
```

## Quản lý metadata

### Metadata store:
```yaml
metadata_store:
  type: "glue_catalog"
  sync_frequency: "real_time"
  
  dataset_metadata:
    - name: "user_events"
      description: "User event data from mobile app"
      schema:
        - field: "id"
          type: "string"
          nullable: false
        - field: "event_type"
          type: "string"
          nullable: true
      lineage:
        source: "kafka/user_events"
        transformation: "spark_job"
        destination: "delta/user_events"
```

## Ví dụ kiểm tra chính sách

### Kiểm tra access control:
```python
def validate_access_control(user_role, resource):
    """Validate if user has access to resource."""
    
    # Define access control rules
    access_rules = {
        "data_engineer": ["read:bronze/*", "write:silver/*"],
        "analyst": ["read:gold/*"],
        "admin": ["read:*", "write:*"]
    }
    
    # Check if user has access
    if user_role in access_rules:
        return resource in access_rules[user_role]
    
    return False

# Example usage
has_access = validate_access_control("data_engineer", "read:bronze/user_events")
```

## Quản lý thay đổi schema

### Schema evolution:
```yaml
schema_evolution:
  process:
    - step: "create_new_version"
      action: "create new schema version"
      
    - step: "update_metadata"
      action: "update catalog with new schema"
      
    - step: "validate_compatibility"
      action: "check backward compatibility"
      
    - step: "notify_stakeholders"
      action: "send notification to data owners"
```

## Ví dụ kiểm tra compliance

### Compliance check:
```python
def check_compliance(dataset, policy):
    """Check if dataset complies with policy."""
    
    # Check data classification
    if not validate_classification(dataset, policy.classification):
        return False
    
    # Check retention policy
    if not validate_retention(dataset, policy.retention):
        return False
    
    # Check access control
    if not validate_access_control(dataset, policy.access_control):
        return False
    
    # Check encryption
    if not validate_encryption(dataset, policy.encryption):
        return False
    
    return True

# Example usage
compliant = check_compliance("user_events", policy)
```

## Kết luận

Quản lý dữ liệu trong lakehouse là một quá trình toàn diện bao gồm:

1. **Phân loại dữ liệu**: Xác định mức độ nhạy cảm
2. **Chính sách truy cập**: Kiểm soát quyền truy cập theo vai trò
3. **Bảo mật**: Mã hóa dữ liệu và quản lý secret
4. **Retention policy**: Quản lý thời gian lưu trữ
5. **Metadata management**: Theo dõi và quản lý metadata
6. **Data quality**: Kiểm tra chất lượng dữ liệu

Việc áp dụng các chính sách này giúp đảm bảo rằng hệ thống lakehouse của bạn tuân thủ các tiêu chuẩn bảo mật và quản lý dữ liệu tốt nhất.