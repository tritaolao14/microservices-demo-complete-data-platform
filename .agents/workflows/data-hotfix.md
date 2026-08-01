# Data Hotfix Workflow

## Mô tả
Quy trình xử lý hotfix dữ liệu khi phát hiện lỗi nghiêm trọng.

## Mục tiêu
- Tự động nhận diện và xử lý hotfix dữ liệu.
- Hỗ trợ các pattern: data quality, data validation, data recovery.

## Các bước thực hiện

### 1. Nhận yêu cầu
- Xác định loại lỗi dữ liệu (data quality, validation, recovery)
- Xác định mức độ nghiêm trọng (blocking, warning)
- Xác định yêu cầu kỹ thuật (SLA, rollback, alerting)

### 2. Phân tích dữ liệu lỗi
- Đọc và phân tích dữ liệu bị ảnh hưởng
- Xác định nguyên nhân lỗi chính (data, logic, infra)
- Phân loại lỗi theo mức độ nghiêm trọng

### 3. Tạo hotfix tự động
- Đề xuất giải pháp xử lý dữ liệu
- Có khả năng rollback nếu cần
- Tạo báo cáo chi tiết về hotfix

### 4. Cấu hình hotfix
- Tạo file config cho hotfix
- Cấu hình các loại lỗi có thể xử lý tự động
- Thiết lập cảnh báo và theo dõi

### 5. Kiểm tra và hoàn thiện
- Validate hotfix có hiệu quả
- Kiểm tra dữ liệu sau khi xử lý
- Tạo tài liệu hướng dẫn sử dụng

## Ví dụ sử dụng

```
Xử lý hotfix dữ liệu cho user_events:
1. Phát hiện lỗi dữ liệu nghiêm trọng
2. Đọc và phân tích dữ liệu bị ảnh hưởng
3. Đề xuất giải pháp xử lý dữ liệu
4. Có khả năng rollback nếu cần
5. Tạo báo cáo chi tiết về hotfix
```

## Mẫu cấu trúc data hotfix

```
data_hotfix/
├── analyzers/
│   ├── data_analyzer.py
│   ├── quality_analyzer.py
│   └── recovery_analyzer.py
├── fixes/
│   ├── data_fixes.py
│   └── recovery_fixes.py
├── reports/
│   └── hotfix_report_generator.py
├── config/
│   └── data_hotfix_config.yaml
└── main.py
```

## Các yêu cầu kỹ thuật

### 1. Data analysis:
- Đọc và phân tích dữ liệu bị ảnh hưởng
- Xác định nguyên nhân lỗi chính
- Phân loại lỗi theo mức độ nghiêm trọng

### 2. Quality analysis:
- Kiểm tra data quality rules (expectations)
- Xác định bản ghi lỗi
- Có quy trình xử lý riêng cho từng loại lỗi

### 3. Recovery handling:
- Có khả năng rollback dữ liệu
- Có quy trình khôi phục dữ liệu từ backup
- Có kiểm tra tính toàn vẹn dữ liệu

### 4. Reporting:
- Tạo báo cáo chi tiết về hotfix
- Có routing cảnh báo theo domain/team
- Có log chi tiết về quá trình xử lý

## Ví dụ code data hotfix

### Main data hotfix:
```python
"""Data Hotfix Agent for automatically fixing data issues."""

import logging
from typing import Dict, Any, List
from analyzers.data_analyzer import DataAnalyzer
from analyzers.quality_analyzer import QualityAnalyzer
from analyzers.recovery_analyzer import RecoveryAnalyzer
from fixes.data_fixes import DataFixes
from fixes.recovery_fixes import RecoveryFixes

class DataHotfixAgent:
    """Data Hotfix Agent for automatically fixing data issues."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.data_analyzer = DataAnalyzer()
        self.quality_analyzer = QualityAnalyzer()
        self.recovery_analyzer = RecoveryAnalyzer()
        self.data_fixes = DataFixes()
        self.recovery_fixes = RecoveryFixes()
        
    def analyze_and_fix(self, dataset_name: str, error_type: str, 
                        affected_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze data issue and suggest fixes."""
        
        # Step 1: Analyze data
        data_analysis = self.data_analyzer.analyze(dataset_name, affected_records)
        
        # Step 2: Analyze quality issues
        quality_issues = self.quality_analyzer.analyze(data_analysis)
        
        # Step 3: Determine fix approach
        fix_approach = self.determine_fix_approach(error_type, quality_issues)
        
        # Step 4: Apply fix if possible
        fix_result = self.apply_fix(fix_approach, dataset_name, affected_records)
        
        # Step 5: Validate fix
        validation_result = self.validate_fix(dataset_name, affected_records)
        
        return {
            "dataset": dataset_name,
            "error_type": error_type,
            "affected_records_count": len(affected_records),
            "analysis": data_analysis,
            "quality_issues": quality_issues,
            "fix_approach": fix_approach,
            "fix_applied": fix_result["applied"],
            "validation": validation_result,
            "report": self.generate_report(data_analysis, quality_issues, 
                                         fix_approach, validation_result)
        }
    
    def determine_fix_approach(self, error_type: str, quality_issues: Dict[str, Any]) -> Dict[str, Any]:
        """Determine the appropriate fix approach based on error type."""
        
        approaches = {
            "schema_violation": {
                "type": "data",
                "priority": "high",
                "action": "correct_schema",
                "rollback_available": True
            },
            "data_missing": {
                "type": "data",
                "priority": "medium",
                "action": "fill_missing_data",
                "rollback_available": True
            },
            "data_duplicate": {
                "type": "data",
                "priority": "high",
                "action": "remove_duplicates",
                "rollback_available": True
            },
            "data_out_of_range": {
                "type": "data",
                "priority": "medium",
                "action": "correct_range",
                "rollback_available": True
            },
            "data_corruption": {
                "type": "recovery",
                "priority": "critical",
                "action": "restore_from_backup",
                "rollback_available": False
            }
        }
        
        return approaches.get(error_type, {
            "type": "unknown",
            "priority": "low",
            "action": "manual_review_required",
            "rollback_available": False
        })
    
    def apply_fix(self, fix_approach: Dict[str, Any], dataset_name: str, 
                  affected_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply the determined fix approach."""
        
        try:
            if fix_approach["type"] == "data":
                return self.data_fixes.apply(fix_approach, dataset_name, affected_records)
            elif fix_approach["type"] == "recovery":
                return self.recovery_fixes.apply(fix_approach, dataset_name, affected_records)
            else:
                return {"applied": False, "error": "Unknown fix type"}
                
        except Exception as e:
            return {"applied": False, "error": str(e)}
    
    def validate_fix(self, dataset_name: str, affected_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate that the fix was applied correctly."""
        
        try:
            # This would run validation checks on the fixed data
            # For example: re-run quality checks
            
            return {
                "validated": True,
                "issues_fixed": len(affected_records),
                "validation_details": "All quality checks passed after fix"
            }
            
        except Exception as e:
            return {
                "validated": False,
                "error": str(e),
                "validation_details": "Validation failed after fix"
            }
    
    def generate_report(self, data_analysis: Dict[str, Any], quality_issues: Dict[str, Any],
                       fix_approach: Dict[str, Any], validation_result: Dict[str, Any]) -> str:
        """Generate detailed report about the hotfix."""
        
        report = "Data Hotfix Report\n"
        report += "=" * 40 + "\n\n"
        
        report += f"Dataset: {data_analysis.get('dataset', 'Unknown')}\n"
        report += f"Error Type: {data_analysis.get('error_type', 'Unknown')}\n"
        report += f"Affected Records: {data_analysis.get('affected_count', 0)}\n\n"
        
        report += "Data Analysis:\n"
        for key, value in data_analysis.items():
            report += f"  {key}: {value}\n"
        
        report += "\nQuality Issues:\n"
        for key, value in quality_issues.items():
            report += f"  {key}: {value}\n"
        
        report += "\nFix Approach:\n"
        for key, value in fix_approach.items():
            report += f"  {key}: {value}\n"
        
        report += "\nValidation Result:\n"
        for key, value in validation_result.items():
            report += f"  {key}: {value}\n"
        
        return report

def main():
    """Main function to run data hotfix agent."""
    
    # Configuration
    config = {
        "data_analysis": {
            "enabled": True,
            "rules": ["schema_validation", "quality_check"]
        },
        "quality_analysis": {
            "enabled": True,
            "rules": ["expectations", "completeness"]
        },
        "auto_fix": {
            "enabled": True,
            "types": ["data", "recovery"],
            "severity_threshold": "high"
        },
        "rollback_support": {
            "enabled": True,
            "backup_location": "/backups/data_hotfix"
        }
    }
    
    # Initialize data hotfix agent
    hotfix_agent = DataHotfixAgent(config)
    
    # Simulate affected records
    affected_records = [
        {"id": "1", "name": "John Doe", "email": "john@example.com"},
        {"id": "2", "name": "Jane Smith", "email": "jane@example.com"}
    ]
    
    # Run hotfix
    result = hotfix_agent.analyze_and_fix("user_events", "data_missing", affected_records)
    
    # Print report
    print(result["report"])

if __name__ == "__main__":
    main()
```

### Data analyzer:
```python
"""Data analyzer for data hotfix agent."""

from typing import Dict, Any, List

class DataAnalyzer:
    """Data analyzer for data hotfix agent."""
    
    def __init__(self):
        self.analysis_rules = [
            "schema_validation",
            "data_integrity",
            "quality_check"
        ]
        
    def analyze(self, dataset_name: str, affected_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze data for issues."""
        
        analysis = {
            "dataset": dataset_name,
            "affected_count": len(affected_records),
            "analysis_timestamp": "2023-01-01T00:00:00Z",
            "data_summary": {
                "total_records": len(affected_records),
                "field_distribution": {},
                "missing_fields": [],
                "duplicate_count": 0
            }
        }
        
        # Analyze each record for issues
        if affected_records:
            # Get field names from first record
            fields = list(affected_records[0].keys())
            
            # Count missing fields
            missing_fields = []
            for record in affected_records:
                for field in fields:
                    if field not in record or record[field] is None:
                        missing_fields.append(field)
            
            analysis["data_summary"]["missing_fields"] = list(set(missing_fields))
            
            # Count duplicates (if any)
            if len(affected_records) > 1:
                # Simple duplicate check based on ID field
                ids = [record.get("id") for record in affected_records if "id" in record]
                unique_ids = set(ids)
                analysis["data_summary"]["duplicate_count"] = len(ids) - len(unique_ids)
            
            # Field distribution
            analysis["data_summary"]["field_distribution"] = {
                field: sum(1 for record in affected_records if field in record) 
                for field in fields
            }
        
        return analysis

    def extract_data_issues(self, dataset_name: str, affected_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Extract specific data issues from records."""
        
        issues = {
            "missing_data": [],
            "invalid_format": [],
            "out_of_range": [],
            "duplicate_records": []
        }
        
        # Check for missing data
        for i, record in enumerate(affected_records):
            missing_fields = [field for field, value in record.items() if value is None or value == ""]
            if missing_fields:
                issues["missing_data"].append({
                    "record_index": i,
                    "missing_fields": missing_fields
                })
        
        return issues
```

### Quality analyzer:
```python
"""Quality analyzer for data hotfix agent."""

from typing import Dict, Any

class QualityAnalyzer:
    """Quality analyzer for data hotfix agent."""
    
    def __init__(self):
        self.quality_rules = {
            "schema_compliance": "Validate schema compliance",
            "data_completeness": "Check for missing data",
            "data_uniqueness": "Validate uniqueness constraints",
            "data_consistency": "Check data consistency",
            "data_timeliness": "Validate freshness of data"
        }
        
    def analyze(self, data_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze data quality issues."""
        
        quality_issues = {
            "schema_violations": [],
            "completeness_issues": [],
            "uniqueness_issues": [],
            "consistency_issues": [],
            "timeliness_issues": []
        }
        
        # Analyze based on data analysis results
        if "missing_fields" in data_analysis["data_summary"]:
            missing_fields = data_analysis["data_summary"]["missing_fields"]
            if missing_fields:
                quality_issues["completeness_issues"] = {
                    "count": len(missing_fields),
                    "details": f"Missing fields: {missing_fields}"
                }
        
        if "duplicate_count" in data_analysis["data_summary"]:
            duplicate_count = data_analysis["data_summary"]["duplicate_count"]
            if duplicate_count > 0:
                quality_issues["uniqueness_issues"] = {
                    "count": duplicate_count,
                    "details": f"Found {duplicate_count} duplicate records"
                }
        
        # Simulate quality rule violations
        if "affected_count" in data_analysis and data_analysis["affected_count"] > 0:
            # Example: Simulate some quality issues
            quality_issues["schema_violations"] = {
                "count": 0,
                "details": "No schema violations found"
            }
            
            quality_issues["consistency_issues"] = {
                "count": 0,
                "details": "No consistency issues found"
            }
        
        return quality_issues

    def validate_expectations(self, dataset_name: str, records: list) -> Dict[str, Any]:
        """Validate data against expectations."""
        
        # This would check actual expectations (e.g., from DQ rules)
        # For now, return a simplified validation
        
        return {
            "passed": True,
            "failed_expectations": [],
            "details": "All expectations validated successfully"
        }
```

### Recovery analyzer:
```python
"""Recovery analyzer for data hotfix agent."""

from typing import Dict, Any

class RecoveryAnalyzer:
    """Recovery analyzer for data hotfix agent."""
    
    def __init__(self):
        self.recovery_strategies = [
            "backup_restore",
            "data_repair",
            "manual_recovery"
        ]
        
    def analyze(self, dataset_name: str, affected_records: list) -> Dict[str, Any]:
        """Analyze recovery needs for data."""
        
        recovery_analysis = {
            "recovery_needed": False,
            "recovery_strategy": None,
            "backup_available": True,
            "rollback_required": False,
            "recovery_details": {}
        }
        
        # Determine if recovery is needed based on error type
        # This would be more sophisticated in real implementation
        
        if len(affected_records) > 0:
            # Simulate that recovery might be needed for some issues
            recovery_analysis["recovery_needed"] = True
            recovery_analysis["recovery_strategy"] = "backup_restore"
            recovery_analysis["rollback_required"] = True
            
            # Add details about what needs to be recovered
            recovery_analysis["recovery_details"] = {
                "dataset": dataset_name,
                "affected_records_count": len(affected_records),
                "backup_location": "/backups/data_hotfix",
                "recovery_timestamp": "2023-01-01T00:00:00Z"
            }
        
        return recovery_analysis

    def validate_backup(self, dataset_name: str) -> bool:
        """Validate that backup exists for dataset."""
        
        # This would check if backup files exist
        # For now, return True as a simplified example
        
        return True

    def get_recovery_plan(self, dataset_name: str) -> Dict[str, Any]:
        """Get recovery plan for dataset."""
        
        return {
            "plan": [
                "Verify backup exists",
                "Restore from backup",
                "Validate restored data",
                "Update metadata"
            ],
            "estimated_time": "5 minutes",
            "risk_level": "low"
        }
```

### Data fixes:
```python
"""Data fixes for data hotfix agent."""

from typing import Dict, Any, List

class DataFixes:
    """Data fixes for data hotfix agent."""
    
    def __init__(self):
        self.fix_strategies = {
            "correct_schema": "Fix schema violations",
            "fill_missing_data": "Fill missing data fields",
            "remove_duplicates": "Remove duplicate records",
            "correct_range": "Correct out-of-range values"
        }
        
    def apply(self, fix_approach: Dict[str, Any], dataset_name: str, 
              affected_records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply data fix to affected records."""
        
        try:
            # Simulate applying fix based on approach
            if fix_approach["action"] == "correct_schema":
                # Example: Fix schema by adding missing fields
                for record in affected_records:
                    if "timestamp" not in record:
                        record["timestamp"] = "2023-01-01T00:00:00Z"
                
                return {
                    "applied": True,
                    "type": "schema_fix",
                    "details": f"Applied schema fix to {len(affected_records)} records"
                }
                
            elif fix_approach["action"] == "fill_missing_data":
                # Example: Fill missing data with default values
                for record in affected_records:
                    if "status" not in record:
                        record["status"] = "pending"
                
                return {
                    "applied": True,
                    "type": "data_fill",
                    "details": f"Filled missing data in {len(affected_records)} records"
                }
                
            elif fix_approach["action"] == "remove_duplicates":
                # Example: Remove duplicates (simplified)
                unique_records = []
                seen_ids = set()
                
                for record in affected_records:
                    record_id = record.get("id")
                    if record_id and record_id not in seen_ids:
                        unique_records.append(record)
                        seen_ids.add(record_id)
                
                return {
                    "applied": True,
                    "type": "duplicate_removal",
                    "details": f"Removed {len(affected_records) - len(unique_records)} duplicates"
                }
                
            else:
                return {
                    "applied": False,
                    "error": f"Unknown fix action: {fix_approach['action']}",
                    "details": "Failed to apply data fix"
                }
                
        except Exception as e:
            return {
                "applied": False,
                "error": str(e),
                "details": f"Failed to apply data fix: {str(e)}"
            }

    def rollback(self, dataset_name: str, backup_location: str) -> Dict[str, Any]:
        """Rollback data fix using backup."""
        
        try:
            # Simulate rollback from backup
            return {
                "rolled_back": True,
                "details": f"Rolled back {dataset_name} from backup"
            }
            
        except Exception as e:
            return {
                "rolled_back": False,
                "error": str(e),
                "details": f"Failed to rollback {dataset_name}: {str(e)}"
            }
```

### Recovery fixes:
```python
"""Recovery fixes for data hotfix agent."""

from typing import Dict, Any

class RecoveryFixes:
    """Recovery fixes for data hotfix agent."""
    
    def __init__(self):
        self.recovery_strategies = {
            "restore_from_backup": "Restore from backup",
            "repair_data": "Repair corrupted data",
            "manual_recovery": "Manual recovery process"
        }
        
    def apply(self, fix_approach: Dict[str, Any], dataset_name: str, 
              affected_records: list) -> Dict[str, Any]:
        """Apply recovery fix to dataset."""
        
        try:
            # Simulate applying recovery fix
            if fix_approach["action"] == "restore_from_backup":
                # Example: Restore from backup
                return {
                    "applied": True,
                    "type": "recovery",
                    "details": f"Restored {dataset_name} from backup",
                    "backup_used": "/backups/data_hotfix/user_events_2023-01-01.bak"
                }
                
            elif fix_approach["action"] == "repair_data":
                # Example: Repair corrupted data
                return {
                    "applied": True,
                    "type": "repair",
                    "details": f"Repaired corrupted data in {dataset_name}"
                }
                
            else:
                return {
                    "applied": False,
                    "error": f"Unknown recovery action: {fix_approach['action']}",
                    "details": "Failed to apply recovery fix"
                }
                
        except Exception as e:
            return {
                "applied": False,
                "error": str(e),
                "details": f"Failed to apply recovery fix: {str(e)}"
            }

    def validate_recovery(self, dataset_name: str) -> Dict[str, Any]:
        """Validate that recovery was successful."""
        
        try:
            # Simulate validation of recovery
            return {
                "validated": True,
                "details": f"Recovery validation successful for {dataset_name}",
                "integrity_check": "passed"
            }
            
        except Exception as e:
            return {
                "validated": False,
                "error": str(e),
                "details": f"Recovery validation failed for {dataset_name}: {str(e)}"
            }
```

## Tài liệu hướng dẫn

### 1. Cấu trúc thư mục:
- `analyzers/` - các công cụ phân tích dữ liệu
- `fixes/` - các sửa lỗi tự động
- `reports/` - báo cáo hotfix
- `config/` - file cấu hình

### 2. Cách chạy data hotfix:
```bash
# Cài đặt dependencies
pip install pyyaml

# Chạy data hotfix agent
python data_hotfix/main.py

# Hoặc chạy với cấu hình cụ thể
python data_hotfix/main.py --config config/data_hotfix_config.yaml
```

### 3. Cấu hình data hotfix:
```yaml
# config/data_hotfix_config.yaml
data_hotfix:
  data_analysis:
    enabled: true
    rules: ["schema_validation", "data_integrity"]
    
  quality_analysis:
    enabled: true
    rules: ["expectations", "completeness"]
    
  auto_fix:
    enabled: true
    types: ["data", "recovery"]
    severity_threshold: "critical"
    
  rollback_support:
    enabled: true
    backup_location: "/backups/data_hotfix"
    retention_days: 30
    
  reporting:
    format: "json"
    output_file: "data_hotfix_report.json"
    
  alerts:
    email_recipients:
      - "data-engineering-team@company.com"
    severity_threshold: "critical"
```

## Kiểm tra chất lượng

### 1. Data analysis:
- Đọc và phân tích dữ liệu bị ảnh hưởng
- Xác định nguyên nhân lỗi chính

### 2. Quality analysis:
- Kiểm tra data quality rules (expectations)
- Xác định bản ghi lỗi

### 3. Recovery handling:
- Có khả năng rollback dữ liệu
- Có quy trình khôi phục dữ liệu từ backup

### 4. Validation:
- Kiểm tra tính toàn vẹn dữ liệu sau khi xử lý
- Có quy trình xác nhận hotfix thành công

## Cảnh báo và xử lý lỗi

### 1. Alerting:
- Cảnh báo khi có lỗi nghiêm trọng
- Có routing alert theo domain/team

### 2. Recovery:
- Có khả năng rollback nếu cần
- Có quy trình khôi phục dữ liệu

### 3. Monitoring:
- Theo dõi số lượng hotfix được áp dụng
- Theo dõi hiệu suất xử lý

## Tối ưu hóa hiệu suất

### 1. Parallel processing:
- Sử dụng multiple threads cho phân tích dữ liệu
- Tối ưu hiệu suất xử lý

### 2. Caching:
- Cache kết quả phân tích dữ liệu
- Không phân tích lại dữ liệu đã xử lý

### 3. Incremental recovery:
- Chỉ xử lý dữ liệu cần thiết
- Tối ưu hiệu suất xử lý

## Kết luận

Quy trình data hotfix giúp tự động nhận diện và xử lý lỗi dữ liệu nghiêm trọng, đảm bảo:
1. Tính nhất quán trong xử lý dữ liệu lỗi
2. Khả năng rollback nếu cần
3. Dễ dàng kiểm tra và bảo trì
4. Tuân thủ các best practices trong data recovery