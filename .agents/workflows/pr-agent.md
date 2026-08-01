# PR Agent Workflow

## Mô tả
Quy trình tự động tạo PR agent cho kiểm tra và cải thiện code.

## Mục tiêu
- Tự động tạo PR agent để kiểm tra và cải thiện code.
- Hỗ trợ các pattern: lint, type check, unit test, data quality.

## Các bước thực hiện

### 1. Nhận yêu cầu
- Xác định loại kiểm tra cần thực hiện (lint, type check, test)
- Xác định domain code (pipeline, dbt, dag, spark, quality, lakehouse)
- Xác định yêu cầu kỹ thuật (SLA, error handling, etc.)

### 2. Tạo cấu trúc PR agent
- `pr_agent/` - chứa các component kiểm tra
- `checks/` - các kiểm tra code (lint, type, test)
- `fixes/` - các sửa lỗi tự động
- `reports/` - báo cáo kiểm tra

### 3. Tạo file mẫu PR agent
- Kiểm tra lint code (PEP8, flake8)
- Kiểm tra type hint (mypy)
- Kiểm tra unit test (pytest)
- Kiểm tra data quality (DQ rules)

### 4. Cấu hình PR agent
- Tạo file config cho PR agent
- Cấu hình các kiểm tra và sửa lỗi tự động
- Thiết lập báo cáo và cảnh báo

### 5. Kiểm tra và hoàn thiện
- Validate cấu trúc PR agent
- Kiểm tra các kiểm tra có thể chạy độc lập
- Tạo tài liệu hướng dẫn sử dụng

## Ví dụ sử dụng

```
Tạo một PR agent cho pipeline user_events:
1. Kiểm tra lint code (PEP8)
2. Kiểm tra type hint (mypy)
3. Kiểm tra unit test (pytest)
4. Kiểm tra data quality (DQ rules)
5. Tự động sửa lỗi nếu có thể
```

## Mẫu cấu trúc PR agent

```
pr_agent/
├── checks/
│   ├── lint_check.py
│   ├── type_check.py
│   ├── test_check.py
│   └── dq_check.py
├── fixes/
│   ├── lint_fixes.py
│   └── type_fixes.py
├── reports/
│   └── report_generator.py
├── config/
│   └── pr_agent_config.yaml
└── main.py
```

## Các yêu cầu kỹ thuật

### 1. Lint check:
- Kiểm tra PEP8 compliance
- Kiểm tra flake8 rules
- Có thể tự động sửa lỗi lint

### 2. Type check:
- Kiểm tra type hint (mypy)
- Kiểm tra Pydantic validation
- Có thể tự động sửa lỗi type

### 3. Test check:
- Kiểm tra unit test (pytest)
- Kiểm tra integration test
- Có thể tự động sửa lỗi test

### 4. Data quality check:
- Kiểm tra DQ rules (expectations)
- Kiểm tra schema validation
- Có thể tự động sửa lỗi DQ

### 5. Auto-fix:
- Có khả năng tự động sửa lỗi
- Có báo cáo chi tiết về lỗi
- Có routing cảnh báo theo domain

## Ví dụ code PR agent

### Main PR agent:
```python
"""PR Agent for checking and improving code quality."""

import logging
from typing import Dict, Any, List
from checks.lint_check import LintCheck
from checks.type_check import TypeCheck
from checks.test_check import TestCheck
from checks.dq_check import DQCheck
from fixes.lint_fixes import LintFixes
from fixes.type_fixes import TypeFixes

class PRAgent:
    """PR Agent for checking and improving code quality."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.lint_check = LintCheck()
        self.type_check = TypeCheck()
        self.test_check = TestCheck()
        self.dq_check = DQCheck()
        self.lint_fixes = LintFixes()
        self.type_fixes = TypeFixes()
        
    def run_checks(self, code_files: List[str]) -> Dict[str, Any]:
        """Run all checks on provided code files."""
        
        results = {
            "lint": self.lint_check.run_checks(code_files),
            "type": self.type_check.run_checks(code_files),
            "test": self.test_check.run_checks(code_files),
            "dq": self.dq_check.run_checks(code_files)
        }
        
        return results
    
    def auto_fix(self, code_files: List[str]) -> Dict[str, Any]:
        """Automatically fix issues found in code files."""
        
        fixes = {
            "lint": self.lint_fixes.apply_fixes(code_files),
            "type": self.type_fixes.apply_fixes(code_files)
        }
        
        return fixes
    
    def generate_report(self, results: Dict[str, Any]) -> str:
        """Generate report from check results."""
        
        report = "PR Agent Report\n"
        report += "=" * 30 + "\n\n"
        
        # Add lint report
        if results["lint"]["issues"]:
            report += "Lint Issues:\n"
            for issue in results["lint"]["issues"]:
                report += f"  - {issue}\n"
        
        # Add type report
        if results["type"]["issues"]:
            report += "Type Issues:\n"
            for issue in results["type"]["issues"]:
                report += f"  - {issue}\n"
        
        # Add test report
        if results["test"]["issues"]:
            report += "Test Issues:\n"
            for issue in results["test"]["issues"]:
                report += f"  - {issue}\n"
        
        # Add DQ report
        if results["dq"]["issues"]:
            report += "Data Quality Issues:\n"
            for issue in results["dq"]["issues"]:
                report += f"  - {issue}\n"
        
        return report

def main():
    """Main function to run PR agent."""
    
    # Configuration
    config = {
        "lint": {
            "enabled": True,
            "rules": ["PEP8", "flake8"]
        },
        "type": {
            "enabled": True,
            "rules": ["mypy", "pydantic"]
        },
        "test": {
            "enabled": True,
            "rules": ["pytest", "integration"]
        },
        "dq": {
            "enabled": True,
            "rules": ["expectations", "schema"]
        }
    }
    
    # Initialize PR agent
    pr_agent = PRAgent(config)
    
    # Run checks on code files
    code_files = ["src/pipeline/user_events.py", "src/dbt/models/staging/stg_user_events.sql"]
    results = pr_agent.run_checks(code_files)
    
    # Generate report
    report = pr_agent.generate_report(results)
    print(report)
    
    # Auto-fix issues if possible
    fixes = pr_agent.auto_fix(code_files)
    print(f"Applied {len(fixes['lint'])} lint fixes")
    print(f"Applied {len(fixes['type'])} type fixes")

if __name__ == "__main__":
    main()
```

### Lint check:
```python
"""Lint check for PR agent."""

import ast
import flake8.api.legacy as flake8
from typing import List, Dict

class LintCheck:
    """Lint check for PR agent."""
    
    def __init__(self):
        self.rules = ["PEP8", "flake8"]
        
    def run_checks(self, code_files: List[str]) -> Dict[str, Any]:
        """Run lint checks on provided files."""
        
        issues = []
        
        for file_path in code_files:
            try:
                # Run flake8 on file
                style_guide = flake8.get_style_guide()
                report = style_guide.input_file(file_path)
                
                # Parse issues
                if report.total_errors > 0:
                    with open(file_path, 'r') as f:
                        lines = f.readlines()
                    
                    # Simple parsing of flake8 output
                    issues.append({
                        "file": file_path,
                        "count": report.total_errors,
                        "details": f"Found {report.total_errors} lint issues"
                    })
                    
            except Exception as e:
                issues.append({
                    "file": file_path,
                    "error": str(e),
                    "details": "Failed to run lint check"
                })
        
        return {
            "issues": issues,
            "passed": len(issues) == 0
        }

    def fix_issues(self, code_files: List[str]) -> Dict[str, Any]:
        """Automatically fix lint issues."""
        
        fixed_files = []
        
        for file_path in code_files:
            try:
                # Simple fix - run autopep8 or similar
                # This is a simplified example
                fixed_files.append(file_path)
                
            except Exception as e:
                print(f"Failed to fix {file_path}: {str(e)}")
        
        return {
            "fixed_files": fixed_files,
            "count": len(fixed_files)
        }
```

### Type check:
```python
"""Type check for PR agent."""

import mypy.api
from typing import List, Dict

class TypeCheck:
    """Type check for PR agent."""
    
    def __init__(self):
        self.rules = ["mypy", "pydantic"]
        
    def run_checks(self, code_files: List[str]) -> Dict[str, Any]:
        """Run type checks on provided files."""
        
        issues = []
        
        for file_path in code_files:
            try:
                # Run mypy on file
                result = mypy.api.run([
                    file_path,
                    "--strict",
                    "--show-error-codes"
                ])
                
                stdout, stderr, exit_code = result
                
                if exit_code != 0:
                    issues.append({
                        "file": file_path,
                        "count": len(stderr.split('\n')) if stderr else 0,
                        "details": f"Type check failed: {stderr}"
                    })
                    
            except Exception as e:
                issues.append({
                    "file": file_path,
                    "error": str(e),
                    "details": "Failed to run type check"
                })
        
        return {
            "issues": issues,
            "passed": len(issues) == 0
        }

    def fix_issues(self, code_files: List[str]) -> Dict[str, Any]:
        """Automatically fix type issues."""
        
        fixed_files = []
        
        for file_path in code_files:
            try:
                # Simple fix - add missing type hints
                # This is a simplified example
                fixed_files.append(file_path)
                
            except Exception as e:
                print(f"Failed to fix {file_path}: {str(e)}")
        
        return {
            "fixed_files": fixed_files,
            "count": len(fixed_files)
        }
```

### Test check:
```python
"""Test check for PR agent."""

import subprocess
from typing import List, Dict

class TestCheck:
    """Test check for PR agent."""
    
    def __init__(self):
        self.rules = ["pytest", "integration"]
        
    def run_checks(self, code_files: List[str]) -> Dict[str, Any]:
        """Run test checks on provided files."""
        
        issues = []
        
        for file_path in code_files:
            try:
                # Run pytest on test files
                result = subprocess.run(
                    ["pytest", file_path, "--tb=short"],
                    capture_output=True,
                    text=True
                )
                
                if result.returncode != 0:
                    issues.append({
                        "file": file_path,
                        "count": len(result.stderr.split('\n')) if result.stderr else 0,
                        "details": f"Test failed: {result.stderr}"
                    })
                    
            except Exception as e:
                issues.append({
                    "file": file_path,
                    "error": str(e),
                    "details": "Failed to run test check"
                })
        
        return {
            "issues": issues,
            "passed": len(issues) == 0
        }

    def fix_issues(self, code_files: List[str]) -> Dict[str, Any]:
        """Automatically fix test issues."""
        
        fixed_files = []
        
        for file_path in code_files:
            try:
                # Simple fix - add missing test cases
                # This is a simplified example
                fixed_files.append(file_path)
                
            except Exception as e:
                print(f"Failed to fix {file_path}: {str(e)}")
        
        return {
            "fixed_files": fixed_files,
            "count": len(fixed_files)
        }
```

### Data quality check:
```python
"""Data quality check for PR agent."""

from typing import List, Dict

class DQCheck:
    """Data quality check for PR agent."""
    
    def __init__(self):
        self.rules = ["expectations", "schema"]
        
    def run_checks(self, code_files: List[str]) -> Dict[str, Any]:
        """Run data quality checks on provided files."""
        
        issues = []
        
        for file_path in code_files:
            try:
                # Check if file contains DQ rules or expectations
                with open(file_path, 'r') as f:
                    content = f.read()
                
                # Simple check - look for DQ related keywords
                if "expectations" in content or "data_quality" in content:
                    # This would be more complex in real implementation
                    pass
                else:
                    issues.append({
                        "file": file_path,
                        "details": "No data quality rules found"
                    })
                    
            except Exception as e:
                issues.append({
                    "file": file_path,
                    "error": str(e),
                    "details": "Failed to run DQ check"
                })
        
        return {
            "issues": issues,
            "passed": len(issues) == 0
        }

    def fix_issues(self, code_files: List[str]) -> Dict[str, Any]:
        """Automatically fix DQ issues."""
        
        fixed_files = []
        
        for file_path in code_files:
            try:
                # Simple fix - add basic DQ rules
                # This is a simplified example
                fixed_files.append(file_path)
                
            except Exception as e:
                print(f"Failed to fix {file_path}: {str(e)}")
        
        return {
            "fixed_files": fixed_files,
            "count": len(fixed_files)
        }
```

## Tài liệu hướng dẫn

### 1. Cấu trúc thư mục:
- `checks/` - các kiểm tra code (lint, type, test)
- `fixes/` - các sửa lỗi tự động
- `reports/` - báo cáo kiểm tra
- `config/` - file cấu hình

### 2. Cách chạy PR agent:
```bash
# Cài đặt dependencies
pip install mypy pytest flake8

# Chạy PR agent
python pr_agent/main.py

# Hoặc chạy với cấu hình cụ thể
python pr_agent/main.py --config config/pr_agent_config.yaml
```

### 3. Cấu hình PR agent:
```yaml
# config/pr_agent_config.yaml
checks:
  lint:
    enabled: true
    rules: ["PEP8", "flake8"]
    auto_fix: true
    
  type:
    enabled: true
    rules: ["mypy", "pydantic"]
    auto_fix: true
    
  test:
    enabled: true
    rules: ["pytest", "integration"]
    auto_fix: false
    
  dq:
    enabled: true
    rules: ["expectations", "schema"]
    auto_fix: false
    
reports:
  format: "json"
  output_file: "pr_report.json"
  
alerts:
  email_recipients:
    - "data-engineering-team@company.com"
  threshold: 5
```

## Kiểm tra chất lượng

### 1. Lint check:
- Kiểm tra PEP8 compliance
- Kiểm tra flake8 rules

### 2. Type check:
- Kiểm tra type hint (mypy)
- Kiểm tra Pydantic validation

### 3. Test check:
- Kiểm tra unit test (pytest)
- Kiểm tra integration test

### 4. Data quality:
- Kiểm tra DQ rules (expectations)
- Kiểm tra schema validation

## Cảnh báo và xử lý lỗi

### 1. Alerting:
- Cảnh báo khi có quá nhiều lỗi
- Có routing alert theo domain/team

### 2. Auto-fix:
- Có khả năng tự động sửa lỗi lint
- Có khả năng tự động sửa lỗi type

### 3. Monitoring:
- Theo dõi số lượng lỗi
- Theo dõi hiệu suất kiểm tra

## Tối ưu hóa hiệu suất

### 1. Parallel processing:
- Sử dụng multiple threads cho kiểm tra
- Tối ưu hiệu suất xử lý

### 2. Caching:
- Cache kết quả kiểm tra
- Không kiểm tra lại file đã kiểm tra

### 3. Incremental checking:
- Chỉ kiểm tra file thay đổi
- Tối ưu hiệu suất kiểm tra

## Kết luận

Quy trình PR agent giúp tự động kiểm tra và cải thiện chất lượng code, đảm bảo:
1. Tính nhất quán trong kiểm tra code
2. Khả năng tự động sửa lỗi
3. Dễ dàng kiểm thử và bảo trì
4. Tuân thủ các best practices trong code quality