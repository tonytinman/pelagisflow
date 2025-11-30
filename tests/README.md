# Pelagisflow Tests

Comprehensive test suite for the Pelagisflow data access control module.

## Structure

```
tests/
├── access/                          # Access module tests
│   ├── __init__.py
│   ├── test_models.py              # Data model tests
│   ├── test_metadata_loader.py     # Metadata loading tests
│   ├── test_delta_generator.py     # Delta generation tests
│   ├── test_uc_inspector.py        # UC inspector tests
│   └── fixtures/                   # Test data
│       └── access/
│           ├── global_roles.yaml
│           └── domains/
│               └── galahad/
│                   ├── domain.roles.yaml
│                   └── domain.mappings.dev.yaml
└── README.md                       # This file
```

## Running Tests

### Prerequisites

Install pytest and dependencies:

```bash
pip install pytest pytest-cov pyyaml
```

### Run All Tests

```bash
pytest
```

### Run Specific Test File

```bash
pytest tests/access/test_models.py
```

### Run Specific Test Class

```bash
pytest tests/access/test_models.py::TestPrivilegeIntent
```

### Run Specific Test

```bash
pytest tests/access/test_models.py::TestPrivilegeIntent::test_equality
```

### Run with Coverage

```bash
pytest --cov=access --cov-report=html
```

This generates a coverage report in `htmlcov/index.html`.

### Run with Verbose Output

```bash
pytest -v
```

### Run Only Fast Tests (Exclude Slow)

```bash
pytest -m "not slow"
```

## Test Categories

Tests are organized by module:

### `test_models.py`

Tests for data models:
- `UCPrivilege` enum
- `PrivilegeIntent` dataclass
- `ActualPrivilege` dataclass
- `PrivilegeDelta` dataclass
- `AccessControlResult` dataclass

**Coverage:**
- Enum values and validation
- String to enum conversion
- Equality and hashing
- SQL generation
- Result properties and calculations

### `test_metadata_loader.py`

Tests for metadata loading:
- Loading global roles
- Loading domain roles and mappings
- Calculating intended privileges
- Scope checking (include/exclude)
- Caching behavior
- Error handling

**Coverage:**
- YAML file loading
- Role inheritance
- AD group mapping
- Table scope filtering
- Cache management

### `test_delta_generator.py`

Tests for delta generation:
- Comparing intents vs actuals
- Generating GRANT deltas
- Generating REVOKE deltas
- No-change detection
- Grouping and summarizing

**Coverage:**
- Differential analysis
- Set operations
- Deduplication
- Summary statistics

### `test_uc_inspector.py`

Tests for UC privilege inspection:
- Querying SHOW GRANTS
- Filtering by principal type
- Filtering by privilege type
- Table existence checking
- Workspace group enumeration

**Coverage:**
- Spark SQL mocking
- Error handling
- Principal filtering
- Privilege parsing

## Test Fixtures

Test fixtures provide sample metadata for testing:

### `global_roles.yaml`

Defines global privilege roles:
- `consumer`: SELECT privilege
- `writer`: SELECT + MODIFY privileges
- `admin`: ALL PRIVILEGES

### `domain.roles.yaml` (galahad)

Defines domain-specific roles:
- `consumer`: Access to all tables
- `consumer.sensitive`: Access to sensitive tables only
- `consumer.general`: Access excluding sensitive tables
- `writer`: Write access to all tables

### `domain.mappings.dev.yaml` (galahad)

Maps roles to AD groups for dev environment:
- `consumer` → ad_grp_galahad_analysts_dev, ad_grp_bi_users_dev
- `consumer.sensitive` → ad_grp_galahad_sensitive_dev
- `consumer.general` → ad_grp_galahad_general_dev
- `writer` → ad_grp_galahad_etl_dev

## Writing New Tests

### Test Structure

```python
import pytest
from access.module import Class

@pytest.fixture
def my_fixture():
    """Create test data."""
    return TestData()

class TestMyClass:
    """Test MyClass."""

    def test_feature(self, my_fixture):
        """Test a specific feature."""
        # Arrange
        obj = Class()

        # Act
        result = obj.method()

        # Assert
        assert result == expected
```

### Best Practices

1. **Use descriptive test names**: `test_get_privileges_with_wildcard_scope`
2. **Follow AAA pattern**: Arrange, Act, Assert
3. **One assertion per test**: Focus on one behavior
4. **Use fixtures**: Share setup code
5. **Mock external dependencies**: Spark, file system, etc.
6. **Test edge cases**: Empty lists, None values, errors
7. **Test error handling**: Exception scenarios
8. **Document test purpose**: Clear docstrings

### Adding Test Fixtures

To add new test data:

1. Create YAML files in `tests/access/fixtures/access/`
2. Follow the schema from existing fixtures
3. Update fixture paths in test files
4. Add new domains under `domains/`

## Continuous Integration

Tests can be integrated into CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Run tests
  run: pytest --cov=access --cov-report=xml

- name: Upload coverage
  uses: codecov/codecov-action@v3
```

## Debugging Tests

### Run with Print Statements

```bash
pytest -s
```

### Run with Python Debugger

```python
def test_feature():
    import pdb; pdb.set_trace()
    # Test code
```

### View Full Traceback

```bash
pytest --tb=long
```

### Run Last Failed Tests

```bash
pytest --lf
```

## Common Issues

### Import Errors

Ensure the project root is in PYTHONPATH:

```bash
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
pytest
```

### Fixture Not Found

Check that fixtures are defined in the same file or `conftest.py`.

### Mock Not Working

Ensure you're mocking the right import path (where it's used, not where it's defined).

## Coverage Goals

Target coverage levels:
- Overall: 90%+
- Core modules (models, delta_generator): 95%+
- I/O modules (metadata_loader, uc_inspector): 85%+

Check coverage:

```bash
pytest --cov=access --cov-report=term-missing
```

## Contributing

When adding new features:
1. Write tests first (TDD)
2. Ensure all tests pass
3. Maintain or improve coverage
4. Update test documentation
