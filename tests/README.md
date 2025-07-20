# Test Suite for SparkAggregator

This directory contains comprehensive pytest tests for the `SparkAggregator` class.

## Test Structure

- `test_spark_aggregator.py` - Main test suite with unit and integration tests
- `test_spark_aggregator_edge_cases.py` - Edge cases and performance tests
- `conftest.py` - Pytest configuration and shared fixtures
- `__init__.py` - Makes the tests directory a Python package

## Test Categories

### Unit Tests (`@pytest.mark.unit`)
- Test individual methods in isolation
- Fast execution
- Mock external dependencies where appropriate

### Integration Tests (`@pytest.mark.integration`)
- Test complete workflows
- Test interactions between components
- May involve file I/O operations

### Slow Tests (`@pytest.mark.slow`)
- Performance tests with large datasets
- Tests that may take longer to execute

## Running Tests

### Run All Tests
```bash
pytest tests/
```

### Run Only Unit Tests
```bash
pytest -m unit tests/
```

### Run Only Integration Tests
```bash
pytest -m integration tests/
```

### Run Tests Excluding Slow Ones
```bash
pytest -m "not slow" tests/
```

### Run with Coverage Report
```bash
pytest --cov=py_spark_example tests/
```

### Run with HTML Coverage Report
```bash
pytest --cov=py_spark_example --cov-report=html tests/
```

### Run Specific Test File
```bash
pytest tests/test_spark_aggregator.py
```

### Run Specific Test Method
```bash
pytest tests/test_spark_aggregator.py::TestSparkAggregator::test_transform_data
```

### Verbose Output
```bash
pytest -v tests/
```

## Test Coverage

The test suite covers:

- ✅ SparkAggregator initialization
- ✅ Spark session behavior (via initialization and cleanup testing)
- ✅ Data extraction from CSV, JSON, and Parquet formats
- ✅ Data transformation and cleaning logic
- ✅ Salary and age categorization
- ✅ Data filtering and validation
- ✅ Aggregation creation (department and age group summaries)
- ✅ Data loading in multiple formats
- ✅ Complete ETL pipeline execution
- ✅ Error handling and exception management
- ✅ Resource cleanup (Spark session stop)
- ✅ Edge cases (empty data, invalid records, extreme values)
- ✅ Performance testing with large datasets
- ✅ Special character handling
- ✅ Various data format parsing

## Test Data

Tests use a combination of:
- Mocked Spark sessions for fast unit testing
- Generated test data for controlled scenarios
- Real sample data file (`data/sample_employee_data.csv`) for integration testing
- Large generated datasets for performance testing

## Design Notes

- The `_create_spark_session` method is private and not tested directly
- Spark session behavior is tested through initialization and resource cleanup tests
- Tests use mocked Spark sessions to avoid overhead and ensure consistency

## Dependencies

Test dependencies are defined in `pyproject.toml` under `[project.optional-dependencies.dev]`:
- pytest
- pytest-cov
- pytest-html
- pytest-dependency
- pytest-mock

## Fixtures

Key fixtures provided:
- `spark_session` - Session-scoped Spark session for testing
- `sample_data` - Generated DataFrame with employee data
- `large_dataset` - Large DataFrame for performance testing
- `temp_dir` - Temporary directory for file operations
- `mock_aggregator` - SparkAggregator instance with mocked Spark session

## CI/CD Integration

The test suite is configured to work with continuous integration:
- Generates JUnit XML reports for CI systems
- Produces HTML coverage reports
- Supports parallel test execution
- Includes performance benchmarks
