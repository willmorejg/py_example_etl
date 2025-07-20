# PySpark ETL Example

A comprehensive ETL (Extract, Transform, Load) pipeline implementation using PySpark, designed for processing employee data with advanced data transformations and aggregations.

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![PySpark](https://img.shields.io/badge/PySpark-3.0+-orange.svg)](https://spark.apache.org/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-green.svg)](https://opensource.org/licenses/Apache-2.0)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

## 🚀 Features

- **Modern PySpark ETL Pipeline**: Object-oriented design with the `SparkAggregator` class
- **Multiple Data Formats**: Support for CSV, JSON, and Parquet file formats
- **Advanced Data Transformations**:
  - Data cleaning and standardization
  - Salary categorization (Low/Medium/High)
  - Age group classification (Young/Middle/Senior)
  - Currency parsing and normalization
- **Comprehensive Aggregations**: Department summaries and demographic analysis
- **Production-Ready**: Error handling, resource cleanup, and logging
- **Extensive Testing**: 20+ unit tests with 100% coverage of public methods
- **Type Hints**: Full type annotation support for better IDE integration

## 📋 Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Usage](#usage)
- [Configuration](#configuration)
- [Testing](#testing)
- [Documentation](#documentation)
- [Development](#development)
- [API Reference](#api-reference)
- [Contributing](#contributing)
- [License](#license)

## 🛠 Installation

### Prerequisites

- Python 3.12 or higher
- Java 8 or higher (for PySpark)

### Setup

1. **Clone the repository**:

   ```bash
   git clone <repository-url>
   cd py_example_etl
   ```

2. **Create and activate virtual environment**:

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**:

   ```bash
   pip install -e .
   ```

4. **Install development dependencies** (optional):

   ```bash
   pip install -e ".[dev]"
   ```

## 🏃‍♂️ Quick Start

### Basic Usage

```python
from py_spark_example.spark_aggregator import SparkAggregator

# Initialize the ETL pipeline
etl = SparkAggregator()

# Run the complete ETL pipeline
etl.run_etl_pipeline()
```

### Step-by-Step Execution

```python
# Initialize
etl = SparkAggregator()

# Extract data
raw_data = etl.extract_data("data/sample_employee_data.csv", "csv")

# Transform data
transformed_data = etl.transform_data(raw_data)

# Create aggregations
dept_summary, age_summary = etl.create_aggregations(transformed_data)

# Load results
etl.load_data(transformed_data, "output/employees", "parquet")
etl.load_data(dept_summary, "output/department_summary", "parquet")
```

## 📁 Project Structure

```txt
py_example_etl/
├── src/
│   └── py_spark_example/
│       └── spark_aggregator.py       # Main ETL implementation
├── data/
│   └── sample_employee_data.csv      # Sample input data
├── tests/
│   ├── test_spark_aggregator.py      # Main test suite
│   ├── test_spark_aggregator_edge_cases.py  # Edge case tests
│   ├── conftest.py                   # Test configuration
│   └── README.md                     # Test documentation
├── output/                           # Generated output files
├── test_reports/                     # Test coverage reports
├── .vscode/
│   └── tasks.json                    # VS Code tasks
├── etl_config.yaml                   # ETL configuration
├── pyproject.toml                    # Project configuration
└── README.md                         # This file
```

## 🎯 Usage

### Data Pipeline Overview

The ETL pipeline processes employee data through three main phases:

1. **Extract**: Load data from various sources (CSV, JSON, Parquet)
2. **Transform**: Clean, standardize, and categorize data
3. **Load**: Save processed data in multiple formats

### Input Data Format

The pipeline expects employee data with the following columns:

```csv
name,age,email,salary,department,hire_date
John Doe,28,john.doe@company.com,$65000,Engineering,2023-01-15
Jane Smith,35,jane.smith@company.com,$85000,Marketing,2022-03-20
```

### Transformations Applied

- **Name Standardization**: Trim whitespace and convert to uppercase
- **Age Handling**: Convert null values to 0, filter out invalid ages
- **Email Validation**: Remove extra spaces, filter records without "@"
- **Salary Processing**: Remove currency symbols, convert to numeric
- **Categorization**:
  - Salary: Low (<$50K), Medium ($50K-$100K), High (>$100K)
  - Age Groups: Young (<30), Middle (30-49), Senior (50+)

### Output Data

The pipeline generates:

1. **Transformed Employee Data**: Cleaned and categorized records
2. **Department Summary**: Employee count, average salary, total salary by department
3. **Age Group Analysis**: Distribution across age groups and salary categories

## ⚙️ Configuration

### ETL Configuration (`etl_config.yaml`)

```yaml
spark:
  app_name: "Employee_ETL_Pipeline"
  configs:
    "spark.sql.adaptive.enabled": "true"
    "spark.sql.adaptive.coalescePartitions.enabled": "true"

sources:
  employee_data:
    path: "data/sample_employee_data.csv"
    format: "csv"
    options:
      header: "true"
      inferSchema: "true"

transformations:
  filters:
    min_age: 18
    min_salary: 1000
  
  salary_categories:
    low: 50000
    medium: 100000
```

### Spark Configuration

The pipeline automatically configures Spark with optimized settings:

- Adaptive Query Execution enabled
- Partition coalescing for better performance
- Local execution mode for development

## 🧪 Testing

### Running Tests

```bash
# Run all tests
pytest tests/

# Run only unit tests
pytest -m unit tests/

# Run with coverage
pytest --cov=py_spark_example tests/

# Run specific test file
pytest tests/test_spark_aggregator.py

# Generate HTML coverage report
pytest --cov=py_spark_example --cov-report=html tests/
```

### Test Categories

- **Unit Tests** (`@pytest.mark.unit`): Fast, isolated method testing
- **Integration Tests** (`@pytest.mark.integration`): End-to-end pipeline testing
- **Edge Case Tests**: Boundary conditions and error scenarios

### VS Code Integration

Use the integrated VS Code task:

- `Ctrl+Shift+P` → "Tasks: Run Task" → "Run Unit Tests"

### Test Coverage

The test suite provides comprehensive coverage:

- ✅ SparkAggregator initialization and configuration
- ✅ Data extraction from multiple formats
- ✅ All transformation logic and edge cases
- ✅ Aggregation creation and validation
- ✅ Data loading and format handling
- ✅ Error handling and resource cleanup
- ✅ Performance testing with large datasets

## � Documentation

### API Documentation

This project uses [pdoc](https://pdoc.dev/) to automatically generate comprehensive API documentation from docstrings.

#### Generate Documentation

**Using Make (Recommended):**

```bash
# Generate all documentation (HTML + Markdown)
make docs

# Generate only HTML documentation
make docs-html

# Generate only Markdown documentation
make docs-markdown

# Serve documentation locally
make docs-serve

# Clean generated documentation
make docs-clean
```

**Using VS Code Tasks:**

- `Ctrl+Shift+P` → "Tasks: Run Task" → "Generate Documentation"
- `Ctrl+Shift+P` → "Tasks: Run Task" → "Generate Markdown Docs"

**Using Command Line:**

```bash
# HTML documentation
python -m pdoc --html --output-dir docs/api --force py_spark_example

# Markdown documentation
python -m pdoc --output-dir docs/markdown --force py_spark_example
```

#### Generated Documentation

- **HTML Documentation**: `docs/api/py_spark_example/index.html`
- **Markdown Documentation**: `docs/markdown/`

#### Documentation Features

- ✅ **Google-style docstrings** with proper formatting
- ✅ **Type hints** for all parameters and return values
- ✅ **Usage examples** in docstrings
- ✅ **Cross-references** between modules and classes
- ✅ **Source code links** for easy navigation
- ✅ **Automatic generation** from code comments

## �💻 Development

### Code Style

This project uses several tools to maintain code quality:

```bash
# Format code
black src/ tests/

# Sort imports
isort src/ tests/

# Remove unused imports
autoflake --remove-all-unused-imports --recursive src/ tests/

# Lint code
flake8 src/ tests/
```

### Pre-commit Setup

1. Install pre-commit: `pip install pre-commit`
2. Setup hooks: `pre-commit install`
3. Run manually: `pre-commit run --all-files`

### Adding New Features

1. **Write tests first** in `tests/test_spark_aggregator.py`
2. **Implement the feature** in `src/py_spark_example/spark_aggregator.py`
3. **Add type hints** for all new methods
4. **Update documentation** as needed
5. **Run the full test suite** to ensure compatibility

### Performance Considerations

- Use DataFrame operations instead of RDD operations when possible
- Leverage Spark's Catalyst optimizer with proper column selection
- Cache DataFrames that are used multiple times
- Use appropriate partitioning for large datasets

## 📚 API Reference

### SparkAggregator Class

#### Methods

##### `__init__()`

Initialize the SparkAggregator with a configured Spark session.

##### `extract_data(source_path: str, source_type: str = "csv") -> DataFrame`

Extract data from various file formats.

**Parameters:**

- `source_path`: Path to the source data file
- `source_type`: File format ("csv", "json", "parquet")

**Returns:** PySpark DataFrame with loaded data

##### `transform_data(df: DataFrame) -> DataFrame`

Apply comprehensive data transformations and filtering.

**Parameters:**

- `df`: Input DataFrame to transform

**Returns:** Transformed and filtered DataFrame

##### `create_aggregations(df: DataFrame) -> tuple[DataFrame, DataFrame]`

Generate department and demographic summaries.

**Parameters:**

- `df`: Transformed DataFrame

**Returns:** Tuple of (department_summary, age_group_summary)

##### `load_data(df: DataFrame, output_path: str, format_type: str = "parquet", mode: str = "overwrite")`

Save DataFrame to specified format and location.

**Parameters:**

- `df`: DataFrame to save
- `output_path`: Target file path
- `format_type`: Output format ("parquet", "csv", "json")
- `mode`: Write mode ("overwrite", "append")

##### `run_etl_pipeline()`

Execute the complete ETL pipeline from start to finish.

## 🤝 Contributing

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/amazing-feature`
3. **Write tests** for your changes
4. **Implement your feature**
5. **Run the test suite**: `pytest tests/`
6. **Commit your changes**: `git commit -m 'Add amazing feature'`
7. **Push to the branch**: `git push origin feature/amazing-feature`
8. **Open a Pull Request**

### Development Guidelines

- Follow PEP 8 style guidelines
- Add type hints to all new functions
- Write comprehensive tests for new features
- Update documentation for API changes
- Keep commits atomic and well-documented

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

```txt
Copyright 2025 James G Willmore

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

## 📞 Support

- **Documentation**: Check the [tests/README.md](tests/README.md) for detailed testing information
- **Issues**: Please use the GitHub issue tracker for bug reports and feature requests
- **Questions**: For usage questions, consider opening a GitHub discussion

## 🔄 Changelog

### Version 0.1.0

- Initial release with core ETL functionality
- SparkAggregator class implementation
- Comprehensive test suite
- Support for CSV, JSON, and Parquet formats
- Advanced data transformations and aggregations

---

Built with ❤️ using PySpark and Python
