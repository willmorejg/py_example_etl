################################################################################
# Copyright 2025 James G Willmore
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
"""
Pytest configuration and shared fixtures
"""
import shutil
import tempfile
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from py_spark_example.spark_aggregator import SparkAggregator


@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing"""
    spark = (
        SparkSession.builder.appName("test")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def sample_data(spark_session):
    """Create sample data for testing"""
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("email", StringType(), True),
            StructField("salary", StringType(), True),
            StructField("department", StringType(), True),
            StructField("hire_date", StringType(), True),
        ]
    )

    data = [
        (
            "John Doe",
            28,
            "john.doe@company.com",
            "$65,000",
            "Engineering",
            "2023-01-15",
        ),
        (
            "Jane Smith",
            35,
            "jane.smith@company.com",
            "$85,000",
            "Marketing",
            "2022-03-20",
        ),
        (
            "Bob Johnson",
            42,
            "bob.johnson@company.com",
            "$95,000",
            "Engineering",
            "2021-07-10",
        ),
        ("Alice Brown", 29, "alice.brown@company.com", "$55,000", "HR", "2023-05-01"),
        (
            "Charlie Wilson",
            38,
            "charlie.wilson@company.com",
            "$75,000",
            "Sales",
            "2022-11-30",
        ),
        ("Invalid User", None, "invalid.email", "$0", "Unknown", "2023-01-01"),
        ("Bad Email", 25, "bad-email", "$50,000", "IT", "2023-02-01"),
    ]

    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def large_dataset(spark_session):
    """Create a large dataset for performance testing"""
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("email", StringType(), True),
            StructField("salary", StringType(), True),
            StructField("department", StringType(), True),
            StructField("hire_date", StringType(), True),
        ]
    )

    # Create 1000 records for performance testing
    data = []
    departments = ["Engineering", "Marketing", "Sales", "HR", "Finance"]
    for i in range(1000):
        data.append(
            (
                f"Employee {i}",
                25 + (i % 40),  # Age between 25-64
                f"employee{i}@company.com",
                f"${50000 + (i % 50000)}",  # Salary between 50k-100k
                departments[i % len(departments)],
                "2023-01-01",
            )
        )

    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing"""
    temp_dir_for_testing = tempfile.mkdtemp()
    yield temp_dir_for_testing
    shutil.rmtree(temp_dir_for_testing)


@pytest.fixture
def mock_aggregator(spark_session):
    """Create a SparkAggregator instance with mocked spark session"""
    with patch.object(SparkAggregator, "_create_spark_session", return_value=spark_session):
        aggregator = SparkAggregator()
        yield aggregator


def pytest_configure(config):
    """Configure custom markers for pytest"""
    config.addinivalue_line("markers", "unit: Unit tests")
    config.addinivalue_line("markers", "integration: Integration tests")
    config.addinivalue_line("markers", "slow: Slow tests that may take longer to run")
