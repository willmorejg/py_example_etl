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
Edge case and performance tests for SparkAggregator
"""
import os
import tempfile

import pytest
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


class TestSparkAggregatorEdgeCases:
    """Test edge cases and boundary conditions"""

    @pytest.mark.unit
    def test_empty_dataframe_transform(self, mock_aggregator, spark_session):
        """Test transformation with empty DataFrame"""
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

        empty_df = spark_session.createDataFrame([], schema)
        result = mock_aggregator.transform_data(empty_df)

        assert result.count() == 0
        # Should still have the expected columns
        expected_columns = {
            "name",
            "age",
            "email",
            "salary",
            "department",
            "hire_date",
            "processed_at",
            "salary_category",
            "age_group",
        }
        assert set(result.columns) == expected_columns

    @pytest.mark.unit
    def test_all_invalid_records(self, mock_aggregator, spark_session):
        """Test transformation when all records are invalid"""
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

        # All records have invalid data
        data = [
            ("Invalid 1", 0, "no-at-symbol", "$0", "Dept", "2023-01-01"),
            ("Invalid 2", -5, "invalid", "-$1000", "Dept", "2023-01-01"),
            ("Invalid 3", None, "bad.email", "$0", "Dept", "2023-01-01"),
        ]

        df = spark_session.createDataFrame(data, schema)
        result = mock_aggregator.transform_data(df)

        assert result.count() == 0

    @pytest.mark.unit
    def test_extreme_salary_values(self, mock_aggregator, spark_session):
        """Test salary categorization with extreme values"""
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
            ("Low Earner", 30, "low@company.com", "$1", "IT", "2023-01-01"),
            ("High Earner", 30, "high@company.com", "$999999", "IT", "2023-01-01"),
            ("Boundary Low", 30, "bound1@company.com", "$49999", "IT", "2023-01-01"),
            ("Boundary Medium", 30, "bound2@company.com", "$50000", "IT", "2023-01-01"),
            ("Boundary High", 30, "bound3@company.com", "$99999", "IT", "2023-01-01"),
            ("Super High", 30, "bound4@company.com", "$100000", "IT", "2023-01-01"),
        ]

        df = spark_session.createDataFrame(data, schema)
        result = mock_aggregator.transform_data(df)

        collected = result.collect()
        salary_categories = [row.salary_category for row in collected]

        # Verify correct categorization
        assert "Low" in salary_categories
        assert "Medium" in salary_categories
        assert "High" in salary_categories

    @pytest.mark.unit
    def test_extreme_age_values(self, mock_aggregator, spark_session):
        """Test age group categorization with extreme values"""
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
            ("Young Person", 18, "young@company.com", "$50000", "IT", "2023-01-01"),
            ("Boundary Young", 29, "bound1@company.com", "$50000", "IT", "2023-01-01"),
            ("Middle Start", 30, "bound2@company.com", "$50000", "IT", "2023-01-01"),
            ("Boundary Middle", 49, "bound3@company.com", "$50000", "IT", "2023-01-01"),
            ("Senior Start", 50, "bound4@company.com", "$50000", "IT", "2023-01-01"),
            ("Very Senior", 70, "senior@company.com", "$50000", "IT", "2023-01-01"),
        ]

        df = spark_session.createDataFrame(data, schema)
        result = mock_aggregator.transform_data(df)

        collected = result.collect()
        age_groups = [row.age_group for row in collected]

        # Verify correct categorization
        assert "Young" in age_groups
        assert "Middle" in age_groups
        assert "Senior" in age_groups

    @pytest.mark.unit
    def test_special_characters_in_names(self, mock_aggregator, spark_session):
        """Test handling of special characters in names"""
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
            ("  John   Doe  ", 30, "john@company.com", "$50000", "IT", "2023-01-01"),
            ("Mary-Jane O'Connor", 30, "mary@company.com", "$50000", "IT", "2023-01-01"),
            ("José García", 30, "jose@company.com", "$50000", "IT", "2023-01-01"),
        ]

        df = spark_session.createDataFrame(data, schema)
        result = mock_aggregator.transform_data(df)

        collected = result.collect()
        names = [row.name for row in collected]

        # Names should be trimmed and uppercase
        assert "JOHN   DOE" in names  # Multiple spaces between first and last name
        assert "MARY-JANE O'CONNOR" in names
        assert "JOSÉ GARCÍA" in names

    @pytest.mark.unit
    def test_various_salary_formats(self, mock_aggregator, spark_session):
        """Test different salary formats"""
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
            ("Person 1", 30, "p1@company.com", "$50,000", "IT", "2023-01-01"),
            ("Person 2", 30, "p2@company.com", "$50000", "IT", "2023-01-01"),
            ("Person 3", 30, "p3@company.com", "50000", "IT", "2023-01-01"),
            ("Person 4", 30, "p4@company.com", "$50,000.00", "IT", "2023-01-01"),
        ]

        df = spark_session.createDataFrame(data, schema)
        result = mock_aggregator.transform_data(df)

        collected = result.collect()
        salaries = [row.salary for row in collected]

        # All should be converted to the same numeric value
        expected_salary = 50000.0
        for salary in salaries:
            assert abs(salary - expected_salary) < 0.01

    @pytest.mark.slow
    def test_large_dataset_performance(self, mock_aggregator, large_dataset):
        """Test performance with large dataset"""
        import time

        start_time = time.time()
        result = mock_aggregator.transform_data(large_dataset)
        transform_time = time.time() - start_time

        start_time = time.time()
        dept_summary, age_summary = mock_aggregator.create_aggregations(result)
        agg_time = time.time() - start_time

        # Verify results
        assert result.count() == 1000  # All records should be valid
        assert dept_summary.count() == 5  # 5 departments
        assert age_summary.count() > 0  # Should have age group combinations

        # Performance assertions (adjust thresholds as needed)
        assert transform_time < 30.0  # Should complete in under 30 seconds
        assert agg_time < 30.0  # Should complete in under 30 seconds

    @pytest.mark.unit
    def test_file_not_found_error(self, mock_aggregator):
        """Test handling of non-existent file"""
        with pytest.raises(Exception):  # PySpark will raise an AnalysisException
            mock_aggregator.extract_data("/non/existent/path.csv", "csv")

    @pytest.mark.unit
    def test_memory_cleanup_after_operations(self, mock_aggregator, large_dataset):
        """Test that DataFrames are properly handled in memory"""
        # This test ensures we don't have memory leaks
        original_count = large_dataset.count()

        # Perform multiple transformations
        for _ in range(3):
            transformed = mock_aggregator.transform_data(large_dataset)
            dept_summary, age_summary = mock_aggregator.create_aggregations(transformed)

            # Verify operations work correctly
            assert transformed.count() <= original_count
            assert dept_summary.count() > 0
            assert age_summary.count() > 0

            # Force garbage collection of intermediate DataFrames
            del transformed, dept_summary, age_summary

    @pytest.mark.integration
    def test_end_to_end_with_real_file(self, mock_aggregator):
        """Test end-to-end with actual sample data file"""
        # This test uses the real sample data file
        data_path = "data/sample_employee_data.csv"

        # Only run if the file exists
        if not os.path.exists(data_path):
            pytest.skip("Sample data file not found")

        # Extract
        raw_data = mock_aggregator.extract_data(data_path, "csv")
        assert raw_data.count() > 0

        # Transform
        transformed_data = mock_aggregator.transform_data(raw_data)
        assert transformed_data.count() > 0

        # Aggregate
        dept_summary, age_summary = mock_aggregator.create_aggregations(transformed_data)
        assert dept_summary.count() > 0
        assert age_summary.count() > 0

        # Test load with temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = os.path.join(temp_dir, "test_output")
            mock_aggregator.load_data(transformed_data, output_path, "parquet")
            assert os.path.exists(output_path)
