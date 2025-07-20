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
import os
from unittest.mock import patch

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType


class TestSparkAggregator:
    """Test class for SparkAggregator"""

    def test_init(self, mock_aggregator):
        """Test SparkAggregator initialization"""
        assert mock_aggregator.spark is not None
        assert hasattr(mock_aggregator, "spark")

    @pytest.mark.unit
    def test_extract_data_csv_success(self, mock_aggregator, temp_dir):
        """Test successful CSV data extraction"""
        # Create test CSV file
        csv_content = """name,age,email,salary,department,hire_date
John Doe,28,john.doe@company.com,$65000,Engineering,2023-01-15
Jane Smith,35,jane.smith@company.com,$85000,Marketing,2022-03-20"""

        csv_file = os.path.join(temp_dir, "test_data.csv")
        with open(csv_file, "w") as f:
            f.write(csv_content)

        df = mock_aggregator.extract_data(csv_file, "csv")

        assert isinstance(df, DataFrame)
        assert df.count() == 2
        assert set(df.columns) == {"name", "age", "email", "salary", "department", "hire_date"}

    @pytest.mark.unit
    def test_extract_data_unsupported_format(self, mock_aggregator):
        """Test extraction with unsupported format raises ValueError"""
        with pytest.raises(ValueError, match="Unsupported source type: xml"):
            mock_aggregator.extract_data("dummy_path", "xml")

    @pytest.mark.unit
    def test_transform_data(self, mock_aggregator, sample_data):
        """Test data transformation"""
        transformed_df = mock_aggregator.transform_data(sample_data)

        # Check that transformed data has expected columns
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
        assert set(transformed_df.columns) == expected_columns

        # Check data types
        salary_column = next(
            field for field in transformed_df.schema.fields if field.name == "salary"
        )
        assert isinstance(salary_column.dataType, DoubleType)

        # Check that invalid records are filtered out
        valid_count = transformed_df.count()
        assert valid_count < sample_data.count()  # Should be less due to filtering

        # Verify filtering conditions
        collected_data = transformed_df.collect()
        for row in collected_data:
            assert row.age > 0
            assert row.salary > 0
            assert "@" in row.email

    @pytest.mark.unit
    def test_transform_data_name_cleaning(self, mock_aggregator, sample_data):
        """Test name field cleaning in transformation"""
        transformed_df = mock_aggregator.transform_data(sample_data)
        names = [row.name for row in transformed_df.collect()]

        # All names should be uppercase and trimmed
        for name in names:
            assert name == name.upper()
            assert name == name.strip()

    @pytest.mark.unit
    def test_transform_data_salary_categories(self, mock_aggregator, sample_data):
        """Test salary categorization"""
        transformed_df = mock_aggregator.transform_data(sample_data)
        salary_categories = {row.salary_category for row in transformed_df.collect()}

        # Should contain expected categories
        expected_categories = {"Low", "Medium", "High"}
        assert salary_categories.issubset(expected_categories)

    @pytest.mark.unit
    def test_transform_data_age_groups(self, mock_aggregator, sample_data):
        """Test age group categorization"""
        transformed_df = mock_aggregator.transform_data(sample_data)
        age_groups = {row.age_group for row in transformed_df.collect()}

        # Should contain expected age groups
        expected_age_groups = {"Young", "Middle", "Senior"}
        assert age_groups.issubset(expected_age_groups)

    @pytest.mark.unit
    def test_create_aggregations(self, mock_aggregator, sample_data):
        """Test aggregation creation"""
        transformed_data = mock_aggregator.transform_data(sample_data)
        dept_summary, age_summary = mock_aggregator.create_aggregations(transformed_data)

        # Test department summary
        assert isinstance(dept_summary, DataFrame)
        dept_columns = {"department", "employee_count", "avg_salary", "total_salary"}
        assert set(dept_summary.columns) == dept_columns

        # Test age group summary
        assert isinstance(age_summary, DataFrame)
        age_columns = {"age_group", "salary_category", "count"}
        assert set(age_summary.columns) == age_columns

    @pytest.mark.unit
    def test_create_aggregations_department_summary(self, mock_aggregator, sample_data):
        """Test department summary aggregation details"""
        transformed_data = mock_aggregator.transform_data(sample_data)
        dept_summary, _ = mock_aggregator.create_aggregations(transformed_data)

        dept_data = dept_summary.collect()

        # Verify aggregation logic
        for row in dept_data:
            assert row.employee_count > 0
            assert row.avg_salary > 0
            assert row.total_salary > 0

    @pytest.mark.unit
    def test_load_data_parquet(self, mock_aggregator, sample_data, temp_dir):
        """Test loading data in Parquet format"""
        output_path = os.path.join(temp_dir, "test_output_parquet")

        mock_aggregator.load_data(sample_data, output_path, "parquet")

        # Verify file was created
        assert os.path.exists(output_path)

        # Verify we can read the data back
        loaded_df = mock_aggregator.spark.read.parquet(output_path)
        assert loaded_df.count() == sample_data.count()

    @pytest.mark.unit
    def test_load_data_csv(self, mock_aggregator, sample_data, temp_dir):
        """Test loading data in CSV format"""
        output_path = os.path.join(temp_dir, "test_output_csv")

        mock_aggregator.load_data(sample_data, output_path, "csv")

        # Verify file was created
        assert os.path.exists(output_path)

        # Verify we can read the data back
        loaded_df = mock_aggregator.spark.read.option("header", "true").csv(output_path)
        assert loaded_df.count() == sample_data.count()

    @pytest.mark.unit
    def test_load_data_json(self, mock_aggregator, sample_data, temp_dir):
        """Test loading data in JSON format"""
        output_path = os.path.join(temp_dir, "test_output_json")

        mock_aggregator.load_data(sample_data, output_path, "json")

        # Verify file was created
        assert os.path.exists(output_path)

        # Verify we can read the data back
        loaded_df = mock_aggregator.spark.read.json(output_path)
        assert loaded_df.count() == sample_data.count()

    @pytest.mark.unit
    def test_load_data_unsupported_format(self, mock_aggregator, sample_data, temp_dir):
        """Test loading data with unsupported format raises ValueError"""
        output_path = os.path.join(temp_dir, "test_output")

        with pytest.raises(ValueError, match="Unsupported format type: xml"):
            mock_aggregator.load_data(sample_data, output_path, "xml")

    @pytest.mark.integration
    @patch("builtins.print")
    def test_run_etl_pipeline_success(self, mock_print, mock_aggregator, temp_dir):
        """Test complete ETL pipeline execution"""
        # Create test data file
        csv_content = """name,age,email,salary,department,hire_date
John Doe,28,john.doe@company.com,$65000,Engineering,2023-01-15
Jane Smith,35,jane.smith@company.com,$85000,Marketing,2022-03-20
Bob Johnson,42,bob.johnson@company.com,$95000,Engineering,2021-07-10"""

        data_dir = os.path.join(temp_dir, "data")
        os.makedirs(data_dir)
        csv_file = os.path.join(data_dir, "sample_employee_data.csv")
        with open(csv_file, "w") as f:
            f.write(csv_content)

        # Mock the extract_data method to use our test file
        with patch.object(mock_aggregator, "extract_data") as mock_extract:
            mock_extract.return_value = (
                mock_aggregator.spark.read.option("header", "true")
                .option("inferSchema", "true")
                .csv(csv_file)
            )

            # Mock load_data to avoid file system operations
            with patch.object(mock_aggregator, "load_data") as mock_load:
                mock_aggregator.run_etl_pipeline()

                # Verify extract was called
                mock_extract.assert_called_once()

                # Verify load was called 3 times (main data + 2 summaries)
                assert mock_load.call_count == 3

                # Verify success message was printed
                mock_print.assert_any_call("ETL Pipeline completed successfully!")

    @pytest.mark.integration
    def test_run_etl_pipeline_with_exception(self, mock_aggregator):
        """Test ETL pipeline handles exceptions properly"""
        # Mock extract_data to raise an exception
        with patch.object(mock_aggregator, "extract_data", side_effect=Exception("Test error")):
            with patch("builtins.print") as mock_print:
                with pytest.raises(Exception, match="Test error"):
                    mock_aggregator.run_etl_pipeline()

                # Verify error message was printed
                mock_print.assert_any_call("Error in ETL pipeline: Test error")

    @pytest.mark.slow
    def test_spark_session_cleanup(self, mock_aggregator):
        """Test that Spark session is properly cleaned up"""
        original_spark = mock_aggregator.spark

        # Mock stop method to verify it's called
        with patch.object(original_spark, "stop") as mock_stop:
            # Mock extract_data to raise an exception to trigger finally block
            with patch.object(mock_aggregator, "extract_data", side_effect=Exception("Test")):
                with pytest.raises(Exception):
                    mock_aggregator.run_etl_pipeline()

                # Verify stop was called in finally block
                mock_stop.assert_called_once()
