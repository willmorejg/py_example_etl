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
from enum import Enum
from typing import Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, current_timestamp, desc, regexp_replace
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import trim, upper, when

from py_spark_example.config import ConfigManager


class SourceType(Enum):
    """Enumeration for supported data source types."""

    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"

    def __str__(self) -> str:
        """Return the string value of the enum."""
        return self.value


class FormatType(Enum):
    """Enumeration for supported data format types."""

    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"

    def __str__(self) -> str:
        """Return the string value of the enum."""
        return self.value


class SparkAggregator:
    """
    A class to handle Spark aggregations with yamlsub configuration management
    """

    def __init__(self, config_path: str = "etl_config.yaml", env_path: str = ".env"):
        """
        Initialize SparkAggregator with configuration management

        Args:
            config_path: Path to the YAML configuration file
            env_path: Path to the .env file
        """
        self.config_manager = ConfigManager(config_path, env_path)
        self.spark = self._create_spark_session()

    def _create_spark_session(self) -> SparkSession:
        """Create and return a Spark session using configuration"""
        spark_config = self.config_manager.get_spark_config()

        app_name = spark_config.get("app_name", "ETL_Pipeline")
        master = spark_config.get("master", "local[*]")
        configs = spark_config.get("configs", {})

        builder = SparkSession.builder.appName(app_name).master(master)

        # Apply additional configurations
        for config_key, config_value in configs.items():
            builder = builder.config(config_key, config_value)

        return builder.getOrCreate()

    def extract_data(
        self, source_path: str, source_type: Union[SourceType, str] = SourceType.CSV
    ) -> DataFrame:
        """
        Extract data from various sources.

        Args:
            source_path: Path to the source data file
            source_type: File format (SourceType enum or string for backward compatibility)

        Returns:
            PySpark DataFrame with loaded data

        Raises:
            ValueError: If source_type is not supported

        Example:
            >>> etl = SparkAggregator()
            >>> df = etl.extract_data("data.csv", SourceType.CSV)
            >>> df = etl.extract_data("data.json", SourceType.JSON)
        """
        # Convert string to enum for backward compatibility
        if isinstance(source_type, str):
            try:
                source_type = SourceType(source_type.lower())
            except ValueError:
                raise ValueError(f"Unsupported source type: {source_type}")

        print(f"Extracting data from {source_path}")

        if source_type == SourceType.CSV:
            df = (
                self.spark.read.option("header", "true")
                .option("inferSchema", "true")
                .csv(source_path)
            )
        elif source_type == SourceType.JSON:
            df = self.spark.read.json(source_path)
        elif source_type == SourceType.PARQUET:
            df = self.spark.read.parquet(source_path)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")

        print(f"Extracted {df.count()} rows")
        return df

    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply various transformations to the data using configuration settings
        """
        print("Starting data transformations...")

        # Get transformation configuration
        transform_config = self.config_manager.get_transformation_config()
        filters = transform_config.get("filters", {})
        salary_categories = transform_config.get("salary_categories", {})

        min_age = filters.get("min_age", 18)
        min_salary = filters.get("min_salary", 1000)
        salary_low_threshold = salary_categories.get("low", 50000)
        salary_medium_threshold = salary_categories.get("medium", 100000)

        # Data cleaning transformations
        cleaned_df = df.select(
            # Clean and standardize name field
            trim(upper(col("name"))).alias("name"),
            # Handle null values in age column
            when(col("age").isNull(), 0).otherwise(col("age")).alias("age"),
            # Clean email field - remove extra spaces and convert to lowercase
            trim(regexp_replace(col("email"), r"\s+", "")).alias("email"),
            # Parse salary field and handle currency symbols
            regexp_replace(col("salary"), r"[$,]", "").cast("double").alias("salary"),
            # Keep other columns as is
            col("department"),
            col("hire_date"),
            # Add processing timestamp
            current_timestamp().alias("processed_at"),
        )

        # Business logic transformations using configuration values
        transformed_df = cleaned_df.withColumn(
            "salary_category",
            when(col("salary") < salary_low_threshold, "Low")
            .when(col("salary") < salary_medium_threshold, "Medium")
            .otherwise("High"),
        ).withColumn(
            "age_group",
            when(col("age") < 30, "Young").when(col("age") < 50, "Middle").otherwise("Senior"),
        )

        # Filter out invalid records using configuration values
        valid_df = transformed_df.filter(
            (col("age") >= min_age) & (col("salary") >= min_salary) & (col("email").contains("@"))
        )

        print(f"Transformation complete. {valid_df.count()} valid records remaining")
        return valid_df

    def create_aggregations(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        Create summary tables and aggregations
        """
        print("Creating aggregations...")

        # Department summary
        dept_summary = (
            df.groupBy("department")
            .agg(
                count("*").alias("employee_count"),
                avg("salary").alias("avg_salary"),
                spark_sum("salary").alias("total_salary"),
            )
            .orderBy(desc("avg_salary"))
        )

        # Age group analysis
        age_group_summary = (
            df.groupBy("age_group", "salary_category")
            .agg(count("*").alias("count"))
            .orderBy("age_group", "salary_category")
        )

        return dept_summary, age_group_summary

    def load_data(
        self,
        df: DataFrame,
        output_path: str,
        format_type: Union[FormatType, str] = FormatType.PARQUET,
        mode: str = "overwrite",
    ):
        """
        Load transformed data to target destination.

        Args:
            df: DataFrame to save
            output_path: Target file path
            format_type: Output format (FormatType enum or string for backward compatibility)
            mode: Write mode ("overwrite", "append", etc.)

        Raises:
            ValueError: If format_type is not supported

        Example:
            >>> etl = SparkAggregator()
            >>> etl.load_data(df, "output/data", FormatType.PARQUET)
            >>> etl.load_data(df, "output/data.csv", FormatType.CSV)
        """
        # Convert string to enum for backward compatibility
        if isinstance(format_type, str):
            try:
                format_type = FormatType(format_type.lower())
            except ValueError:
                raise ValueError(f"Unsupported format type: {format_type}")

        print(f"Loading data to {output_path} in {format_type} format")

        if format_type == FormatType.PARQUET:
            df.write.mode(mode).parquet(output_path)
        elif format_type == FormatType.CSV:
            df.write.mode(mode).option("header", "true").csv(output_path)
        elif format_type == FormatType.JSON:
            df.write.mode(mode).json(output_path)
        else:
            raise ValueError(f"Unsupported format type: {format_type}")

        print(f"Data successfully loaded to {output_path}")

    def run_etl_pipeline(self):
        """
        Main ETL pipeline orchestration using configuration
        """

        try:
            # Get source and target configurations
            source_config = self.config_manager.get_source_config("employee_data")
            transformed_config = self.config_manager.get_target_config("transformed_data")
            dept_summary_config = self.config_manager.get_target_config("department_summary")
            age_summary_config = self.config_manager.get_target_config("age_summary")

            # ETL Pipeline Steps

            # 1. EXTRACT - using configuration
            source_path = source_config.get("path", "data/sample_employee_data.csv")
            source_format = source_config.get("format", "csv")

            # Convert string format to enum
            try:
                source_type_enum = SourceType(source_format.lower())
            except ValueError:
                raise ValueError(f"Unsupported source format in configuration: {source_format}")

            raw_data = self.extract_data(source_path, source_type=source_type_enum)

            # Show sample of raw data
            print("Sample of raw data:")
            raw_data.show(5)
            raw_data.printSchema()

            # 2. TRANSFORM
            transformed_data = self.transform_data(raw_data)

            # Show sample of transformed data
            print("Sample of transformed data:")
            transformed_data.show(5)

            # Create aggregations
            dept_summary, age_summary = self.create_aggregations(transformed_data)

            print("Department Summary:")
            dept_summary.show()

            print("Age Group Summary:")
            age_summary.show()

            # 3. LOAD - using configuration
            # Load main transformed data
            transformed_format = transformed_config.get("format", "parquet")
            try:
                transformed_format_enum = FormatType(transformed_format.lower())
            except ValueError:
                raise ValueError(
                    f"Unsupported format in transformed_data configuration: {transformed_format}"
                )

            self.load_data(
                transformed_data,
                transformed_config.get("path", "output/transformed_employees"),
                format_type=transformed_format_enum,
                mode=transformed_config.get("mode", "overwrite"),
            )

            # Load summary tables
            dept_format = dept_summary_config.get("format", "parquet")
            try:
                dept_format_enum = FormatType(dept_format.lower())
            except ValueError:
                raise ValueError(
                    f"Unsupported format in department_summary configuration: {dept_format}"
                )

            self.load_data(
                dept_summary,
                dept_summary_config.get("path", "output/department_summary"),
                format_type=dept_format_enum,
                mode=dept_summary_config.get("mode", "overwrite"),
            )

            age_format = age_summary_config.get("format", "csv")
            try:
                age_format_enum = FormatType(age_format.lower())
            except ValueError:
                raise ValueError(f"Unsupported format in age_summary configuration: {age_format}")

            self.load_data(
                age_summary,
                age_summary_config.get("path", "output/age_group_summary"),
                format_type=age_format_enum,
                mode=age_summary_config.get("mode", "overwrite"),
            )

            print("ETL Pipeline completed successfully!")

        except Exception as e:
            print(f"Error in ETL pipeline: {str(e)}")
            raise
        finally:
            # Clean up resources
            self.spark.stop()


if __name__ == "__main__":
    etl = SparkAggregator()
    etl.run_etl_pipeline()
