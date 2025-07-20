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
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, current_timestamp, desc, regexp_replace
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import trim, upper, when

from py_spark_example.config import ConfigManager


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

    def extract_data(self, source_path: str, source_type: str = "csv") -> DataFrame:
        """
        Extract data from various sources
        """
        print(f"Extracting data from {source_path}")

        if source_type.lower() == "csv":
            df = (
                self.spark.read.option("header", "true")
                .option("inferSchema", "true")
                .csv(source_path)
            )
        elif source_type.lower() == "json":
            df = self.spark.read.json(source_path)
        elif source_type.lower() == "parquet":
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
        self, df: DataFrame, output_path: str, format_type: str = "parquet", mode: str = "overwrite"
    ):
        """
        Load transformed data to target destination
        """
        print(f"Loading data to {output_path} in {format_type} format")

        if format_type.lower() == "parquet":
            df.write.mode(mode).parquet(output_path)
        elif format_type.lower() == "csv":
            df.write.mode(mode).option("header", "true").csv(output_path)
        elif format_type.lower() == "json":
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
            raw_data = self.extract_data(source_path, source_type=source_format)

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
            self.load_data(
                transformed_data,
                transformed_config.get("path", "output/transformed_employees"),
                format_type=transformed_config.get("format", "parquet"),
                mode=transformed_config.get("mode", "overwrite"),
            )

            # Load summary tables
            self.load_data(
                dept_summary,
                dept_summary_config.get("path", "output/department_summary"),
                format_type=dept_summary_config.get("format", "parquet"),
                mode=dept_summary_config.get("mode", "overwrite"),
            )

            self.load_data(
                age_summary,
                age_summary_config.get("path", "output/age_group_summary"),
                format_type=age_summary_config.get("format", "csv"),
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
