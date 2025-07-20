"""
PySpark ETL Example Package

A comprehensive ETL (Extract, Transform, Load) pipeline implementation using PySpark
with advanced configuration management, data transformations, and aggregations.

This package provides:

- **SparkAggregator**: Main ETL pipeline class with comprehensive data processing
- **ConfigManager**: Configuration management using yamlsub for environment variables
- **Comprehensive Testing**: Full test suite with fixtures and edge case coverage

Example:
    Basic usage of the ETL pipeline:
    
    >>> from py_spark_example.spark_aggregator import SparkAggregator
    >>> etl = SparkAggregator()
    >>> etl.run_etl_pipeline()

Modules:
    spark_aggregator: Main ETL pipeline implementation
    config: Configuration management with yamlsub
"""

__version__ = "0.1.0"
__author__ = "James G Willmore"
__email__ = "willmorejg@gmail.com"
__license__ = "Apache-2.0"

# Import main classes for easier access
from .config import ConfigManager
from .spark_aggregator import SparkAggregator

__all__ = [
    "SparkAggregator",
    "ConfigManager",
]
