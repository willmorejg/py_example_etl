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
Configuration management using yamlsub for environment variable substitution
"""
from pathlib import Path
from typing import Any

from yamlsub.config import Config


class ConfigManager:
    """
    Configuration manager using yamlsub for environment variable substitution
    """

    def __init__(self, config_path: str = "etl_config.yaml", env_path: str = ".env"):
        """
        Initialize the configuration manager

        Args:
            config_path: Path to the YAML configuration file
            env_path: Path to the .env file

        Raises:
            FileNotFoundError: If config_path or env_path does not exist
        """
        # Check if configuration file exists
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        if not config_file.is_file():
            raise FileNotFoundError(f"Configuration path is not a file: {config_path}")

        # Check if environment file exists (optional but warn if specified and missing)
        env_file = Path(env_path)
        if env_path and not env_file.exists():
            print(
                f"Warning: Environment file not found: {env_path}. Proceeding without environment variables."
            )
        elif env_path and not env_file.is_file():
            print(
                f"Warning: Environment path is not a file: {env_path}. Proceeding without environment variables."
            )

        self.config = Config(env_path=env_path, yaml_path=config_path)

    def get_spark_config(self) -> Any:
        """
        Get Spark-specific configuration

        Returns:
            Dictionary containing Spark configuration
        """
        return self.config.get_config_key("spark")

    def get_source_config(self, source_name: str = "employee_data") -> Any:
        """
        Get source configuration for a specific data source

        Args:
            source_name: Name of the data source

        Returns:
            Dictionary containing source configuration
        """
        sources = self.config.get_config_key("sources")
        if sources is None:
            raise ValueError(f"No sources found in configuration for {source_name}")
        return sources.get(source_name, {})

    def get_target_config(self, target_name: str) -> Any:
        """
        Get target configuration for a specific output target

        Args:
            target_name: Name of the target (e.g., "transformed_data", "department_summary")

        Returns:
            Dictionary containing target configuration
        """
        targets = self.config.get_config_key("targets")
        if targets is None:
            raise ValueError(f"No targets found in configuration for {target_name}")
        return targets.get(target_name, {})

    def get_transformation_config(self) -> Any:
        """
        Get transformation configuration

        Returns:
            Dictionary containing transformation settings
        """
        return self.config.get_config_key("transformations")
