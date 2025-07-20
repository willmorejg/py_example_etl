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

"""Setup script for the AlignForge package.

This module defines custom setuptools commands for code formatting, import cleanup,
and other development tasks.
"""

import subprocess

from setuptools import Command, find_packages, setup


class FormatCommand(Command):
    """Format code using Black with line length limit."""

    description = "Format code using Black"
    user_options = []

    def initialize_options(self) -> None:
        """Initialize command options - no options needed."""

    def finalize_options(self) -> None:
        """Finalize command options - no options to finalize."""

    def run(self) -> None:
        """Run the formatting command."""
        subprocess.run(["black", "--verbose", "--line-length", "100", "."], check=True)


class RemoveUnusedImports(Command):
    """Remove unused imports using autoflake."""

    description = "Remove unused imports"
    user_options = []

    def initialize_options(self) -> None:
        """Initialize command options - no options needed."""

    def finalize_options(self) -> None:
        """Finalize command options - no options to finalize."""

    def run(self) -> None:
        """Run the unused imports removal command."""
        subprocess.run(
            [
                "autoflake",
                "--verbose",
                "--recursive",
                "--remove-all-unused-imports",
                "--in-place",
                ".",
            ],
            check=True,
        )


class CorrectImports(Command):
    """Correct imports using isort."""

    description = "Correct imports"
    user_options = []

    def initialize_options(self) -> None:
        """Initialize command options - no options needed."""

    def finalize_options(self) -> None:
        """Finalize command options - no options to finalize."""

    def run(self) -> None:
        """Run the import correction command."""
        subprocess.run(
            ["isort", "--verbose", "--overwrite-in-place", "--recursive", "."],
            check=True,
        )


if __name__ == "__main__":
    setup(
        packages=find_packages(where="src"),
        package_dir={"": "src", "tests": "tests"},
        cmdclass={
            "format": FormatCommand,
            "remove_unused_imports": RemoveUnusedImports,
            "correct_imports": CorrectImports,
        },
    )
