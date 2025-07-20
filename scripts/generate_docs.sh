#!/bin/bash
################################################################################
# Documentation Generation Script
# Generates API documentation using pdoc
################################################################################

set -e  # Exit on any error

echo "🚀 Generating API documentation with pdoc..."

# Create docs directory if it doesn't exist
mkdir -p docs/api

# Generate HTML documentation
echo "📚 Generating HTML documentation..."
pdoc \
    --html \
    --output-dir docs/api \
    --force \
    --config show_source_code=True \
    --config sort_identifiers=True \
    --config include_undocumented=True \
    py_spark_example

# Generate markdown documentation
echo "📝 Generating Markdown documentation..."
mkdir -p docs/markdown
pdoc \
    --output-dir docs/markdown \
    --force \
    py_spark_example

echo "✅ Documentation generated successfully!"
echo "📖 HTML docs: docs/api/py_spark_example/"
echo "📄 Markdown docs: docs/markdown/"

# Open documentation in browser (optional)
if command -v xdg-open >/dev/null 2>&1; then
    echo "🌐 Opening documentation in browser..."
    xdg-open docs/api/py_spark_example/index.html
elif command -v open >/dev/null 2>&1; then
    echo "🌐 Opening documentation in browser..."
    open docs/api/py_spark_example/index.html
fi
