# Makefile for PySpark ETL Project
# Documentation and development tasks

.PHONY: docs docs-html docs-markdown docs-clean docs-serve install test lint format help

# Documentation targets
docs: docs-html docs-markdown  ## Generate all documentation
	@echo "✅ All documentation generated successfully!"

docs-html:  ## Generate HTML documentation with pdoc
	@echo "📚 Generating HTML documentation..."
	@mkdir -p docs/api
	@python -m pdoc --output-dir docs/api py_spark_example
	@echo "📖 HTML docs generated in docs/api/"

docs-markdown:  ## Generate Markdown documentation with pdoc
	@echo "📝 Generating Markdown documentation..."
	@mkdir -p docs/markdown
	@python -m pdoc --output-dir docs/markdown --docformat markdown py_spark_example
	@echo "📄 Markdown docs generated in docs/markdown/"

docs-clean:  ## Clean generated documentation
	@echo "🧹 Cleaning documentation..."
	@rm -rf docs/api docs/markdown
	@echo "✅ Documentation cleaned!"

docs-serve:  ## Serve documentation locally (requires python http.server)
	@echo "🌐 Serving documentation at http://localhost:8000"
	@cd docs/api && python -m http.server 8000

# Development targets
install:  ## Install dependencies
	@echo "📦 Installing dependencies..."
	@pip install -e ".[dev]"

test:  ## Run tests
	@echo "🧪 Running tests..."
	@python -m pytest tests/ -v

test-unit:  ## Run unit tests only
	@echo "🧪 Running unit tests..."
	@python -m pytest -m unit tests/ -v

lint:  ## Run linting
	@echo "🔍 Running linting..."
	@flake8 src/ tests/

format:  ## Format code
	@echo "🎨 Formatting code..."
	@black src/ tests/
	@isort src/ tests/
	@autoflake --remove-all-unused-imports --recursive --in-place src/ tests/

# Help target
help:  ## Show this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'
