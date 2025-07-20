# PySpark ETL Example - API Documentation

This directory contains automatically generated API documentation for the PySpark ETL Example project using pdoc.

## 📚 Documentation Access

### GitHub Pages (Recommended)

- **Live Documentation**: <https://willmorejg.github.io/py_example_etl/>
- **Auto-Updated**: Documentation is automatically rebuilt and deployed on every push to main
- **Always Current**: Reflects the latest code changes

### Direct GitHub Access

- **View in GitHub**: Browse the documentation files directly in this repository
- **Main Entry**: [index.html](./index.html) - Landing page with modern UI
- **API Reference**: [api/py_spark_example.html](./api/py_spark_example.html) - Complete API docs

## 🔧 GitHub Pages Setup

To enable automatic deployment to GitHub Pages:

1. Go to [Repository Settings → Pages](https://github.com/willmorejg/py_example_etl/settings/pages)
2. Under "Source", select "GitHub Actions"  
3. The next push to main will automatically deploy the documentation

## 📁 Local Documentation Generation

### Command Line

Generate HTML documentation:

```bash
python -m pdoc --html --output-dir docs/api --force py_spark_example
```

Generate Markdown documentation:

```bash
python -m pdoc --output-dir docs/markdown --force py_spark_example
```

### VS Code Tasks

Use the integrated VS Code tasks:

- `Ctrl+Shift+P` → "Tasks: Run Task" → "Generate Documentation"
- `Ctrl+Shift+P` → "Tasks: Run Task" → "Generate Markdown Docs"

### Script

Run the documentation generation script:

```bash
./scripts/generate_docs.sh
```

## 📂 Output Locations

- **HTML docs**: `docs/api/py_spark_example/`
- **Markdown docs**: `docs/markdown/`
- **Landing page**: `docs/index.html`

## 🔄 Automatic Updates

The documentation is automatically updated via GitHub Actions workflow when:

- Code is pushed to the main branch
- A pull request is merged to main  
- The workflow is manually triggered

---

*📝 This documentation is automatically generated from Python docstrings using [pdoc](https://pdoc.dev/).*
