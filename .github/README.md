# GitHub Workflows

This directory contains GitHub Actions workflows for the PySpark ETL Example project.

## Documentation Workflow (`docs.yml`)

Automatically generates and deploys API documentation using pdoc to GitHub Pages.

### Triggers

- Push to `main` branch
- Pull requests to `main` branch  
- Manual workflow dispatch

### What it does

1. **Builds Documentation**: Uses pdoc to generate HTML documentation from Python docstrings
2. **Creates Landing Page**: Generates a user-friendly index.html that redirects to the API docs
3. **Deploys to GitHub Pages**: Automatically publishes documentation to `https://<username>.github.io/<repository>/`

### Setup Requirements

To enable GitHub Pages deployment:

1. Go to your repository settings
2. Navigate to "Pages" section
3. Under "Source", select "GitHub Actions"
4. The workflow will automatically deploy documentation on the next push to main

### Documentation URL

After setup, your documentation will be available at:

```text
https://<username>.github.io/<repository>/
```

### Local Testing

To test documentation generation locally:

```bash
# Install development dependencies
pip install -e ".[dev]"

# Generate documentation
python -m pdoc --output-dir docs/api --docformat markdown --include-undocumented --show-source src/py_spark_example
```
