# foem
FastOMOP Evaluation and Monitoring

### ⚠️ Under development
This project is a work in progress.

## Overview
**foem** (FastOMOP Evaluation and Monitoring) is a Python toolkit for evaluating and monitoring OMOP (Observational Medical Outcomes Partnership) databases. It automates SQL test generation, template-based query construction, and result aggregation for clinical data analysis.

## Features
- Automated SQL test generation for OMOP databases
- Template-based query construction for common clinical questions
- PostgreSQL connection management
- Outputs results in JSON format
- Easily extensible with new tests and templates

## Requirements
- Python **3.8+**
- PostgreSQL database
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

## Installation

### Using uv (recommended)
```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Sync dependencies and create virtual environment
uv sync
```

### Using pip
```bash
# Create & activate a virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
pip install -e .
```

## Configuration
Create a **.env** file at the project root:
```env
DB_CONNECTION_STRING=postgresql://USER:PASSWORD@HOST:PORT/DBNAME
```

## Usage

### Running Tests
Run the predefined SQL tests and write results to `dataset.json`:
```bash
# With uv
uv run python main.py

# Or if using pip/venv
python main.py
```

### Exporting to CSV
Convert the JSON results to CSV format using the `csv_export` script:

```bash
# Export using execution_result field (default)
uv run python script/csv_export.py

# Export using expected_output field
uv run python script/csv_export.py --type expected

# Specify custom input/output paths
uv run python script/csv_export.py --input path/to/input.json --output path/to/output.csv
```

**Options:**
- `--type` — Export type: `execution` (uses execution_result) or `expected` (uses expected_output). Default: `execution`
- `--input` — Path to input JSON file. Default: `output/dataset.json`
- `--output` — Path to output CSV file. Default: `output/dataset.csv`

The exported CSV contains three columns: `id`, `input`, and `expected_output`. Single values are extracted directly, while multiple rows/columns are stored as JSON strings.

### Customization
Extend/customize:
- Add/modify tests in **[src/foem/sql_test.py](src/foem/sql_test.py)**
- Add/modify query templates in the **[dataset/](dataset/)** directory

## File Structure
```
foem/
├── src/foem/           # Core package modules
│   ├── __init__.py     # Package initialization
│   ├── config.py       # Database connection setup
│   └── sql_test.py     # SQL test logic and database interaction
├── script/             # Utility scripts
│   ├── csv_export.py           # Export results to CSV
│   ├── langfuse_load_data.py   # Load data to Langfuse
│   └── compare_results_llm.py  # LLM-based result comparison
├── dataset/            # SQL query templates and test data
├── output/             # Generated output files
├── main.py             # Entry point for running tests
└── pyproject.toml      # Project configuration and dependencies
```

## License
See **`LICENSE`** for details.