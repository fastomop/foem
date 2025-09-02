# foem
FastOMOP Evaluation and Monitoring

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
- Python packages: `psycopg2` (or `psycopg2-binary`), `python-dotenv`

## Installation
```bash
# (Optional) create & activate a virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
pip install psycopg2-binary python-dotenv
# or, if you have a requirements file:
# pip install -r requirements.txt
```

## Configuration
Create a **.env** file at the project root:
```env
DB_CONNECTION_STRING=postgresql://USER:PASSWORD@HOST:PORT/DBNAME
```

## Usage
Run the predefined SQL tests and write results to `dataset.json`:
```bash
python main.py
```

Extend/customize:
- Add/modify tests in **`validator.py`**
- Add/modify templates in **`template.py`**

## File Structure
- **`main.py`** — Entry point for running tests and writing output
- **`validator.py`** — SQL test logic and database interaction
- **`template.py`** — Query templates for OMOP concepts
- **`config.py`** — Database connection setup
- **`dataset.json`** — Results of executed tests

## License
See **`LICENSE`** for details.