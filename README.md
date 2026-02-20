# Python Data Engineering Portfolio Project

A beginner-friendly, end-to-end ETL (Extract, Transform, Load) pipeline built with Python. This project demonstrates the core activities that data engineers perform every day: reading messy raw data, validating its quality, transforming it into something useful, and storing the results for analysis.

## What This Project Covers

| Topic | Where |
|---|---|
| Reading CSV files | `src/extract.py` |
| Reading JSON files | `src/extract.py` |
| Data validation (nulls, duplicates, ranges) | `src/validate.py` |
| Data cleaning with pandas | `src/transform.py` |
| Joining/merging DataFrames | `src/transform.py` |
| Calculating derived metrics | `src/transform.py` |
| Aggregating data with `groupby` | `src/transform.py` |
| Loading data into SQLite | `src/load.py` |
| Exporting processed CSVs | `src/load.py` |
| Querying a database from Python | `src/load.py` |
| Orchestrating a pipeline | `pipeline.py` |
| Logging pipeline activity | `pipeline.py` |
| Unit testing with pytest | `tests/test_pipeline.py` |

## Scenario

A fictional retail company sells electronics, stationery, and home office products across three stores. Raw sales data arrives as a CSV export from the point-of-sale system, and product information is maintained in a JSON catalogue. The pipeline:

1. **Extracts** both files into pandas DataFrames
2. **Validates** the sales data for common quality issues
3. **Transforms** it by cleaning, joining, and calculating revenue and profit metrics
4. **Loads** the results into a SQLite database and exports processed CSVs

## Project Structure

```
.
├── data/
│   ├── raw/
│   │   ├── sales.csv          # Raw sales transactions (source data)
│   │   └── products.json      # Product catalogue (source data)
│   └── processed/             # Generated: cleaned/enriched CSVs land here
├── database/
│   └── retail.db              # Generated: SQLite database
├── logs/
│   └── pipeline.log           # Generated: full pipeline run log
├── src/
│   ├── extract.py             # Stage 1 – read CSV and JSON files
│   ├── validate.py            # Stage 2 – data quality checks
│   ├── transform.py           # Stage 3 – clean, enrich, aggregate
│   └── load.py                # Stage 4 – write to SQLite and CSV
├── notebooks/
│   └── exploration.ipynb      # Jupyter notebook: charts and analysis
├── tests/
│   └── test_pipeline.py       # Unit tests for every module
├── pipeline.py                # Main entry point — runs the full pipeline
├── requirements.txt
└── README.md
```

## Setup

**Prerequisites:** Python 3.10 or later.

```bash
# 1. Clone the repository
git clone https://github.com/carlosmt87/snake
cd snake

# 2. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate      # macOS / Linux
.venv\Scripts\activate         # Windows

# 3. Install dependencies
pip install -r requirements.txt
```

## Running the Pipeline

```bash
python pipeline.py
```

You should see output like this:

```
2024-01-31 12:00:00  INFO      pipeline — Starting ETL pipeline
2024-01-31 12:00:00  INFO      pipeline — [STAGE 1] EXTRACT
2024-01-31 12:00:00  INFO      src.extract — Extracting CSV: data/raw/sales.csv
2024-01-31 12:00:00  INFO      src.extract —   -> Loaded 51 rows, 8 columns
...
2024-01-31 12:00:00  INFO      pipeline — [STAGE 2] VALIDATE
2024-01-31 12:00:00  WARNING   pipeline — 2 validation check(s) failed: [...]
...
2024-01-31 12:00:00  INFO      pipeline — Pipeline completed successfully!
```

After the run, three outputs are created:

| Output | Path | Description |
|---|---|---|
| SQLite database | `database/retail.db` | Five queryable tables |
| Enriched CSV | `data/processed/sales_enriched.csv` | All sales with product info and metrics |
| Category summary | `data/processed/summary_by_category.csv` | Revenue and profit per category |
| Store summary | `data/processed/summary_by_store.csv` | Revenue and profit per store |
| Daily summary | `data/processed/summary_by_date.csv` | Day-by-day sales totals |

## Running the Tests

```bash
pytest tests/ -v
```

The test suite covers all four modules (extract, validate, transform, load) with focused unit tests. Every test uses either a small inline DataFrame or a temporary file so there are no dependencies on the actual raw data files.

## Querying the Database

After running the pipeline, you can explore the database with any SQLite client or from Python:

```python
from src.load import query_sqlite

# Which category generates the most profit?
df = query_sqlite(
    "SELECT category, ROUND(total_profit, 2) AS profit FROM summary_by_category ORDER BY profit DESC",
    "database/retail.db",
)
print(df)

# What are the top-selling products?
df = query_sqlite(
    """
    SELECT name, SUM(quantity) AS units_sold, ROUND(SUM(net_revenue), 2) AS revenue
    FROM   sales
    GROUP  BY name
    ORDER  BY revenue DESC
    """,
    "database/retail.db",
)
print(df)
```

## Module Descriptions

### `src/extract.py`
Contains two functions: `extract_csv()` and `extract_json()`. Each accepts a file path, validates that the file exists, reads it into a pandas DataFrame, and logs how many rows were loaded. Keeping extraction in its own module means you can easily swap in a different source (a database, an API, an S3 bucket) without touching the rest of the pipeline.

### `src/validate.py`
Data validation is one of the most important (and often skipped) steps in a pipeline. This module provides individual check functions:

- `check_required_columns` — ensures expected columns exist
- `check_no_nulls` — flags missing values in critical columns
- `check_no_duplicates` — catches duplicate primary keys
- `check_numeric_range` — validates numbers are within sensible bounds
- `check_date_format` — confirms dates can be parsed correctly

`run_sales_validation()` runs all checks and logs a pass/fail summary. In a production system, you might halt the pipeline on critical failures or route bad rows to a quarantine table.

### `src/transform.py`
The transformation stage has three layers:

1. **Cleaning** (`clean_sales`, `clean_products`) — removes duplicates, parses types, fills nulls
2. **Enrichment** (`merge_sales_products`) — joins the two datasets on `product_id`
3. **Metrics** (`calculate_metrics`) — derives `gross_revenue`, `net_revenue`, `profit`, and `profit_margin_pct`
4. **Aggregation** (`aggregate_by_category`, `aggregate_by_store`, `aggregate_by_date`) — `groupby` summaries ready to load

### `src/load.py`
Handles writing data to its final destinations. Uses pandas' `to_sql()` to write to SQLite, which requires no database server — just a file. `query_sqlite()` provides a quick way to read data back for verification or ad-hoc analysis.

### `pipeline.py`
The orchestrator. It imports the four modules above and runs them in sequence, handling logging, error messages, and a final summary query. This is the file you'd schedule with cron, Airflow, or another scheduler in a real system.

## Key Concepts Explained

**Why separate modules?**
Each stage (extract, validate, transform, load) is independent. This means you can test each one in isolation, replace one without touching the others, and understand the pipeline by reading one file at a time.

**Why validate before transforming?**
Validation catches problems early. If bad data slips through to the transform stage, the errors can be harder to diagnose because they show up far from the source. Validating first lets you either fix the issue in the source system or handle it explicitly in cleaning.

**Why SQLite?**
SQLite is a file-based database built into Python's standard library. There's no server to install or configure, which keeps this project easy to run on any machine. The same `load_to_sqlite` function would work with PostgreSQL or MySQL by swapping in a different connection string.

**Why use `df.copy()`?**
pandas operations can modify DataFrames in place unexpectedly. Calling `.copy()` at the start of each transform function ensures the input DataFrame is never mutated — the caller's data is always preserved, which prevents subtle bugs.

## Technologies Used

| Library | Version | Purpose |
|---|---|---|
| [pandas](https://pandas.pydata.org/) | 2.2.x | Data manipulation and analysis |
| [matplotlib](https://matplotlib.org/) | 3.9.x | Charts and visualisations |
| [sqlite3](https://docs.python.org/3/library/sqlite3.html) | stdlib | Database storage and querying |
| [pytest](https://pytest.org/) | 8.x | Unit testing |
| [jupyter](https://jupyter.org/) | 1.1.x | Interactive notebook environment |

All dependencies are pure Python — no database server, no Docker, no cloud account required.

## Exploring the Data (Jupyter Notebook)

After running the pipeline, open the notebook to interactively explore the results:

```bash
jupyter notebook notebooks/exploration.ipynb
```

The notebook covers:
- Summary statistics across all 50 transactions
- Revenue and profit by category (bar charts)
- Daily sales trend (line + bar chart)
- Store performance comparison
- Top products by revenue (horizontal bar chart)
- Discount impact scatter plot
- Known vs unknown customer breakdown (pie chart)

## Ideas for Extension

Once you're comfortable with the project, here are some ways to take it further:

- **Add a new data source** — read from a REST API and add a third extract function
- **Schedule the pipeline** — use Python's `schedule` library to run it on a timer
- **Add more validations** — check referential integrity (every `product_id` in sales exists in products)
- **Swap SQLite for PostgreSQL** — use `psycopg2` and change the connection string in `load.py`
- **Extend the notebook** — add moving averages, correlation heatmaps, or interactive plots with Plotly
- **Parametrise with config** — move file paths and settings into a `config.yaml` file read at startup
- **Add CI** — create a GitHub Actions workflow that runs `pytest` on every push
