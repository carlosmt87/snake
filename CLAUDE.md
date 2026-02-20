# CLAUDE.md

## Project
Python data engineering portfolio project — an end-to-end ETL pipeline for retail sales data.
GitHub: https://github.com/carlosmt87/snake (branch: master)

## Common Commands

```bash
# Run the full ETL pipeline (required before opening the notebook)
python3 pipeline.py

# Run tests
python3 -m pytest tests/ -v

# Open the exploration notebook
jupyter notebook notebooks/exploration.ipynb

# Install dependencies (no venv — system Python with override flag)
python3 -m pip install --break-system-packages -r requirements.txt
```

## Project Structure

| File/Dir | Purpose |
|---|---|
| `pipeline.py` | Main entry point — orchestrates all ETL stages |
| `src/extract.py` | Read CSV and JSON files into DataFrames |
| `src/validate.py` | Data quality checks (nulls, duplicates, ranges, dates) |
| `src/transform.py` | Clean, merge, calculate metrics, aggregate |
| `src/load.py` | Write to SQLite and processed CSVs; query helper |
| `notebooks/exploration.ipynb` | Jupyter charts and analysis |
| `tests/test_pipeline.py` | 26 unit tests covering all four modules |
| `data/raw/` | Source files: `sales.csv` (50 rows), `products.json` (7 products) |

## Generated Outputs (gitignored — created by running the pipeline)

- `database/retail.db` — SQLite database with 5 tables: `sales`, `products`, `summary_by_category`, `summary_by_store`, `summary_by_date`
- `data/processed/` — enriched and summary CSVs
- `logs/pipeline.log` — full pipeline run log

## Environment Notes

- Python 3.12 (system), no virtualenv
- pip scripts install to `~/.local/bin` (already on PATH via `~/.zshrc`)
- SSH key configured for GitHub pushes
