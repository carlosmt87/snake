"""
pipeline.py — ETL Pipeline Orchestrator

This is the main entry point. Run it with:

    python pipeline.py

It coordinates all three ETL stages in order:

  Extract   -> read raw CSV and JSON files
  Validate  -> check data quality before transforming
  Transform -> clean, enrich, and aggregate the data
  Load      -> write results to SQLite and processed CSVs

Logging is written to both the console and a log file so you can review
what happened after the run completes.
"""

import logging
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Configure logging before importing project modules so all loggers
# created inside them inherit the same configuration.
# ---------------------------------------------------------------------------
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "pipeline.log", mode="w", encoding="utf-8"),
    ],
)

logger = logging.getLogger("pipeline")

# ---------------------------------------------------------------------------
# Project imports
# ---------------------------------------------------------------------------
from src.extract import extract_csv, extract_json
from src.validate import run_sales_validation
from src.transform import (
    clean_sales,
    clean_products,
    merge_sales_products,
    calculate_metrics,
    aggregate_by_category,
    aggregate_by_store,
    aggregate_by_date,
)
from src.load import load_to_sqlite, load_to_csv, query_sqlite

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
RAW_SALES_CSV = Path("data/raw/sales.csv")
RAW_PRODUCTS_JSON = Path("data/raw/products.json")
PROCESSED_DIR = Path("data/processed")
DB_PATH = Path("database/retail.db")


def run_pipeline() -> None:
    """Execute the full ETL pipeline end-to-end."""
    logger.info("=" * 60)
    logger.info("Starting ETL pipeline")
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # STAGE 1: EXTRACT
    # ------------------------------------------------------------------
    logger.info("\n[STAGE 1] EXTRACT")
    raw_sales = extract_csv(RAW_SALES_CSV)
    raw_products = extract_json(RAW_PRODUCTS_JSON)

    # ------------------------------------------------------------------
    # STAGE 2: VALIDATE
    # ------------------------------------------------------------------
    logger.info("\n[STAGE 2] VALIDATE")
    validation_results = run_sales_validation(raw_sales)

    # Count failures
    failures = [name for name, r in validation_results.items() if not r["passed"]]
    if failures:
        logger.warning(
            f"  -> {len(failures)} validation check(s) failed: {failures}\n"
            "     The pipeline will continue and clean issues in the Transform stage."
        )
    else:
        logger.info("  -> All validation checks passed!")

    # ------------------------------------------------------------------
    # STAGE 3: TRANSFORM
    # ------------------------------------------------------------------
    logger.info("\n[STAGE 3] TRANSFORM")

    # Clean individual datasets
    clean_sales_df = clean_sales(raw_sales)
    clean_products_df = clean_products(raw_products)

    # Merge and enrich
    enriched_df = merge_sales_products(clean_sales_df, clean_products_df)

    # Add calculated columns
    enriched_df = calculate_metrics(enriched_df)

    # Build summary aggregations
    logger.info("Building aggregations...")
    category_summary = aggregate_by_category(enriched_df)
    store_summary = aggregate_by_store(enriched_df)
    daily_summary = aggregate_by_date(enriched_df)

    # ------------------------------------------------------------------
    # STAGE 4: LOAD
    # ------------------------------------------------------------------
    logger.info("\n[STAGE 4] LOAD")

    # Load to SQLite
    logger.info("Writing tables to SQLite database...")
    load_to_sqlite(enriched_df, "sales", DB_PATH)
    load_to_sqlite(clean_products_df, "products", DB_PATH)
    load_to_sqlite(category_summary, "summary_by_category", DB_PATH)
    load_to_sqlite(store_summary, "summary_by_store", DB_PATH)
    load_to_sqlite(daily_summary, "summary_by_date", DB_PATH)

    # Export processed CSVs
    logger.info("Writing processed CSVs...")
    load_to_csv(enriched_df, PROCESSED_DIR / "sales_enriched.csv")
    load_to_csv(category_summary, PROCESSED_DIR / "summary_by_category.csv")
    load_to_csv(store_summary, PROCESSED_DIR / "summary_by_store.csv")
    load_to_csv(daily_summary, PROCESSED_DIR / "summary_by_date.csv")

    # ------------------------------------------------------------------
    # SUMMARY: Sample query to verify the load worked
    # ------------------------------------------------------------------
    logger.info("\n[SUMMARY] Querying loaded data...")

    top_categories = query_sqlite(
        """
        SELECT category,
               total_transactions,
               ROUND(total_net_revenue, 2) AS net_revenue,
               ROUND(total_profit, 2)      AS profit
        FROM   summary_by_category
        ORDER  BY net_revenue DESC
        """,
        DB_PATH,
    )

    logger.info("Revenue by category:\n" + top_categories.to_string(index=False))

    top_products = query_sqlite(
        """
        SELECT name,
               COUNT(*)              AS transactions,
               SUM(quantity)         AS units_sold,
               ROUND(SUM(net_revenue), 2) AS net_revenue
        FROM   sales
        GROUP  BY name
        ORDER  BY net_revenue DESC
        LIMIT  5
        """,
        DB_PATH,
    )

    logger.info("\nTop 5 products by net revenue:\n" + top_products.to_string(index=False))

    logger.info("\n" + "=" * 60)
    logger.info("Pipeline completed successfully!")
    logger.info(f"  Database : {DB_PATH}")
    logger.info(f"  CSVs     : {PROCESSED_DIR}/")
    logger.info(f"  Log file : {LOG_DIR}/pipeline.log")
    logger.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()
