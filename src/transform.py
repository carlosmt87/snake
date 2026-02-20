"""
transform.py — Transform (the "T" in ETL)

This module takes raw, validated DataFrames and produces clean, enriched,
analysis-ready data. Transformations include:

  - Cleaning:     fixing types, removing duplicates, filling nulls
  - Enriching:    joining sales with product info
  - Calculating:  deriving new columns (revenue, profit, margin)
  - Aggregating:  summarising by category, store, and date
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cleaning
# ---------------------------------------------------------------------------

def clean_sales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply cleaning rules to the raw sales DataFrame.

    Steps:
      1. Drop duplicate rows (same transaction_id).
      2. Parse the 'date' column to datetime.
      3. Fill missing customer_id with the placeholder 'UNKNOWN'.
      4. Cast quantity to int and numeric price/discount columns to float.

    Args:
        df: Raw sales DataFrame straight from extract_csv().

    Returns:
        A cleaned copy of the DataFrame.
    """
    logger.info("Cleaning sales data...")
    original_len = len(df)

    # Work on a copy so we never mutate the caller's DataFrame
    df = df.copy()

    # 1. Remove duplicate transactions — keep the first occurrence
    df = df.drop_duplicates(subset=["transaction_id"], keep="first")
    removed = original_len - len(df)
    if removed:
        logger.info(f"  -> Removed {removed} duplicate row(s)")

    # 2. Parse dates
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")

    # 3. Fill missing customer IDs
    missing_customers = df["customer_id"].isna().sum()
    if missing_customers:
        logger.info(f"  -> Filling {missing_customers} missing customer_id(s) with 'UNKNOWN'")
    df["customer_id"] = df["customer_id"].fillna("UNKNOWN")

    # 4. Ensure correct numeric types
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").astype("Int64")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce").fillna(0.0)

    logger.info(f"  -> Cleaned sales: {len(df)} rows remaining")
    return df


def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply cleaning rules to the raw products DataFrame.

    Steps:
      1. Strip whitespace from string columns.
      2. Ensure cost_price is a positive float.

    Args:
        df: Raw products DataFrame straight from extract_json().

    Returns:
        A cleaned copy of the DataFrame.
    """
    logger.info("Cleaning products data...")
    df = df.copy()

    # Strip leading/trailing whitespace from all string columns
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())

    # Ensure cost_price is numeric
    df["cost_price"] = pd.to_numeric(df["cost_price"], errors="coerce")

    logger.info(f"  -> Cleaned products: {len(df)} rows")
    return df


# ---------------------------------------------------------------------------
# Enrichment
# ---------------------------------------------------------------------------

def merge_sales_products(
    sales_df: pd.DataFrame, products_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Enrich sales data by joining with the product catalogue.

    Uses a LEFT JOIN on product_id so every sale is kept even if the
    product is missing from the catalogue (those will have NaN in the
    product columns and are worth investigating).

    Args:
        sales_df:    Cleaned sales DataFrame.
        products_df: Cleaned products DataFrame.

    Returns:
        Merged DataFrame with product details alongside each sale.
    """
    logger.info("Merging sales with product catalogue...")

    merged = sales_df.merge(
        products_df[["product_id", "name", "category", "brand", "cost_price"]],
        on="product_id",
        how="left",
    )

    # Warn if any sales have no matching product
    unmatched = merged["name"].isna().sum()
    if unmatched:
        logger.warning(f"  -> {unmatched} sale(s) could not be matched to a product")

    logger.info(f"  -> Merged DataFrame: {len(merged)} rows, {len(merged.columns)} columns")
    return merged


# ---------------------------------------------------------------------------
# Calculated columns
# ---------------------------------------------------------------------------

def calculate_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add derived business metric columns to the merged sales DataFrame.

    New columns:
      - gross_revenue:  quantity × unit_price (before discount)
      - discount_amount: gross_revenue × discount_pct
      - net_revenue:    gross_revenue − discount_amount
      - cost_total:     quantity × cost_price
      - profit:         net_revenue − cost_total
      - profit_margin:  profit / net_revenue (as a percentage, rounded to 2 dp)

    Args:
        df: Merged sales+product DataFrame.

    Returns:
        DataFrame with the new metric columns appended.
    """
    logger.info("Calculating business metrics...")
    df = df.copy()

    df["gross_revenue"] = df["quantity"] * df["unit_price"]
    df["discount_amount"] = df["gross_revenue"] * df["discount_pct"]
    df["net_revenue"] = df["gross_revenue"] - df["discount_amount"]
    df["cost_total"] = df["quantity"] * df["cost_price"]
    df["profit"] = df["net_revenue"] - df["cost_total"]

    # Avoid division by zero (guard against zero net_revenue rows)
    df["profit_margin_pct"] = (
        (df["profit"] / df["net_revenue"].replace(0, pd.NA)) * 100
    ).round(2)

    logger.info("  -> Metrics calculated: gross_revenue, net_revenue, profit, profit_margin_pct")
    return df


# ---------------------------------------------------------------------------
# Aggregations
# ---------------------------------------------------------------------------

def aggregate_by_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Summarise sales metrics grouped by product category.

    Args:
        df: Enriched + metrics DataFrame.

    Returns:
        A summary DataFrame with one row per category.
    """
    summary = (
        df.groupby("category", as_index=False)
        .agg(
            total_transactions=("transaction_id", "count"),
            total_units_sold=("quantity", "sum"),
            total_gross_revenue=("gross_revenue", "sum"),
            total_net_revenue=("net_revenue", "sum"),
            total_profit=("profit", "sum"),
        )
        .round(2)
        .sort_values("total_net_revenue", ascending=False)
        .reset_index(drop=True)
    )
    logger.info(f"  -> Category summary: {len(summary)} categories")
    return summary


def aggregate_by_store(df: pd.DataFrame) -> pd.DataFrame:
    """
    Summarise sales metrics grouped by store.

    Args:
        df: Enriched + metrics DataFrame.

    Returns:
        A summary DataFrame with one row per store.
    """
    summary = (
        df.groupby("store_id", as_index=False)
        .agg(
            total_transactions=("transaction_id", "count"),
            total_units_sold=("quantity", "sum"),
            total_net_revenue=("net_revenue", "sum"),
            total_profit=("profit", "sum"),
            avg_profit_margin_pct=("profit_margin_pct", "mean"),
        )
        .round(2)
        .sort_values("total_net_revenue", ascending=False)
        .reset_index(drop=True)
    )
    logger.info(f"  -> Store summary: {len(summary)} stores")
    return summary


def aggregate_by_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Summarise daily sales totals — useful for spotting trends over time.

    Args:
        df: Enriched + metrics DataFrame.

    Returns:
        A summary DataFrame with one row per day, sorted chronologically.
    """
    summary = (
        df.groupby("date", as_index=False)
        .agg(
            total_transactions=("transaction_id", "count"),
            total_units_sold=("quantity", "sum"),
            total_net_revenue=("net_revenue", "sum"),
            total_profit=("profit", "sum"),
        )
        .round(2)
        .sort_values("date")
        .reset_index(drop=True)
    )
    logger.info(f"  -> Daily summary: {len(summary)} days")
    return summary
