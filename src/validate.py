"""
validate.py — Data Validation

Data validation catches quality problems early so bad data never reaches
the database or downstream analysis. Each function returns a dict with
a 'passed' boolean and a 'message' describing what was found.

In production pipelines, validation failures might trigger alerts, stop
the pipeline, or route bad rows to a quarantine table for manual review.
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


def check_required_columns(df: pd.DataFrame, required: list[str]) -> dict:
    """
    Verify that all expected columns are present in the DataFrame.

    Args:
        df: The DataFrame to check.
        required: Column names that must exist.

    Returns:
        A result dict with 'passed' (bool) and 'message' (str).
    """
    missing = [col for col in required if col not in df.columns]
    if missing:
        return {
            "passed": False,
            "message": f"Missing required columns: {missing}",
        }
    return {"passed": True, "message": "All required columns present"}


def check_no_nulls(df: pd.DataFrame, columns: list[str]) -> dict:
    """
    Check that specified columns contain no null/NaN values.

    Args:
        df: The DataFrame to check.
        columns: Columns that must not have nulls.

    Returns:
        A result dict with 'passed', 'message', and 'null_counts'.
    """
    null_counts = {col: int(df[col].isna().sum()) for col in columns if col in df.columns}
    problem_cols = {col: count for col, count in null_counts.items() if count > 0}

    if problem_cols:
        return {
            "passed": False,
            "message": f"Null values found: {problem_cols}",
            "null_counts": null_counts,
        }
    return {
        "passed": True,
        "message": f"No nulls in required columns",
        "null_counts": null_counts,
    }


def check_no_duplicates(df: pd.DataFrame, key_column: str) -> dict:
    """
    Check that a column intended to be a unique key has no duplicate values.

    Args:
        df: The DataFrame to check.
        key_column: The column that should be unique (e.g. transaction_id).

    Returns:
        A result dict with 'passed', 'message', and 'duplicate_values'.
    """
    if key_column not in df.columns:
        return {"passed": False, "message": f"Column '{key_column}' not found"}

    duplicates = df[df.duplicated(subset=[key_column], keep=False)][key_column].unique()
    if len(duplicates) > 0:
        return {
            "passed": False,
            "message": f"Duplicate values in '{key_column}': {list(duplicates)}",
            "duplicate_values": list(duplicates),
        }
    return {
        "passed": True,
        "message": f"No duplicates in '{key_column}'",
        "duplicate_values": [],
    }


def check_numeric_range(
    df: pd.DataFrame, column: str, min_val: float, max_val: float
) -> dict:
    """
    Check that all values in a numeric column fall within [min_val, max_val].

    Args:
        df: The DataFrame to check.
        column: The numeric column to validate.
        min_val: Minimum acceptable value (inclusive).
        max_val: Maximum acceptable value (inclusive).

    Returns:
        A result dict with 'passed', 'message', and 'out_of_range_count'.
    """
    if column not in df.columns:
        return {"passed": False, "message": f"Column '{column}' not found"}

    series = pd.to_numeric(df[column], errors="coerce")
    out_of_range = series[(series < min_val) | (series > max_val)]

    if len(out_of_range) > 0:
        return {
            "passed": False,
            "message": (
                f"'{column}' has {len(out_of_range)} value(s) outside "
                f"[{min_val}, {max_val}]"
            ),
            "out_of_range_count": len(out_of_range),
        }
    return {
        "passed": True,
        "message": f"'{column}' values are within [{min_val}, {max_val}]",
        "out_of_range_count": 0,
    }


def check_date_format(df: pd.DataFrame, column: str, fmt: str = "%Y-%m-%d") -> dict:
    """
    Check that all values in a date column can be parsed with the given format.

    Args:
        df: The DataFrame to check.
        column: The date column to validate.
        fmt: Expected strftime format string.

    Returns:
        A result dict with 'passed', 'message', and 'invalid_count'.
    """
    if column not in df.columns:
        return {"passed": False, "message": f"Column '{column}' not found"}

    parsed = pd.to_datetime(df[column], format=fmt, errors="coerce")
    invalid_count = int(parsed.isna().sum())

    if invalid_count > 0:
        return {
            "passed": False,
            "message": f"'{column}' has {invalid_count} value(s) not matching format '{fmt}'",
            "invalid_count": invalid_count,
        }
    return {
        "passed": True,
        "message": f"All '{column}' values match format '{fmt}'",
        "invalid_count": 0,
    }


def run_sales_validation(df: pd.DataFrame) -> dict[str, dict]:
    """
    Run the full set of validation checks for the raw sales DataFrame.

    Args:
        df: The raw sales DataFrame to validate.

    Returns:
        A dict mapping check name -> result dict. Log a summary of results.
    """
    logger.info("Running sales data validation...")

    results = {
        "required_columns": check_required_columns(
            df,
            ["transaction_id", "date", "product_id", "quantity", "unit_price",
             "store_id", "discount_pct"],
        ),
        "no_duplicate_transactions": check_no_duplicates(df, "transaction_id"),
        "no_null_transaction_ids": check_no_nulls(df, ["transaction_id"]),
        "no_null_product_ids": check_no_nulls(df, ["product_id"]),
        "valid_quantity": check_numeric_range(df, "quantity", 1, 10_000),
        "valid_unit_price": check_numeric_range(df, "unit_price", 0.01, 100_000),
        "valid_discount": check_numeric_range(df, "discount_pct", 0.0, 1.0),
        "valid_date_format": check_date_format(df, "date"),
    }

    passed = sum(1 for r in results.values() if r["passed"])
    total = len(results)
    logger.info(f"  -> Validation complete: {passed}/{total} checks passed")

    for name, result in results.items():
        status = "PASS" if result["passed"] else "FAIL"
        logger.info(f"     [{status}] {name}: {result['message']}")

    return results
