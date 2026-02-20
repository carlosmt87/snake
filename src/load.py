"""
load.py — Load (the "L" in ETL)

Responsible for writing transformed data to its final destinations:
  - SQLite database (tables for querying with SQL)
  - Processed CSV files (for sharing / downstream tools)

Using SQLite means no server setup is needed — the database is just a
single file, which makes this project easy to run anywhere.
"""

import logging
import sqlite3
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def load_to_sqlite(
    df: pd.DataFrame,
    table_name: str,
    db_path: str | Path,
    if_exists: str = "replace",
) -> None:
    """
    Write a DataFrame to a SQLite table.

    Args:
        df:         The DataFrame to write.
        table_name: Name of the target SQLite table.
        db_path:    Path to the SQLite database file (created if absent).
        if_exists:  What to do if the table already exists:
                    'replace' drops and recreates it (default),
                    'append' adds rows to the existing table,
                    'fail' raises an error.
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Loading {len(df)} rows into SQLite table '{table_name}' at {db_path}")

    # pandas handles the SQLite connection via its `to_sql` method.
    # We open the connection explicitly so we can close it cleanly.
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table_name, conn, if_exists=if_exists, index=False)

    logger.info(f"  -> '{table_name}' written successfully")


def load_to_csv(df: pd.DataFrame, filepath: str | Path) -> None:
    """
    Write a DataFrame to a CSV file.

    Args:
        df:       The DataFrame to write.
        filepath: Destination file path (parent directories are created).
    """
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(filepath, index=False)
    logger.info(f"  -> CSV written: {filepath} ({len(df)} rows)")


def query_sqlite(query: str, db_path: str | Path) -> pd.DataFrame:
    """
    Run a SQL SELECT query against a SQLite database and return results.

    This is a utility function that makes it easy to verify what was loaded
    or to run ad-hoc analysis queries from Python.

    Args:
        query:   A SQL SELECT statement.
        db_path: Path to the SQLite database file.

    Returns:
        A DataFrame containing the query results.

    Example:
        >>> df = query_sqlite("SELECT category, SUM(net_revenue) FROM sales GROUP BY category", "database/retail.db")
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(query, conn)

    return df
