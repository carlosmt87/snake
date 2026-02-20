"""
extract.py — Extract (the "E" in ETL)

Responsible for reading raw data from source files and returning it as
pandas DataFrames. Keeping extraction separate from transformation makes
the pipeline easier to debug and swap out sources later.
"""

import json
import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)


def extract_csv(filepath: str | Path) -> pd.DataFrame:
    """
    Read a CSV file and return it as a DataFrame.

    Args:
        filepath: Path to the CSV file.

    Returns:
        A pandas DataFrame with the file's contents.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"CSV file not found: {filepath}")

    logger.info(f"Extracting CSV: {filepath}")
    df = pd.read_csv(filepath)
    logger.info(f"  -> Loaded {len(df)} rows, {len(df.columns)} columns")
    return df


def extract_json(filepath: str | Path) -> pd.DataFrame:
    """
    Read a JSON file (array of objects) and return it as a DataFrame.

    The JSON file is expected to contain a list of records, e.g.:
        [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    Args:
        filepath: Path to the JSON file.

    Returns:
        A pandas DataFrame with one row per JSON object.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the JSON is not a list of objects.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"JSON file not found: {filepath}")

    logger.info(f"Extracting JSON: {filepath}")
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError(
            f"Expected a JSON array (list of objects), got {type(data).__name__}"
        )

    df = pd.DataFrame(data)
    logger.info(f"  -> Loaded {len(df)} rows, {len(df.columns)} columns")
    return df
