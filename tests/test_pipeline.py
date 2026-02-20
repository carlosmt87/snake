"""
tests/test_pipeline.py

Unit tests for the ETL pipeline modules.

Run with:
    pytest tests/

Each test is intentionally small and focused — it tests one behaviour at a time.
This makes it easy to see exactly what broke when a test fails.
"""

import sqlite3
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from src.extract import extract_csv, extract_json
from src.validate import (
    check_no_duplicates,
    check_no_nulls,
    check_numeric_range,
    check_required_columns,
    check_date_format,
)
from src.transform import (
    aggregate_by_category,
    calculate_metrics,
    clean_products,
    clean_sales,
    merge_sales_products,
)
from src.load import load_to_sqlite, load_to_csv, query_sqlite


# ---------------------------------------------------------------------------
# Fixtures — reusable test data
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_sales_df() -> pd.DataFrame:
    """A small, clean sales DataFrame for use in transform tests."""
    return pd.DataFrame(
        {
            "transaction_id": ["T001", "T002", "T003"],
            "date": ["2024-01-15", "2024-01-16", "2024-01-17"],
            "product_id": ["P001", "P002", "P001"],
            "quantity": [2, 1, 3],
            "unit_price": [29.99, 9.99, 29.99],
            "customer_id": ["C101", "C102", None],
            "store_id": ["S01", "S01", "S02"],
            "discount_pct": [0.0, 0.10, 0.05],
        }
    )


@pytest.fixture
def sample_products_df() -> pd.DataFrame:
    """A small products DataFrame for use in transform tests."""
    return pd.DataFrame(
        {
            "product_id": ["P001", "P002"],
            "name": ["Wireless Mouse", "Notebook (A5)"],
            "category": ["Electronics", "Stationery"],
            "brand": ["TechGear", "WriteWell"],
            "cost_price": [12.50, 3.25],
            "stock_quantity": [200, 500],
        }
    )


# ---------------------------------------------------------------------------
# Extract tests
# ---------------------------------------------------------------------------

class TestExtract:

    def test_extract_csv_reads_file(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a,b,c\n1,2,3\n4,5,6\n")
        df = extract_csv(csv_file)
        assert len(df) == 2
        assert list(df.columns) == ["a", "b", "c"]

    def test_extract_csv_raises_for_missing_file(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            extract_csv(tmp_path / "no_such_file.csv")

    def test_extract_json_reads_list_of_objects(self, tmp_path):
        json_file = tmp_path / "test.json"
        json_file.write_text('[{"id": 1, "val": "x"}, {"id": 2, "val": "y"}]')
        df = extract_json(json_file)
        assert len(df) == 2
        assert "id" in df.columns

    def test_extract_json_raises_for_non_list(self, tmp_path):
        json_file = tmp_path / "bad.json"
        json_file.write_text('{"key": "value"}')
        with pytest.raises(ValueError, match="Expected a JSON array"):
            extract_json(json_file)

    def test_extract_json_raises_for_missing_file(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            extract_json(tmp_path / "missing.json")


# ---------------------------------------------------------------------------
# Validate tests
# ---------------------------------------------------------------------------

class TestValidate:

    def test_required_columns_all_present(self):
        df = pd.DataFrame({"a": [1], "b": [2]})
        result = check_required_columns(df, ["a", "b"])
        assert result["passed"] is True

    def test_required_columns_missing(self):
        df = pd.DataFrame({"a": [1]})
        result = check_required_columns(df, ["a", "b"])
        assert result["passed"] is False
        assert "b" in result["message"]

    def test_no_nulls_passes(self):
        df = pd.DataFrame({"x": [1, 2, 3]})
        result = check_no_nulls(df, ["x"])
        assert result["passed"] is True

    def test_no_nulls_fails_when_nulls_present(self):
        df = pd.DataFrame({"x": [1, None, 3]})
        result = check_no_nulls(df, ["x"])
        assert result["passed"] is False
        assert result["null_counts"]["x"] == 1

    def test_no_duplicates_passes(self):
        df = pd.DataFrame({"id": ["A", "B", "C"]})
        result = check_no_duplicates(df, "id")
        assert result["passed"] is True

    def test_no_duplicates_detects_duplicates(self):
        df = pd.DataFrame({"id": ["A", "B", "A"]})
        result = check_no_duplicates(df, "id")
        assert result["passed"] is False
        assert "A" in result["duplicate_values"]

    def test_numeric_range_passes(self):
        df = pd.DataFrame({"qty": [1, 5, 10]})
        result = check_numeric_range(df, "qty", 0, 100)
        assert result["passed"] is True

    def test_numeric_range_fails_for_out_of_range(self):
        df = pd.DataFrame({"qty": [1, -5, 10]})
        result = check_numeric_range(df, "qty", 0, 100)
        assert result["passed"] is False
        assert result["out_of_range_count"] == 1

    def test_date_format_passes(self):
        df = pd.DataFrame({"date": ["2024-01-01", "2024-12-31"]})
        result = check_date_format(df, "date")
        assert result["passed"] is True

    def test_date_format_fails_for_bad_dates(self):
        df = pd.DataFrame({"date": ["2024-01-01", "not-a-date"]})
        result = check_date_format(df, "date")
        assert result["passed"] is False
        assert result["invalid_count"] == 1


# ---------------------------------------------------------------------------
# Transform tests
# ---------------------------------------------------------------------------

class TestTransform:

    def test_clean_sales_removes_duplicates(self, sample_sales_df):
        # Add a duplicate row
        df_with_dup = pd.concat(
            [sample_sales_df, sample_sales_df.iloc[[0]]], ignore_index=True
        )
        cleaned = clean_sales(df_with_dup)
        assert len(cleaned) == len(sample_sales_df)

    def test_clean_sales_fills_null_customer_ids(self, sample_sales_df):
        cleaned = clean_sales(sample_sales_df)
        assert cleaned["customer_id"].isna().sum() == 0
        assert "UNKNOWN" in cleaned["customer_id"].values

    def test_clean_sales_parses_dates(self, sample_sales_df):
        cleaned = clean_sales(sample_sales_df)
        assert pd.api.types.is_datetime64_any_dtype(cleaned["date"])

    def test_merge_adds_product_columns(self, sample_sales_df, sample_products_df):
        cleaned_sales = clean_sales(sample_sales_df)
        cleaned_products = clean_products(sample_products_df)
        merged = merge_sales_products(cleaned_sales, cleaned_products)
        assert "name" in merged.columns
        assert "category" in merged.columns
        assert "cost_price" in merged.columns

    def test_calculate_metrics_adds_revenue_columns(
        self, sample_sales_df, sample_products_df
    ):
        cleaned_sales = clean_sales(sample_sales_df)
        cleaned_products = clean_products(sample_products_df)
        merged = merge_sales_products(cleaned_sales, cleaned_products)
        result = calculate_metrics(merged)

        for col in ["gross_revenue", "net_revenue", "profit", "profit_margin_pct"]:
            assert col in result.columns

    def test_calculate_metrics_values_are_correct(
        self, sample_sales_df, sample_products_df
    ):
        cleaned_sales = clean_sales(sample_sales_df)
        cleaned_products = clean_products(sample_products_df)
        merged = merge_sales_products(cleaned_sales, cleaned_products)
        result = calculate_metrics(merged)

        # T001: qty=2, price=29.99, discount=0.0, cost=12.50
        # gross = 2 * 29.99 = 59.98, discount = 0, net = 59.98
        # cost_total = 2 * 12.50 = 25.0, profit = 34.98
        row = result[result["transaction_id"] == "T001"].iloc[0]
        assert row["gross_revenue"] == pytest.approx(59.98, abs=0.01)
        assert row["net_revenue"] == pytest.approx(59.98, abs=0.01)
        assert row["profit"] == pytest.approx(34.98, abs=0.01)

    def test_aggregate_by_category_returns_one_row_per_category(
        self, sample_sales_df, sample_products_df
    ):
        cleaned_sales = clean_sales(sample_sales_df)
        cleaned_products = clean_products(sample_products_df)
        merged = merge_sales_products(cleaned_sales, cleaned_products)
        result = calculate_metrics(merged)
        summary = aggregate_by_category(result)

        assert len(summary) == 2  # Electronics and Stationery
        assert set(summary["category"]) == {"Electronics", "Stationery"}


# ---------------------------------------------------------------------------
# Load tests
# ---------------------------------------------------------------------------

class TestLoad:

    def test_load_to_sqlite_creates_table(self, tmp_path):
        df = pd.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        db_path = tmp_path / "test.db"
        load_to_sqlite(df, "my_table", db_path)

        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM my_table")
            count = cursor.fetchone()[0]
        assert count == 2

    def test_load_to_csv_creates_file(self, tmp_path):
        df = pd.DataFrame({"x": [1, 2, 3]})
        out_path = tmp_path / "output.csv"
        load_to_csv(df, out_path)
        assert out_path.exists()
        loaded = pd.read_csv(out_path)
        assert len(loaded) == 3

    def test_query_sqlite_returns_dataframe(self, tmp_path):
        df = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
        db_path = tmp_path / "test.db"
        load_to_sqlite(df, "nums", db_path)

        result = query_sqlite("SELECT * FROM nums WHERE value > 15", db_path)
        assert len(result) == 2

    def test_query_sqlite_raises_for_missing_db(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            query_sqlite("SELECT 1", tmp_path / "ghost.db")
