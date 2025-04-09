import sys
import os
import pytest

# Add the 'urbaniot' directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage.duckdb_manager import create_duckdb_view
from civiliot.main_etl import query_data_for_month
from etl.load_csv import load_csv
from etl.transform import transform_data
from storage.parquet_writer import write_parquet

# Configuration for testing
DUCKDB_FILE = "/home/NAS/homes/ycchen-10014/moenv_air/moenv_temperature.duckdb"
VIEW_NAME = "moenv"
PARQUET_DIR = "/home/NAS/homes/ycchen-10014/urbaniot/parquet_files"
CSV_DIR = (
    "/home/NAS/homes/ycchen-10014/urbaniot/csv_files"  # Update with the correct path
)
CSV_PATTERN = f"{CSV_DIR}/*.csv"  # Pattern to match CSV files


def ensure_parquet_files():
    """
    Ensure Parquet files are generated from CSV files.
    """
    if not os.path.exists(PARQUET_DIR) or not os.listdir(PARQUET_DIR):
        print("Generating Parquet files from CSV...")
        os.makedirs(PARQUET_DIR, exist_ok=True)
        # Load CSV files
        df = load_csv(
            CSV_PATTERN, dtype={"column1": "str", "column2": "float"}
        )  # Update dtypes as needed
        # Transform data
        df = transform_data(
            df,
            rename_columns={
                "old_column_name": "new_column_name"
            },  # Update column mappings
            datetime_column="phenomenonTime",
            datetime_format="%Y-%m-%d %H:%M:%S",
        )
        # Write Parquet files
        write_parquet(df, PARQUET_DIR, schema=None)


def ensure_duckdb_view():
    """
    Ensure the DuckDB view is created.
    """
    ensure_parquet_files()
    create_duckdb_view(
        duckdb_file=DUCKDB_FILE, view_name=VIEW_NAME, parquet_path=PARQUET_DIR
    )


@pytest.fixture(scope="module", autouse=True)
def setup_duckdb_view():
    """
    Pytest fixture to ensure the DuckDB view is created before tests.
    """
    ensure_duckdb_view()


@pytest.mark.parametrize(
    "year, month",
    [
        (2024, 4),  # Test for April 2024
    ],
)
def test_query_data_for_month(year, month):
    """
    Test the query_data_for_month function for specific year and month.
    """
    data = query_data_for_month(
        duckdb_file=DUCKDB_FILE, view_name=VIEW_NAME, year=year, month=month
    )
    assert not data.empty, f"Query returned no data for {year}-{month}"
    assert "phenomenonTime" in data.columns, "Missing 'phenomenonTime' column in result"

    # Print the first 10 rows of the result
    print(f"First 10 rows for {year}-{month}:")
    print(data.head(10))


if __name__ == "__main__":
    # Ensure the view is created when running the script directly
    ensure_duckdb_view()

    # Run the test function directly
    year, month = 2024, 4
    data = query_data_for_month(
        duckdb_file=DUCKDB_FILE, view_name=VIEW_NAME, year=year, month=month
    )
    print(f"First 10 rows for {year}-{month}:")
    print(data.head(10))
