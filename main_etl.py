import os
import pyarrow as pa
import sys
import yaml

# from etl.load_csv import load_csv
# from etl.transform import transform_data
# from storage.parquet_writer import write_parquet
# from storage.duckdb_manager import create_duckdb_view, query_month_data
from fetcher.filefetcher import (
    FileFetcher,
)


# default configuration
default_config = {
    "DOWNLOAD_DIR": "/home/NAS/homes/ycchen-10014/moenv_air/downloads",
    "EXTRACT_DIR": "/home/NAS/homes/ycchen-10014/moenv_air/raw",
    "PARQUET_DIR": "/home/NAS/homes/ycchen-10014/moenv_air/parquet_files",
    "DUCKDB_FILE": "/home/NAS/homes/ycchen-10014/moenv_air/moenv_temperature.duckdb",
    "VIEW_NAME": "moenv",
}


def load_config(config_path: str) -> dict:
    """
    load cofig from .yaml, if not exit, return default_config
    """
    try:
        with open(config_path, "r") as file:
            user_config = yaml.safe_load(file)
            config = {
                **default_config,
                **user_config,
            }  # 先用預設值, 如果使用者有給再覆蓋
            return config
    except FileNotFoundError:
        print(
            f"Configuration file {config_path} not found. Using default configuration.",
            file=sys.stderr,
        )
        return default_config


# def query_data_for_month(duckdb_file, view_name, year, month):
#     """
#     Independent function to query data for a specific month.
#     Includes error handling for robustness.
#     """
#     try:
#         data = query_month_data(
#             duckdb_file=duckdb_file, view_name=view_name, year=year, month=month
#         )
#         return data
#     except Exception as e:
#         print(f"Error querying data for {year}-{month}: {e}", file=sys.stderr)
#         return None


def main():
    # === 1.load configuration ===
    config_path = "civiliot/config/settings.yaml"
    config = load_config(config_path)

    download_dir = config["DOWNLOAD_DIR"]
    extract_dir = config["EXTRACT_DIR"]
    parquet_dir = config["PARQUET_DIR"]
    duckdb_file = config["DUCKDB_FILE"]
    view_name = config["VIEW_NAME"]

    # === 2. ETL pipeline ===
    # first open url where the zip files at
    fetcher = FileFetcher(
        api_url="https://history.colife.org.tw/?r=/getdir",
        base_url="https://history.colife.org.tw",
        headers={  # 設定 headers
            "Accept": "application/json, text/plain, */*",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
            "x-csrf-token": "jHoFaKQ34KBSm9G-J3lixlDnkb-h5ypfaJfbVUugaYM",  # csrf token 每隔一段時間需要更新
            "Cookie": "filegator=qoa7lm9hein5gd4v0obsbeo5mj",
        },
        download_dir=download_dir,
        extract_dir=extract_dir,
    )
    
    zip_files = fetcher.fetch_zip_links()

    if not zip_files:
        print("no zip files found", file=sys.stderr)
        exit()

    # multithreaded download and extract
    fetcher.download_and_extract(zip_files, max_workers=5)
    print("All ZIP files have been downloaded and extracted.")

    # === 3. 繼續資料轉換與查詢 ===
    # csv_data = load_csv(extract_dir)
    # transformed = transform_data(csv_data)
    # write_parquet(transformed, parquet_dir)
    # create_duckdb_view(parquet_dir, duckdb_file, view_name)

    """    # === 4. 查詢特定月份的資料 ===
    year, month = 2024, 4
    result = query_data_for_month(duckdb_file, view_name, year, month)
    if result is not None:
        print(result.head())
    else:
        print("⚠️ No data returned.", file=sys.stderr)
    """


if __name__ == "__main__":
    main()
