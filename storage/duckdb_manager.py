import duckdb


def create_duckdb_view(duckdb_file, view_name, parquet_path):
    conn = duckdb.connect(duckdb_file)
    conn.execute(
        f"""
        CREATE OR REPLACE VIEW {view_name} AS 
        SELECT * FROM '{parquet_path}/*.parquet';
    """
    )
    conn.close()


def query_month_data(duckdb_file, view_name, year, month):
    """
    Query data for a specific month from the DuckDB view.
    """
    conn = duckdb.connect(duckdb_file)
    query = f"""
        SELECT *
        FROM {view_name}
        WHERE YEAR(phenomenonTime) = {year} AND MONTH(phenomenonTime) = {month};
    """
    result = conn.execute(query).fetchdf()
    conn.close()
    return result
