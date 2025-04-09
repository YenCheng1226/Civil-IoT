import dask.dataframe as dd


def transform_data(df, rename_columns, datetime_column, datetime_format):
    df = df.rename(columns=rename_columns)
    df[datetime_column] = dd.to_datetime(
        df[datetime_column], errors="coerce", format=datetime_format
    )
    return df
