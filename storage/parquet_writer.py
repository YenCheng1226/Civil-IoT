def write_parquet(df, output_dir, schema, compression="snappy"):
    df.to_parquet(
        output_dir,
        engine="pyarrow",
        compression=compression,
        write_index=False,
        schema=schema,
    )
