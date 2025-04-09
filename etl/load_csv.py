import dask.dataframe as dd


def load_csv(file_pattern, dtype, blocksize="256MB"):
    return dd.read_csv(
        file_pattern, dtype=dtype, assume_missing=True, blocksize=blocksize
    )
