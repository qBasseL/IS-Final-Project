import pandas as pd


def calculate_read_write_ratio(filename, block_size=10000):
    read_ops = 0
    write_ops = 0

    for chunk in pd.read_csv(filename, chunksize=block_size):
        numeric_cols = chunk.select_dtypes(include="number").shape[1]
        total_cols = chunk.shape[1]

        read_ops += numeric_cols * len(chunk)
        write_ops += (total_cols - numeric_cols) * len(chunk)

    total = read_ops + write_ops
    return read_ops / total, write_ops / total, total
