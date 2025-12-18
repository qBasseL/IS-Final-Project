# analysis/stats.py
import pandas as pd
import numpy as np
import time
from analysis.iops_calculator import disk_load

def measure_read_write(filename, block_size=10000):
    """
    Measure average read and write times for CSV in blocks.
    """
    read_times = []
    write_times = []

    for chunk in pd.read_csv(filename, chunksize=block_size):
        # Measure read
        start_read = time.time()
        _ = chunk.sum()  # simulate processing
        end_read = time.time()
        read_times.append(end_read - start_read)

        # Measure write
        start_write = time.time()
        _ = chunk.to_csv("storage/temp_write.csv", index=False)
        end_write = time.time()
        write_times.append(end_write - start_write)

    avg_read = np.mean(read_times)
    avg_write = np.mean(write_times)
    return avg_read, avg_write, len(read_times) + len(write_times)

def calculate_read_write_ratio(filename, block_size=10000):
    """
    Dynamically calculate read/write ratio from CSV content.
    """
    read_ops = 0
    write_ops = 0

    for chunk in pd.read_csv(filename, chunksize=block_size):
        numeric_cols = chunk.select_dtypes(include='number').shape[1]
        total_cols = chunk.shape[1]

        read_ops += numeric_cols * len(chunk)          # numeric cells -> read
        write_ops += (total_cols - numeric_cols) * len(chunk)  # other cells -> write

    total_ops = read_ops + write_ops
    read_ratio = read_ops / total_ops
    write_ratio = write_ops / total_ops
    return read_ratio, write_ratio, total_ops   # <-- return total_ops as well
