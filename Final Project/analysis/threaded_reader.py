import pandas as pd
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed


def process_block(chunk, block_id, out_folder):
    # READ
    start_r = time.perf_counter()
    _ = chunk.sum(numeric_only=True)
    read_time = time.perf_counter() - start_r

    # WRITE
    os.makedirs(out_folder, exist_ok=True)
    start_w = time.perf_counter()
    chunk.to_csv(f"{out_folder}/block_{block_id}.csv", index=False)
    write_time = time.perf_counter() - start_w

    return read_time, write_time


def threaded_csv_rw(filename, block_size, threads, out_folder):
    read_times = []
    write_times = []

    futures = []

    with ThreadPoolExecutor(max_workers=threads) as executor:
        for block_id, chunk in enumerate(
            pd.read_csv(filename, chunksize=block_size)
        ):
            futures.append(
                executor.submit(process_block, chunk, block_id, out_folder)
            )

        for future in as_completed(futures):
            r, w = future.result()
            read_times.append(r)
            write_times.append(w)

    avg_read = sum(read_times) / len(read_times)
    avg_write = sum(write_times) / len(write_times)
    total_ops = len(read_times) + len(write_times)

    return avg_read, avg_write, total_ops
