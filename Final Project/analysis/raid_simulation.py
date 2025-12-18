import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor

# ----------------------------
# Helper functions
# ----------------------------
def clear_folder(folder):
    """Delete and recreate a folder"""
    if os.path.exists(folder):
        shutil.rmtree(folder)
    os.makedirs(folder, exist_ok=True)

def write_disk(disk_path, lines):
    """Write lines to disk and measure time"""
    start = time.perf_counter()
    with open(disk_path, "w") as f:
        f.write("\n".join(lines))
    end = time.perf_counter()
    return end - start

def read_disk(disk_path):
    """Read lines from disk and measure time"""
    start = time.perf_counter()
    with open(disk_path, "r") as f:
        _ = f.readlines()
    end = time.perf_counter()
    return end - start

# ----------------------------
# RAID Simulations (threaded)
# ----------------------------
def raid0(filename, num_disks=3, folder='storage/raid0'):
    """RAID 0: striping"""
    clear_folder(folder)
    with open(filename, "r") as f:
        lines = f.readlines()

    disks = [[] for _ in range(num_disks)]
    for i, line in enumerate(lines):
        disks[i % num_disks].append(line.strip())

    # Write in parallel
    with ThreadPoolExecutor(max_workers=num_disks) as executor:
        futures = [executor.submit(write_disk, os.path.join(folder,f"disk{i+1}.txt"), disk)
                   for i,disk in enumerate(disks)]
        write_times = [f.result() for f in futures]

    # Read in parallel
    with ThreadPoolExecutor(max_workers=num_disks) as executor:
        futures = [executor.submit(read_disk, os.path.join(folder,f"disk{i+1}.txt"))
                   for i in range(num_disks)]
        read_times = [f.result() for f in futures]

    avg_read = max(read_times)         # slowest disk determines total read
    avg_write = max(write_times)       # total write = slowest disk
    return avg_read, avg_write

def raid1(filename, num_disks=2, folder='storage/raid1'):
    """RAID 1: mirrored"""
    clear_folder(folder)
    with open(filename, "r") as f:
        lines = f.readlines()

    # Write to all disks in parallel
    with ThreadPoolExecutor(max_workers=num_disks) as executor:
        futures = [executor.submit(write_disk, os.path.join(folder,f"disk{i+1}.txt"), lines)
                   for i in range(num_disks)]
        write_times = [f.result() for f in futures]

    # Read in parallel
    with ThreadPoolExecutor(max_workers=num_disks) as executor:
        futures = [executor.submit(read_disk, os.path.join(folder,f"disk{i+1}.txt"))
                   for i in range(num_disks)]
        read_times = [f.result() for f in futures]

    avg_read = max(read_times)         # can read from fastest disk, but to be safe use max
    avg_write = sum(write_times)       # write to all disks
    return avg_read, avg_write

def raid5(filename, num_disks=3, folder='storage/raid5'):
    """RAID 5: striping + parity"""
    clear_folder(folder)
    with open(filename, "r") as f:
        lines = f.readlines()

    stripe_disks = num_disks - 1
    disks = [[] for _ in range(num_disks)]
    for i, line in enumerate(lines):
        disks[i % stripe_disks].append(line.strip())

    # Last disk = parity
    disks[-1] = ["PARITY" for _ in range(len(disks[0]))]

    # Write in parallel
    with ThreadPoolExecutor(max_workers=num_disks) as executor:
        futures = [executor.submit(write_disk, os.path.join(folder,f"disk{i+1}.txt"), disk)
                   for i,disk in enumerate(disks)]
        write_times = [f.result() for f in futures]

    # Read in parallel
    with ThreadPoolExecutor(max_workers=num_disks) as executor:
        futures = [executor.submit(read_disk, os.path.join(folder,f"disk{i+1}.txt"))
                   for i in range(num_disks)]
        read_times = [f.result() for f in futures]

    avg_read = max(read_times)                  # slowest disk
    avg_write = max(write_times) * 1.2          # extra 20% overhead for parity writes
    return avg_read, avg_write
