import os
import shutil

def clear_folder(folder):
    """Delete and recreate a folder"""
    if os.path.exists(folder):
        shutil.rmtree(folder)
    os.makedirs(folder, exist_ok=True)

def raid0(filename, num_disks=3, folder='storage/raid0'):
    clear_folder(folder)
    with open(filename, "r") as f:
        lines = f.readlines()
    disks = [[] for _ in range(num_disks)]
    for i, line in enumerate(lines):
        disks[i % num_disks].append(line.strip())
    for i, disk in enumerate(disks):
        with open(os.path.join(folder, f"disk{i+1}.txt"), "w") as f:
            f.write("\n".join(disk))

def raid1(filename, num_disks=2, folder='storage/raid1'):
    clear_folder(folder)
    with open(filename, "r") as f:
        lines = f.readlines()
    for i in range(num_disks):
        with open(os.path.join(folder, f"disk{i+1}.txt"), "w") as f:
            f.write("".join(lines))  # mirrored

def raid5(filename, num_disks=3, folder='storage/raid5'):
    clear_folder(folder)
    with open(filename, "r") as f:
        lines = f.readlines()
    disks = [[] for _ in range(num_disks)]
    for i, line in enumerate(lines):
        disks[i % (num_disks - 1)].append(line.strip())
    # simple parity simulation
    disks[-1] = ["PARITY" for _ in range(len(disks[0]))]
    for i, disk in enumerate(disks):
        with open(os.path.join(folder, f"disk{i+1}.txt"), "w") as f:
            f.write("\n".join(disk))


def measure_raid_rw(folder, num_disks):
    """Measure read/write times from RAID files"""
    import time
    # Read simulation
    start_read = time.time()
    for i in range(num_disks):
        with open(os.path.join(folder, f"disk{i+1}.txt"), "r") as f:
            _ = f.readlines()
    end_read = time.time()
    avg_read = (end_read - start_read) / num_disks

    # Write simulation
    start_write = time.time()
    for i in range(num_disks):
        with open(os.path.join(folder, f"disk{i+1}.txt"), "a") as f:
            f.write("")  # simulate write
    end_write = time.time()
    avg_write = (end_write - start_write) / num_disks

    return avg_read, avg_write