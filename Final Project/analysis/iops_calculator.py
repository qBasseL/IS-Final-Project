# analysis/iops_calculator.py

def disk_load(total_iops, read_ratio, write_ratio, write_penalty):
    """
    Compute total IOPS with dynamic read/write ratio and RAID write penalty.
    """
    return (total_iops * read_ratio) + (total_iops * write_ratio * write_penalty)
