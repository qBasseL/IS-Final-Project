# total_ops = measured read + write operations (number of lines or blocks)
# Adjusted for RAID write penalty
def calculate_iops_per_raid(total_ops, read_ratio, write_ratio, write_penalty, avg_read, avg_write):
  
    effective_time_per_op = (avg_read * read_ratio) + (avg_write * write_ratio * write_penalty)
    
    iops = total_ops / effective_time_per_op
    return iops
