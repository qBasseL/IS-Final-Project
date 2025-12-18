import gradio as gr
import pandas as pd

from analysis.raid_simulation import raid0, raid1, raid5
from analysis.stats import calculate_read_write_ratio
from analysis.iops_calculator import calculate_iops_per_raid

# ------------------------
# RAID configuration
# ------------------------
RAID_WRITE_PENALTY = {
    "RAID 0": 1,
    "RAID 1": 2,
    "RAID 5": 4
}

RAID_FOLDERS = {
    "RAID 0": "storage/raid0",
    "RAID 1": "storage/raid1",
    "RAID 5": "storage/raid5"
}

# ------------------------
# Main simulation function
# ------------------------
def run_simulation(file, raid_types):
    filename = file.name

    # Calculate read/write ratio and total operations
    read_ratio, write_ratio, total_ops = calculate_read_write_ratio(filename)

    results = []

    for raid in raid_types:
        folder = RAID_FOLDERS[raid]

        # Run RAID simulation (threaded inside RAID functions)
        if raid == "RAID 0":
            avg_read, avg_write = raid0(filename, folder=folder)
        elif raid == "RAID 1":
            avg_read, avg_write = raid1(filename, folder=folder)
        elif raid == "RAID 5":
            avg_read, avg_write = raid5(filename, folder=folder)

        # Calculate IOPS
        iops = calculate_iops_per_raid(
            total_ops=total_ops,
            read_ratio=read_ratio,
            write_ratio=write_ratio,
            write_penalty=RAID_WRITE_PENALTY[raid],
            avg_read=avg_read,
            avg_write=avg_write
)

        # Append results
        results.append([
            raid,
            round(avg_read, 6),
            round(avg_write, 6),
            round(iops, 2)
        ])

    # Create DataFrame
    df = pd.DataFrame(
        results,
        columns=["RAID Type", "Avg Read Time", "Avg Write Time", "IOPS"]
    )

    # Save performance log
    df.to_csv("raid_performance.csv", index=False)
    return df

# ------------------------
# Gradio Interface
# ------------------------
file_input = gr.File(label="Upload CSV (100MB+)")
raid_choice = gr.CheckboxGroup(
    ["RAID 0", "RAID 1", "RAID 5"],
    label="Select RAID Type"
)
output_table = gr.Dataframe(headers=["RAID Type", "Avg Read Time", "Avg Write Time", "IOPS"])

gr.Interface(
    fn=run_simulation,
    inputs=[file_input, raid_choice],
    outputs=output_table,
    title="RAID Performance Simulator",
    description="Threaded block-by-block RAID I/O simulation with dynamic IOPS calculation."
).launch()
