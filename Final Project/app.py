import gradio as gr
import pandas as pd
from analysis.raid_simulation import raid0, raid1, raid5, measure_raid_rw
from analysis.iops_calculator import disk_load
from analysis.stats import calculate_read_write_ratio

# RAID write penalties
raid_write_penalty = {"RAID 0":1, "RAID 1":2, "RAID 5":3}
raid_disks = {"RAID 0":3, "RAID 1":2, "RAID 5":3}

raid_folders = {
    "RAID 0": "storage/raid0",
    "RAID 1": "storage/raid1",
    "RAID 5": "storage/raid5"
}

def run_simulation(file, raid_types):
    filename = file.name
    read_ratio, write_ratio, total_ops = calculate_read_write_ratio(filename)
    results = []

    for raid in raid_types:
        folder = raid_folders[raid]
        num_disks = raid_disks[raid]

        # simulate RAID
        if raid=="RAID 0": raid0(filename, num_disks, folder)
        elif raid=="RAID 1": raid1(filename, num_disks, folder)
        elif raid=="RAID 5": raid5(filename, num_disks, folder)

        # measure read/write from RAID disks
        avg_read, avg_write = measure_raid_rw(folder, num_disks)
        # calculate IOPS
        iops = disk_load(total_ops, read_ratio, write_ratio, raid_write_penalty[raid])
        results.append([raid, round(avg_read,6), round(avg_write,6), round(iops,2)])

    df = pd.DataFrame(results, columns=["RAID Type","Avg Read Time","Avg Write Time","IOPS"])
    df.to_csv("raid_performance.csv", index=False)
    return df

# Gradio UI
file_input = gr.File(label="Upload CSV")
raid_choice = gr.CheckboxGroup(["RAID 0", "RAID 1", "RAID 5"], label="Select RAID Type")
output_table = gr.Dataframe(headers=["RAID Type","Avg Read Time","Avg Write Time","IOPS"])

gr.Interface(
    fn=run_simulation,
    inputs=[file_input, raid_choice],
    outputs=[output_table],
    title="RAID Performance Simulator",
    description="Upload a large CSV file and simulate RAID 0/1/5 with read/write times and dynamic IOPS."
).launch()
