import os
import re
import sys
import csv
import argparse
from pathlib import Path
from datetime import datetime,timedelta
import psutil
import matplotlib
import matplotlib.pyplot as plt
import time

def get_process_info():
    cpu_percent = psutil.cpu_percent(interval=0.5)
    mem_percent = psutil.virtual_memory()

    return cpu_percent, mem_percent.percent

def build_chart(name, cpus, memory,labels):
    print("Chart building...")

    fig, ax = plt.subplots(constrained_layout=True)
    plt.xlabel("Time(minute:second)")
    plt.ylabel("Percentage of usage")
    ax.plot(labels, cpus, label='CPU%')
    ax.plot(labels, memory, label='Memory%')
    ax.legend()

    plt.xticks(rotation=90)

    plt.title("System utilization %")
    cwd = os.getcwd()
    plt.savefig(os.path.join(cwd, name))

def get_data():
    cpus = []
    memory = []
    timestamps = []
    for x in range(20):
        cpu, mem = get_process_info()
        ts = datetime.fromtimestamp(time.time()).strftime("%M:%S")
        cpus.append(cpu)
        memory.append(mem)
        timestamps.append(ts)
        time.sleep(1)
        print(cpu,mem,ts)

    return cpus,memory,timestamps

def main():
    cpus,memory,timestamps = get_data()

    build_chart("sys_usage.png", cpus, memory, timestamps)


if __name__ == '__main__':
    main()
