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

prev_sent = 0
prev_recv = 0

def get_process_info():
    cpu_percent = psutil.cpu_percent(interval=0.5)
    mem_percent = psutil.virtual_memory()

    return cpu_percent, mem_percent.percent

def get_net_info():
    global prev_sent, prev_recv

    net = psutil.net_io_counters(pernic=True, nowrap=True)['ens33']
    curr_sent = net.bytes_sent
    curr_recv = net.bytes_recv

    sent = round((curr_sent - prev_sent) / 1024 / 1024, 3)
    recv = round((curr_recv - prev_recv) / 1024 / 1024, 3)

    prev_sent = curr_sent
    prev_recv = curr_recv

    return sent, recv

def build_chart(name, cpus, memory,net_sent,net_recv,labels):
    print("Chart building...")

    fig, ax = plt.subplots(constrained_layout=True)
    plt.xlabel("Time(minutes:seconds)")
    plt.ylabel("Percentage of usage(%)")
    ax.plot(labels, cpus, label='CPU%')
    ax.plot(labels, memory, label='Memory%')
    ax.plot(labels, net_sent, label='Sent(MB)')
    ax.plot(labels, net_recv, label='Recv(MB)')
    ax.legend()

    plt.xticks(rotation=90)

    plt.title("System utilization %")
    cwd = os.getcwd()
    plt.savefig(os.path.join(cwd, name))



def get_data():
    cpus = []
    memory = []
    net_sent = []
    net_recv = []
    timestamps = []
    for x in range(30):
        cpu, mem = get_process_info()
