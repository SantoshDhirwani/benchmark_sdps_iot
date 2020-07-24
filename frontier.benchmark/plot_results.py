##################################################
## Author: Santosh Dhirwani
##################################################

import os
import re
import sys
import csv
import argparse
from pathlib import Path
from datetime import datetime, timedelta

import matplotlib
import matplotlib.pyplot as plt


CURRENT_DIR = os.path.dirname(os.path.abspath(__name__))
BENCHMARK_PATH = os.path.join(
    CURRENT_DIR,
    "InputGenerator/InputGenerator/src/main/java/de/adrian/"
    "thesis/generator/benchmark/Benchmark.java"
)
SOURCE_DIR_PATH = os.path.join(
    CURRENT_DIR,
    "seep-system/examples/stateful-window-query/"
)
OUTPUT_DIR_PATH = CURRENT_DIR
OUTPUT_FILENAME = "latency_chart_{name}.png"

CHART_TITLE = "Latency chart ({mps} messages per second)"


def load_messages_per_second():
    if not os.path.exists(BENCHMARK_PATH):
        message = (
            "There is not file Benchmark.java to load "
            "default messages per second value"
        )
        raise ValueError(message)

    with open(BENCHMARK_PATH) as fp:
        content = fp.read()

    try:
        return int(re.findall(
            r"protected int messagesPerSecond = (\d+);", content
        )[0])
    except Exception:
        raise ValueError("Can't load default messages per second value")


def load_source():
    print("Find the last benchmark output...")
    paths = [
        path.name
        for path in sorted(
            Path(SOURCE_DIR_PATH).iterdir(),
            key=os.path.getmtime,
            reverse=True,
        )
        if path.name.startswith("output")
    ]
    if not paths:
        raise ValueError("There isn't any generated benchmarks")

    folder_name = paths[0]

    print("Handle {} folder...".format(folder_name))

    last_generated_folder = os.path.join(SOURCE_DIR_PATH, folder_name)
    labels, latency = [], []
    for filename in os.listdir(last_generated_folder):
        if filename.startswith('.'):
            continue

        with open(os.path.join(last_generated_folder, filename), encoding="utf-8", errors="ignore") as fp:
            for row in csv.reader(fp, delimiter=","):
                ts = int(row[1])
                dt = datetime.fromtimestamp(ts // 1000)
                ms = ts % 1000
                label = (dt + timedelta(milliseconds=ms)).strftime("%S")
                labels.append(label)
                latency.append(int(row[2]))

    labels.sort()
    labels = [str(l) for l in labels]

    return folder_name, labels, latency



def build_chart(name, labels, latency, messages_per_second):
    print("Chart building...")

    plt.xlabel("Time(second)")
    plt.ylabel("Latency(ms)")

    title = CHART_TITLE.format(mps=messages_per_second)

    plt.xticks(rotation=90)

    plt.plot(labels,latency, label=title)
    plt.title(title)

    plt.savefig(os.path.join(CURRENT_DIR, OUTPUT_FILENAME.format(name=name)))


def main():
    default_mps = 1000

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--messagesPerSecond",
        default=default_mps,
        help="The number of messages per second",
        type=int
    )
    args = parser.parse_args()

    folder_name, labels, latency = load_source()
    build_chart(folder_name, labels, latency, args.messagesPerSecond)


if __name__ == '__main__':
    main()
