##################################################
## Author: Santosh Dhirwani
##################################################

import os
import re
import sys
import csv
import argparse
from pathlib import Path
from datetime import datetime

import matplotlib
import matplotlib.pyplot as plt


CURRENT_DIR = os.path.dirname(os.path.abspath(__name__))
BENCHMARK_PATH = os.path.join(
    CURRENT_DIR,
    "InputGenerator-master/InputGenerator/src/main/java/de/adrian/"
    "thesis/generator/benchmark/Benchmark.java"
)
SOURCE_DIR_PATH = os.path.join(
    CURRENT_DIR,
    "FlinkJobsMisc-master"
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
        if path.name.startswith("yahooBenchmarkOutput")
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
                labels.append(datetime.fromtimestamp(int(row[3]) // 1000))
                latency.append(int(row[2]))

    labels.sort()
    labels = [str(l) for l in labels]

    return folder_name, labels, latency



def build_chart(name, labels, latency, messages_per_second):
    print("Chart building...")
    plt.rcParams["figure.figsize"] = (20, 10)
    plt.rcParams['axes.titlepad'] = 20

    title = CHART_TITLE.format(mps=messages_per_second)

    fig, ax = plt.subplots()

    y_pos = range(len(labels))

    ax.barh(y_pos, latency, align='center')
    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels)
    # ax.invert_yaxis()
    ax.set_xlabel('Latency')
    ax.set_title(title, {
        "fontsize": 26,
    })

    for i, v in enumerate(latency):
        ax.text(v + 3, i - .45, str(v), color='blue', fontweight='bold')

    plt.savefig(os.path.join(CURRENT_DIR, OUTPUT_FILENAME.format(name=name)))


def main():
    default_mps = load_messages_per_second()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--messagesPerSecond",
        default=default_mps,
        help="The number of messages per second",
        type=int,
    )
    args = parser.parse_args()

    folder_name, labels, latency = load_source()
    build_chart(folder_name, labels, latency, args.messagesPerSecond)


if __name__ == '__main__':
    main()
