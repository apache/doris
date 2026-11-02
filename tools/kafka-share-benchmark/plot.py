#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
from collections import defaultdict
import json
import os
from pathlib import Path
import statistics


def main():
    parser = argparse.ArgumentParser(description="Plot verified native Routine Load benchmark results")
    parser.add_argument("results", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    data = json.loads(args.results.read_text())
    args.output.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(args.output / ".matplotlib"))
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import StrMethodFormatter

    colors = {"standard": "#2563eb", "share": "#ea580c"}
    labels = {"standard": "Classic", "share": "Share"}
    scenarios = ["balanced", "hot95", "single"]
    titles = ["3 balanced partitions", "95% on one hot partition", "One partition"]
    tasks = data["environment"]["readers"]
    summary = {(r["scenario"], r["requested_tasks"], r["mode"]): r for r in data["summary"]}
    top = max(r["maximum_unique_rows_s"] for r in data["summary"]) * 1.25
    fig, axes = plt.subplots(1, 3, figsize=(16, 5), sharey=True, constrained_layout=True)
    for ax, scenario, title in zip(axes, scenarios, titles):
        for mode, displacement in (("standard", -0.2), ("share", 0.2)):
            rows = [summary[scenario, count, mode] for count in tasks]
            rates = [r["median_unique_rows_s"] for r in rows]
            errors = [[r["median_unique_rows_s"] - r["minimum_unique_rows_s"] for r in rows],
                      [r["maximum_unique_rows_s"] - r["median_unique_rows_s"] for r in rows]]
            bars = ax.bar([i + displacement for i in range(len(tasks))], rates, width=0.38,
                          color=colors[mode], label=labels[mode], yerr=errors, capsize=3)
            ax.bar_label(bars, labels=[f"{r:,.0f}" for r in rates], padding=4, fontsize=8)
        ax.set_xticks(range(len(tasks)), [str(t) for t in tasks])
        ax.set_title(title)
        ax.set_xlabel("Desired Routine Load tasks")
        ax.set_ylim(0, top)
        ax.grid(axis="y", alpha=0.2)
        ax.set_axisbelow(True)
        ax.yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))
        ax.legend()
    axes[0].set_ylabel("Unique visible rows/s")
    fig.suptitle(f"Native Kafka -> Doris Routine Load\nMedians of {data['environment']['repetitions']} measured runs; whiskers show min/max; classic tasks capped by partitions")
    for extension in ("png", "svg"):
        fig.savefig(args.output / f"throughput.{extension}", dpi=160)
    plt.close(fig)

    runs = [r for r in data["runs"] if not r["warmup"] and r["scenario"] == "hot95"
            and r["requested_tasks"] == max(tasks)]
    pairs = defaultdict(dict)
    for run in runs:
        pairs[run["repetition"]][run["mode"]] = run
    ratios = {key: (pair["share"]["unique_rows"] / pair["share"]["seconds"]) /
              (pair["standard"]["unique_rows"] / pair["standard"]["seconds"]) for key, pair in pairs.items()}
    middle = statistics.median(ratios.values())
    selected = pairs[min(ratios, key=lambda key: abs(ratios[key] - middle))]
    fig, ax = plt.subplots(figsize=(9, 5), constrained_layout=True)
    for mode in ("standard", "share"):
        run = selected[mode]
        ax.step([0] + [r["seconds"] for r in run["samples"]],
                [0] + [100 * r["unique"] / run["expected"] for r in run["samples"]],
                where="post", label=labels[mode], color=colors[mode], linewidth=2)
    ax.axhline(95, color="#64748b", linestyle=":", label="95% of unique IDs")
    ax.set(xlabel="Seconds since each job was created", ylabel="Expected unique IDs visible (%)", ylim=(0, 105),
           title=f"Hot partition backlog: {max(tasks)} desired tasks\nRepresentative paired repetition; independently aligned starts; 500 ms observations")
    ax.grid(alpha=0.2)
    ax.legend()
    for extension in ("png", "svg"):
        fig.savefig(args.output / f"hot95-progress.{extension}", dpi=160)
    plt.close(fig)

    receipts = data.get("reader_evidence", [])
    if receipts:
        fig, axes = plt.subplots(1, 2, figsize=(13, 5), sharey=True, constrained_layout=True)
        for ax, mode in zip(axes, ("standard", "share")):
            rows = [r for r in receipts if str(r["job_id"]) == str(selected[mode]["job_id"])]
            readers = sorted({r["reader"] for r in rows})
            bottom = [0] * len(readers)
            for partition, color in ((0, "#dc2626"), (1, "#94a3b8"), (2, "#475569")):
                values = [sum(r["received_records"] for r in rows if r["reader"] == reader
                              and r["partition"] == partition) for reader in readers]
                ax.bar(range(len(readers)), values, bottom=bottom, color=color,
                       label=f"P{partition}" + (" (hot)" if partition == 0 else ""))
                bottom = [a + b for a, b in zip(bottom, values)]
            ax.set_xticks(range(len(readers)), [f"R{i + 1}" for i in range(len(readers))])
            ax.set_title(labels[mode] + ": observed reader handles")
            ax.set_xlabel("Reader handle within this run")
            ax.grid(axis="y", alpha=0.2)
            ax.set_axisbelow(True)
            ax.legend()
        axes[0].set_ylabel("Received record attempts (whole run)")
        fig.suptitle("Hot partition distribution from BE telemetry\nWhole-run participation, not instantaneous concurrency; full reader IDs are in results.json")
        for extension in ("png", "svg"):
            fig.savefig(args.output / f"hot95-readers.{extension}", dpi=160)
        plt.close(fig)
    print(args.output / "throughput.png")


if __name__ == "__main__":
    main()
