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

"""Compare same-agent ABBA Parquet benchmark samples and enforce a conservative gate."""

import argparse
import hashlib
import json
import math
import random
import statistics
import sys
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-a1", type=Path, required=True)
    parser.add_argument("--head-b1", type=Path, required=True)
    parser.add_argument("--head-b2", type=Path, required=True)
    parser.add_argument("--base-a2", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-markdown", type=Path, required=True)
    parser.add_argument("--regression-threshold-pct", type=float, default=15.0)
    parser.add_argument("--warning-threshold-pct", type=float, default=5.0)
    parser.add_argument("--confidence-margin-pct", type=float, default=5.0)
    parser.add_argument("--max-cv-pct", type=float, default=3.0)
    return parser.parse_args()


def load_samples(path):
    with path.open(encoding="utf-8") as stream:
        document = json.load(stream)
    samples = {}
    for row in document.get("benchmarks", []):
        if row.get("error_occurred", False):
            raise ValueError(f"{path}: benchmark failed: {row.get('name')}")
        if row.get("run_type") == "aggregate" or "aggregate_name" in row:
            continue
        name = row["name"]
        metric = row.get("ns/raw_row")
        if not isinstance(metric, (int, float)) or not math.isfinite(metric) or metric <= 0:
            raise ValueError(f"{path}: invalid ns/raw_row for {name}")
        counters = {}
        for counter_name in ("raw_rows", "selected_rows"):
            counter = row.get(counter_name)
            if not isinstance(counter, (int, float)) or not math.isfinite(counter):
                raise ValueError(f"{path}: invalid {counter_name} for {name}")
            counters[counter_name] = float(counter)
        case = samples.setdefault(
            name, {"metric": [], "raw_rows": set(), "selected_rows": set()}
        )
        case["metric"].append(float(metric))
        case["raw_rows"].add(counters["raw_rows"])
        case["selected_rows"].add(counters["selected_rows"])
    if not samples:
        raise ValueError(f"{path}: no iteration samples")
    return samples


def coefficient_of_variation(values):
    if len(values) < 2:
        return math.inf
    return statistics.stdev(values) / statistics.mean(values) * 100.0


def regression_pct(base_values, head_values):
    return (statistics.median(head_values) / statistics.median(base_values) - 1.0) * 100.0


def bootstrap_interval(name, base_values, head_values, iterations=10000):
    seed = int.from_bytes(hashlib.sha256(name.encode()).digest()[:8], "big")
    rng = random.Random(seed)
    ratios = []
    for _ in range(iterations):
        base = [rng.choice(base_values) for _ in base_values]
        head = [rng.choice(head_values) for _ in head_values]
        ratios.append(regression_pct(base, head))
    ratios.sort()
    return ratios[int(iterations * 0.025)], ratios[int(iterations * 0.975)]


def main():
    args = parse_args()
    phases = {
        "base_a1": load_samples(args.base_a1),
        "head_b1": load_samples(args.head_b1),
        "head_b2": load_samples(args.head_b2),
        "base_a2": load_samples(args.base_a2),
    }
    names = set(phases["base_a1"])
    if any(set(samples) != names for samples in phases.values()):
        raise ValueError("ABBA phases do not contain the same benchmark cases")

    results = []
    for name in sorted(names):
        phase_cases = {phase: samples[name] for phase, samples in phases.items()}
        for counter_name in ("raw_rows", "selected_rows"):
            values = set()
            for case in phase_cases.values():
                values.update(case[counter_name])
            if len(values) != 1:
                raise ValueError(f"{name}: ABBA {counter_name} values differ: {sorted(values)}")
        phase_values = {phase: case["metric"] for phase, case in phase_cases.items()}
        if any(len(values) != 5 for values in phase_values.values()):
            raise ValueError(f"{name}: expected five repetitions in every ABBA phase")
        base = phase_values["base_a1"] + phase_values["base_a2"]
        head = phase_values["head_b1"] + phase_values["head_b2"]
        delta = regression_pct(base, head)
        half_1 = regression_pct(phase_values["base_a1"], phase_values["head_b1"])
        half_2 = regression_pct(phase_values["base_a2"], phase_values["head_b2"])
        lower, upper = bootstrap_interval(name, base, head)
        base_cv = coefficient_of_variation(base)
        head_cv = coefficient_of_variation(head)
        stable = base_cv <= args.max_cv_pct and head_cv <= args.max_cv_pct
        failed = (
            stable
            and delta >= args.regression_threshold_pct
            and lower >= args.regression_threshold_pct - args.confidence_margin_pct
            and half_1 >= args.regression_threshold_pct - args.confidence_margin_pct
            and half_2 >= args.regression_threshold_pct - args.confidence_margin_pct
        )
        if failed:
            status = "FAIL"
        elif delta >= args.warning_threshold_pct:
            status = "WARN" if stable else "NOISY"
        else:
            status = "PASS" if stable else "NOISY"
        results.append(
            {
                "name": name,
                "status": status,
                "regression_pct": delta,
                "ci95_pct": [lower, upper],
                "abba_half_pct": [half_1, half_2],
                "base_median_ns_per_raw_row": statistics.median(base),
                "head_median_ns_per_raw_row": statistics.median(head),
                "base_cv_pct": base_cv,
                "head_cv_pct": head_cv,
            }
        )

    summary = {
        "metric": "ns/raw_row",
        "policy": {
            "regression_threshold_pct": args.regression_threshold_pct,
            "warning_threshold_pct": args.warning_threshold_pct,
            "confidence_margin_pct": args.confidence_margin_pct,
            "max_cv_pct": args.max_cv_pct,
            "required_repetitions_per_revision": 10,
            "order": "ABBA",
        },
        "counts": {
            status: sum(result["status"] == status for result in results)
            for status in ("FAIL", "WARN", "NOISY", "PASS")
        },
        "results": results,
    }
    args.output_json.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    lines = [
        "# Parquet microbenchmark comparison",
        "",
        f"Metric: `ns/raw_row`; order: ABBA; samples: 10 per revision; "
        f"hard gate: {args.regression_threshold_pct:g}%; warning: "
        f"{args.warning_threshold_pct:g}%; max CV: {args.max_cv_pct:g}%.",
        "",
        "| Status | Regression | 95% CI | Base CV | PR CV | Case |",
        "|---|---:|---:|---:|---:|---|",
    ]
    order = {"FAIL": 0, "WARN": 1, "NOISY": 2, "PASS": 3}
    for result in sorted(
        results, key=lambda item: (order[item["status"]], -item["regression_pct"])
    ):
        lines.append(
            f"| {result['status']} | {result['regression_pct']:+.2f}% | "
            f"[{result['ci95_pct'][0]:+.2f}%, {result['ci95_pct'][1]:+.2f}%] | "
            f"{result['base_cv_pct']:.2f}% | {result['head_cv_pct']:.2f}% | "
            f"`{result['name']}` |"
        )
    args.output_markdown.write_text("\n".join(lines) + "\n", encoding="utf-8")

    for result in results:
        print(
            f"{result['status']:5} {result['regression_pct']:+7.2f}% "
            f"base_cv={result['base_cv_pct']:.2f}% head_cv={result['head_cv_pct']:.2f}% "
            f"{result['name']}"
        )
    return 1 if summary["counts"]["FAIL"] else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(2)
