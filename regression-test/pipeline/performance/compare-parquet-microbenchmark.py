#!/usr/bin/env python
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

from __future__ import print_function

import argparse
import hashlib
import io
import json
import math
import random
import sys

try:
    text_type = unicode
except NameError:
    text_type = str


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-a1", required=True)
    parser.add_argument("--head-b1", required=True)
    parser.add_argument("--head-b2", required=True)
    parser.add_argument("--base-a2", required=True)
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--output-markdown", required=True)
    parser.add_argument("--regression-threshold-pct", type=float, default=15.0)
    parser.add_argument("--warning-threshold-pct", type=float, default=5.0)
    parser.add_argument("--confidence-margin-pct", type=float, default=5.0)
    parser.add_argument("--max-cv-pct", type=float, default=3.0)
    return parser.parse_args()


def load_samples(path):
    with io.open(path, encoding="utf-8") as stream:
        document = json.load(stream)
    samples = {}
    for row in document.get("benchmarks", []):
        if row.get("error_occurred", False):
            raise ValueError(
                "{0}: benchmark failed: {1}".format(path, row.get("name"))
            )
        if row.get("run_type") == "aggregate" or "aggregate_name" in row:
            continue
        name = row["name"]
        metric = row.get("ns/raw_row")
        if (
            not isinstance(metric, (int, float))
            or not is_finite(metric)
            or metric <= 0
        ):
            raise ValueError(
                "{0}: invalid ns/raw_row for {1}".format(path, name)
            )
        counters = {}
        for counter_name in ("raw_rows", "selected_rows"):
            counter = row.get(counter_name)
            if not isinstance(counter, (int, float)) or not is_finite(counter):
                raise ValueError(
                    "{0}: invalid {1} for {2}".format(path, counter_name, name)
                )
            counters[counter_name] = float(counter)
        case = samples.setdefault(
            name, {"metric": [], "raw_rows": set(), "selected_rows": set()}
        )
        case["metric"].append(float(metric))
        case["raw_rows"].add(counters["raw_rows"])
        case["selected_rows"].add(counters["selected_rows"])
    if not samples:
        raise ValueError("{0}: no iteration samples".format(path))
    return samples


def is_finite(value):
    return not math.isnan(value) and not math.isinf(value)


def median(values):
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2.0


def sample_stdev(values):
    mean = sum(values) / float(len(values))
    return math.sqrt(
        sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    )


def coefficient_of_variation(values):
    if len(values) < 2:
        return float("inf")
    return sample_stdev(values) / (sum(values) / float(len(values))) * 100.0


def regression_pct(base_values, head_values):
    return (median(head_values) / median(base_values) - 1.0) * 100.0


def bootstrap_interval(name, base_values, head_values, iterations=10000):
    seed = int(hashlib.sha256(name.encode("utf-8")).hexdigest()[:16], 16)
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
                raise ValueError(
                    "{0}: ABBA {1} values differ: {2}".format(
                        name, counter_name, sorted(values)
                    )
                )
        phase_values = {phase: case["metric"] for phase, case in phase_cases.items()}
        if any(len(values) != 5 for values in phase_values.values()):
            raise ValueError(
                "{0}: expected five repetitions in every ABBA phase".format(name)
            )
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
            status = "WARN" if stable else "INCONCLUSIVE"
        else:
            status = "PASS" if stable else "NOISY"
        results.append(
            {
                "name": name,
                "status": status,
                "regression_pct": delta,
                "ci95_pct": [lower, upper],
                "abba_half_pct": [half_1, half_2],
                "base_median_ns_per_raw_row": median(base),
                "head_median_ns_per_raw_row": median(head),
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
            for status in ("FAIL", "WARN", "INCONCLUSIVE", "NOISY", "PASS")
        },
        "results": results,
    }
    with io.open(args.output_json, "w", encoding="utf-8") as output_stream:
        output_stream.write(text_type(json.dumps(summary, indent=2)) + u"\n")

    lines = [
        "# Parquet microbenchmark comparison",
        "",
        "Metric: `ns/raw_row`; order: ABBA; samples: 10 per revision; "
        "hard gate: {0:g}%; warning: {1:g}%; max CV: {2:g}%.".format(
            args.regression_threshold_pct,
            args.warning_threshold_pct,
            args.max_cv_pct,
        ),
        "",
        "| Status | Regression | 95% CI | Base CV | PR CV | Case |",
        "|---|---:|---:|---:|---:|---|",
    ]
    order = {"FAIL": 0, "INCONCLUSIVE": 1, "WARN": 2, "NOISY": 3, "PASS": 4}
    for result in sorted(
        results, key=lambda item: (order[item["status"]], -item["regression_pct"])
    ):
        lines.append(
            "| {0} | {1:+.2f}% | [{2:+.2f}%, {3:+.2f}%] | "
            "{4:.2f}% | {5:.2f}% | `{6}` |".format(
                result["status"],
                result["regression_pct"],
                result["ci95_pct"][0],
                result["ci95_pct"][1],
                result["base_cv_pct"],
                result["head_cv_pct"],
                result["name"],
            )
        )
    with io.open(args.output_markdown, "w", encoding="utf-8") as output_stream:
        output_stream.write(text_type("\n".join(lines)) + u"\n")

    for result in results:
        print(
            "{0:5} {1:+7.2f}% base_cv={2:.2f}% head_cv={3:.2f}% {4}".format(
                result["status"],
                result["regression_pct"],
                result["base_cv_pct"],
                result["head_cv_pct"],
                result["name"],
            )
        )
    if summary["counts"]["FAIL"]:
        return 1
    if summary["counts"]["INCONCLUSIVE"]:
        return 3
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, KeyError) as error:
        print("ERROR: {0}".format(error), file=sys.stderr)
        sys.exit(2)
