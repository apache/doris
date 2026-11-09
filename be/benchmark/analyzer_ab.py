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

"""Paired throughput comparison of two benchmark_test binaries built with
benchmark_analyzer.hpp, as analyzer_benchmark.md describes.

Each round runs both binaries once, alternating which goes first, so drift in the host's load
hits both sides alike. For every benchmark the candidate/baseline throughput ratio is taken per
round and the median over the rounds is reported, with the geometric mean over the corpora of
each analyzer. Give the same binary as both sides for the A/A check: every ratio should stay
within 5% of 1.

usage: analyzer_ab.py --baseline BIN --candidate BIN --filter REGEX --out DIR [--rounds N]

The environment the binaries need (ANALYZER_BENCH_CORPUS_DIR, ANALYZER_BENCH_DICT_DIR, ...) is
passed through.
"""

import argparse
import json
import math
import os
import statistics
import subprocess
import sys


def run(binary, benchmark_filter, out):
    subprocess.run([binary, f"--benchmark_filter={benchmark_filter}", f"--benchmark_out={out}",
                    "--benchmark_out_format=json"], check=True, stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL)


def throughput(path):
    """Bytes per second by (path, analyzer, corpus), from a benchmark JSON file."""
    result = {}
    with open(path) as handle:
        for benchmark in json.load(handle)["benchmarks"]:
            name = benchmark["name"].split("/min_time")[0]
            result[tuple(name.split("/")[:3])] = benchmark["bytes_per_second"]
    return result


def geomean(values):
    return math.exp(sum(math.log(v) for v in values) / len(values))


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--filter", required=True, help="--benchmark_filter for both binaries")
    parser.add_argument("--out", required=True, help="directory for the JSON of every run")
    parser.add_argument("--rounds", type=int, default=5)
    args = parser.parse_args()
    os.makedirs(args.out, exist_ok=True)

    ratios = {}
    for round_index in range(args.rounds):
        sides = [("baseline", args.baseline), ("candidate", args.candidate)]
        if round_index % 2 == 1:
            sides.reverse()
        results = {}
        for side, binary in sides:
            path = os.path.join(args.out, f"{side}{round_index}.json")
            run(binary, args.filter, path)
            results[side] = throughput(path)
        for key, baseline in results["baseline"].items():
            ratios.setdefault(key, []).append(results["candidate"][key] / baseline)

    medians = {key: statistics.median(values) for key, values in ratios.items()}
    analyzers = sorted({(path, analyzer) for path, analyzer, _ in medians})
    print(f"{'benchmark':40} {'geomean':>8}  per corpus: median (min-max) of the round ratios")
    for path, analyzer in analyzers:
        cells = sorted((corpus, medians[(p, a, corpus)], ratios[(p, a, corpus)])
                       for p, a, corpus in medians if (p, a) == (path, analyzer))
        mean = geomean([median for _, median, _ in cells])
        detail = "  ".join(f"{corpus} {median:.2f} ({min(values):.2f}-{max(values):.2f})"
                           for corpus, median, values in cells)
        print(f"{path + '/' + analyzer:40} {mean:8.2f}  {detail}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
