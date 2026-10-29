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
import csv
import hashlib
import html
import json
import math
from pathlib import Path
import re
import statistics


def summarize(events):
    environments = [e for e in events if e.get("event") == "environment"]
    complete = [e for e in events if e.get("event") == "complete" and e.get("passed")]
    if len(environments) != 1 or len(complete) != 1 or events[-1] != complete[0]:
        raise ValueError("Evidence must contain one environment and a final successful completion")
    environment = environments[0]
    if environment["repetitions"] < 3 or not environment["readers"]:
        raise ValueError("At least three measured repetitions and one concurrency are required")
    runs = [e for e in events if e.get("event") == "result"]
    groups = {}
    for run in runs:
        if not run.get("passed"):
            raise ValueError(f"Failed or incomplete run: {run.get('job')}")
        if (run["unique_rows"] != run["expected"] or run["bad_rows"] != 0
                or run["duplicates"] < 0 or not math.isfinite(run["seconds"]) or run["seconds"] <= 0):
            raise ValueError(f"Invalid correctness or timing evidence: {run.get('job')}")
        if sum(run["producer_partition_counts"]) != run["expected"]:
            raise ValueError("Producer confirmation count differs from expected input")
        key = (run["scenario"], run["requested_readers"], run["repetition"])
        modes = groups.setdefault(key, {})
        if run["mode"] in modes:
            raise ValueError(f"Duplicate mode in pair: {key}")
        modes[run["mode"]] = run
    expected_keys = {(scenario, readers, repetition)
                     for scenario in ("balanced", "hot95", "single")
                     for readers in environment["readers"]
                     for repetition in range(environment["repetitions"] + 1)}
    if set(groups) != expected_keys:
        raise ValueError("Incomplete scenario/concurrency/repetition matrix")
    for key, modes in groups.items():
        if set(modes) != {"standard", "share"}:
            raise ValueError(f"Missing comparison mode: {key}")
        a, b = modes["standard"], modes["share"]
        fields = ("topic", "expected", "partitions", "producer_partition_counts", "warmup")
        if any(a[field] != b[field] for field in fields):
            raise ValueError(f"Mismatched source data: {key}")
        if a["warmup"] != (key[2] == 0) or {a["order"], b["order"]} != {0, 1}:
            raise ValueError(f"Invalid warmup or order evidence: {key}")
        expected_partitions = 1 if key[0] == "single" else 3
        if a["partitions"] != expected_partitions:
            raise ValueError(f"Wrong partition count: {key}")
        expected_counts = [0] * expected_partitions
        for event_id in range(a["expected"]):
            partition = (0 if event_id % 100 < 95 else 1 + (event_id % 100 - 95) % 2) \
                if key[0] == "hot95" else event_id % expected_partitions
            expected_counts[partition] += 1
        if a["producer_partition_counts"] != expected_counts:
            raise ValueError(f"Wrong source skew: {key}")
        for mode, run in modes.items():
            expected_readers = key[1] if mode == "share" else min(key[1], expected_partitions)
            if run["effective_readers"] != expected_readers:
                raise ValueError(f"Concurrency capped unexpectedly: {key}, {mode}")
    summaries = []
    for scenario in ("balanced", "hot95", "single"):
        for readers in environment["readers"]:
            paired_rates = []
            for repetition in range(1, environment["repetitions"] + 1):
                pair = groups[scenario, readers, repetition]
                paired_rates.append((pair["share"]["unique_rows"] / pair["share"]["seconds"]) /
                                    (pair["standard"]["unique_rows"] / pair["standard"]["seconds"]))
            for mode in ("standard", "share"):
                samples = [groups[scenario, readers, r][mode]
                           for r in range(1, environment["repetitions"] + 1)]
                rates = [r["unique_rows"] / r["seconds"] for r in samples]
                summaries.append(dict(scenario=scenario, requested_readers=readers, mode=mode,
                                      effective_readers=samples[0]["effective_readers"],
                                      measured_runs=len(samples), median_unique_rows_s=statistics.median(rates),
                                      minimum_unique_rows_s=min(rates), maximum_unique_rows_s=max(rates),
                                      median_drain_seconds=statistics.median(r["seconds"] for r in samples),
                                      median_visible_95_percent_seconds=statistics.median(
                                          r["visible_95_percent_seconds"] for r in samples),
                                      duplicate_rows=sum(r["duplicates"] for r in samples),
                                      median_paired_share_over_classic=statistics.median(paired_rates)))
    return {"environment": environment, "summary": summaries, "runs": runs}


def reader_evidence(paths, runs):
    jobs = {str(r["job_id"]) for r in runs if "job_id" in r}
    counts = {}
    pattern = re.compile(r"kafka_reader_sample job=(\d+) reader=(\S+) mode=([AC]) "
                         r"partition=(\d+) records=(\d+)")
    for path in paths:
        with path.open(errors="replace") as stream:
            for line in stream:
                match = pattern.search(line)
                if match and match[1] in jobs:
                    key = (match[1], match[2], int(match[4]))
                    counts[key] = counts.get(key, 0) + int(match[5])
    return [dict(job_id=job, reader=reader, partition=partition, received_records=count)
            for (job, reader, partition), count in sorted(counts.items())]


def main():
    parser = argparse.ArgumentParser(description="Report completed native Routine Load comparisons")
    parser.add_argument("evidence", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--be-log", type=Path, action="append", default=[])
    args = parser.parse_args()
    raw = args.evidence.read_bytes()
    result = summarize([json.loads(line) for line in raw.splitlines() if line.strip()])
    result["evidence_sha256"] = hashlib.sha256(raw).hexdigest()
    result["reader_evidence"] = reader_evidence(args.be_log, result["runs"])
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "results.json").write_text(json.dumps(result, indent=2) + "\n")
    with (args.output / "summary.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(result["summary"][0]))
        writer.writeheader()
        writer.writerows(result["summary"])
    columns = ["scenario", "requested_readers", "mode", "effective_readers", "measured_runs",
               "median_unique_rows_s", "minimum_unique_rows_s", "maximum_unique_rows_s",
               "median_drain_seconds", "median_visible_95_percent_seconds", "duplicate_rows",
               "median_paired_share_over_classic"]
    rows = []
    for row in result["summary"]:
        values = [f"{row[c]:.3f}" if isinstance(row[c], float) else str(row[c]) for c in columns]
        rows.append("<tr>" + "".join(f"<td>{html.escape(v)}</td>" for v in values) + "</tr>")
    page = """<!doctype html><html lang="en"><meta charset="utf-8">
<title>Native Kafka Routine Load comparison</title>
<style>body{font:16px system-ui;margin:2rem;color:#172033}table{border-collapse:collapse}
td,th{padding:.6rem;border:1px solid #ccd;text-align:right}th{background:#eef2ff}
pre{white-space:pre-wrap}section{overflow:auto}</style>
<h1>Native Kafka Routine Load: classic and share groups</h1>
<p>Sequential runs on identical preloaded topics. Warmups excluded; medians and ranges shown.
Throughput counts unique visible IDs. The 95% time is backlog completion time,
not a per-record latency percentile. Scheduling, job creation and polling are included.
Three buckets, one replica, LZ4 producer batches and ZSTD Doris tables.
No simulated handler delay. Default development ASAN builds are not production capacity measurements.
Effective readers means scheduled task concurrency, not simultaneous readers receiving data.</p>
<section><table><thead><tr>"""
    page += "".join(f"<th>{html.escape(c)}</th>" for c in columns) + "</tr></thead><tbody>"
    page += "".join(rows) + "</tbody></table></section><h2>Recorded environment</h2><pre>"
    page += html.escape(json.dumps(result["environment"], indent=2)) + "</pre>"
    page += "<p>Revision fields are operator declarations; compare them with the running builds. "
    page += "CPU/memory overhead and replicated failure durability are not measured.</p>"
    page += "<h2>Reader receipt evidence</h2><p>Matching BE telemetry samples only. "
    page += "Reader identities over a whole run do not establish simultaneous concurrency.</p><pre>"
    page += html.escape(json.dumps(result["reader_evidence"], indent=2)) + "</pre>"
    page += f"<p>Raw evidence SHA-256: {result['evidence_sha256']}</p></html>"
    (args.output / "report.html").write_text(page)
    print(args.output / "report.html")


if __name__ == "__main__":
    main()
