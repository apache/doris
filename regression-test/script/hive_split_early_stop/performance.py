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

"""Run comparable Split benchmarks with external process sampling and summarize measurements."""

import argparse
import csv
import json
import os
import statistics
import subprocess
import threading
import time
from collections import defaultdict
from pathlib import Path

from report import physical_evidence, require

SCENARIOS = ("limit10_cold_metadata", "limit10_warm_metadata", "cancel_sum", "timeout_sum",
             "full_sum_10000_batch", "full_sum_10000_sync", "full_sum_10000_cached")


def process_sample(pid):
    # A command name can contain spaces or parentheses. The remaining fields begin at field 3.
    fields = Path("/proc", str(pid), "stat").read_text().rsplit(")", 1)[1].split()
    require(fields[0] != "Z", "Benchmark process exited: " + str(pid))
    return ((int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK"),
            int(fields[21]) * os.sysconf("SC_PAGE_SIZE"))


def run(args):
    destination = args.output / args.label
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "run.json").unlink(missing_ok=True)
    processes = {"fe": int((args.fe_dir / "bin/fe.pid").read_text()),
                 "be": int((args.be_dir / "bin/be.pid").read_text()),
                 "be2": int((args.be2_dir / "bin/be.pid").read_text())}
    arguments = Path("/proc", str(processes["fe"]), "cmdline").read_bytes().split(b"\0")
    require(not any(argument.startswith(b"-javaagent:") for argument in arguments),
            "Remove Java agents before measuring performance")
    stopped = threading.Event()
    failures = []

    def sample():
        try:
            with (destination / "process.csv").open("w") as source:
                writer = csv.writer(source)
                writer.writerow(("time_ms", "role", "cpu_seconds", "rss_bytes", "host_load1"))
                while not stopped.is_set():
                    now, load = time.time_ns() // 1000000, os.getloadavg()[0]
                    for role, pid in processes.items():
                        cpu, rss = process_sample(pid)
                        writer.writerow((now, role, cpu, rss, load))
                    stopped.wait(0.1)
        except Exception as error:
            failures.append(error)

    thread = threading.Thread(target=sample, name="split-performance-process-sampler")
    thread.start()
    command = ["./run-regression-test.sh", "--run", "--conf", str(args.config),
               "-d", "external_table_p0/hive", "-s", "test_hive_split_early_stop_performance", "-parallel", "1"]
    if args.generate:
        command.append("-forceGenOut")
    environment = {key: value for key, value in os.environ.items() if key.lower() not in
                   ("http_proxy", "https_proxy", "all_proxy")}
    environment["JAVA_OPTS"] = "-Xmx1024m -XX:ActiveProcessorCount=2"
    try:
        with (destination / "regression.log").open("w") as log:
            completed = subprocess.run(command, env=environment, stdout=log, stderr=subprocess.STDOUT)
        # Keep the sampler alive beyond the last observation marker.
        stopped.wait(0.3)
    finally:
        stopped.set()
        thread.join()
    require(not failures, "Process sampler failed: " + str(failures))
    require(completed.returncode == 0, "Regression runner failed; inspect " + str(destination / "regression.log"))
    (destination / "run.json").write_text(json.dumps({"label": args.label, "processes": processes,
        "command": command, "sampling_interval_ms": 100, "java_agent": False}, indent=2) + "\n")
    print("Completed benchmark pass: " + args.label)


def measured_cases(log, label, repetitions, events, samples, scenarios=SCENARIOS):
    require("All suites success." in log, label + ": runner did not succeed")
    cases = [json.loads(line.split("SPLIT_PERF ", 1)[1]) for line in log.splitlines() if "SPLIT_PERF {" in line]
    require([(item["scenario"], item["repetition"]) for item in cases]
            == [(name, repetition) for repetition in range(repetitions) for name in scenarios],
            label + ": missing, repeated or out-of-order scenarios")
    for item in cases:
        require(item["run"] == label, "Configuration label does not match the measured run")
        begin, returned, end = (item[key] for key in ("begin_ms", "return_ms", "observed_ms"))
        window = 1000 if item["scenario"].startswith("full_") else 10000
        require(begin <= returned and end >= returned + window, "Incomplete performance observation")
        requests = [event for event in events if event["phase"] == item["phase"]
                    and begin <= event["time_ms"] <= end]
        listings = [event for event in requests if event["action"] == "LIST"]
        require(all(event.get("synthetic_listing") is False for event in listings), "Synthetic physical listing")
        # The HTTP metrics snapshot precedes the client's observation marker. Additional
        # requests can arrive in between; the event log covers the whole measured window.
        require(len(listings) >= item["counts"]["LIST"], "Request log is missing proxy-counted listings")
        item["list_total"] = len(listings)
        item["list_after_return"] = sum(event["time_ms"] > returned for event in listings)
        item["list_after_signal"] = sum(event["time_ms"] > item["signal_ms"] for event in listings)
        item["return_after_signal_ms"] = returned - item["signal_ms"]
        item["get_total"] = sum(event["action"] == "GET" for event in requests)
        if not item["scenario"].startswith("full_"):
            drained = [event for event in events if event["phase"] == item["phase"]
                       and event["time_ms"] >= begin and event["action"] == "LIST"]
            item["list_until_idle"] = len(drained)
            item["last_list_after_return_until_idle_ms"] = max(
                (event["time_ms"] - returned for event in drained), default=0)
        item["last_list_after_return_ms"] = max(
            (event["time_ms"] - returned for event in listings), default=0)
        if item["scenario"] == "cancel_sum":
            require(any(event["action"] == "GET" and event["time_ms"] <= item["signal_ms"] for event in requests),
                    "Cancelled SUM did not reach BE file reads")
        if item["scenario"] == "cancel_before_read":
            require(sum(event["time_ms"] <= item["signal_ms"] for event in listings) >= 8
                    and not any(event["action"] == "GET" and event["time_ms"] <= item["signal_ms"]
                                for event in requests), "Cancellation did not occur before BE file reads")
        if item["scenario"] == "full_sum_10000_cached":
            require(not listings, "Timed cache-hit query missed the file-list cache")
        for role in ("fe", "be", "be2"):
            rows = samples[role]
            prior = [row for row in rows if row["time_ms"] <= begin]
            following = [row for row in rows if row["time_ms"] >= end]
            require(prior and following, role + ": sampler did not cover the case")
            first, last = prior[-1], following[0]
            near_return = min(rows, key=lambda row: abs(row["time_ms"] - returned))
            require(begin - first["time_ms"] < 1000 and last["time_ms"] - end < 1000
                    and abs(near_return["time_ms"] - returned) < 1000, role + ": stale process samples")
            during = [row for row in rows if first["time_ms"] <= row["time_ms"] <= last["time_ms"]]
            item[role + "_query_cpu_s"] = near_return["cpu_seconds"] - first["cpu_seconds"]
            item[role + "_post_return_cpu_s"] = last["cpu_seconds"] - near_return["cpu_seconds"]
            item[role + "_peak_rss_mb"] = max(row["rss_bytes"] for row in during) / 1024 ** 2
            item[role + "_rss_change_mb"] = (last["rss_bytes"] - first["rss_bytes"]) / 1024 ** 2
            if role == "fe":
                item["host_load1_median"] = statistics.median(float(row["host_load1"]) for row in during)
    return cases


def summarize(args):
    (args.output / "performance-results.json").unlink(missing_ok=True)
    events = [json.loads(line) for line in (args.output / "s3-requests.jsonl").read_text().splitlines()]
    groups = defaultdict(list)
    all_cases = []
    passes = [(label, "before", SCENARIOS) for label in args.before]
    passes += [(label, "after", SCENARIOS) for label in args.after]
    require(bool(args.planning_before) == bool(args.planning_after), "Supply both planning comparison passes")
    if args.planning_before:
        passes += [(args.planning_before, "before", ("cancel_before_read",)),
                   (args.planning_after, "after", ("cancel_before_read",))]
    require(bool(args.steady_before) == bool(args.steady_after), "Supply both steady LIMIT comparison passes")
    if args.steady_before:
        passes += [(args.steady_before, "before", ("limit10_steady",)),
                   (args.steady_after, "after", ("limit10_steady",))]
    for label, version, scenarios in passes:
        samples = defaultdict(list)
        with (args.output / label / "process.csv").open() as source:
            for row in csv.DictReader(source):
                for key in ("time_ms", "rss_bytes"):
                    row[key] = int(row[key])
                row["cpu_seconds"] = float(row["cpu_seconds"])
                samples[row["role"]].append(row)
        cases = measured_cases((args.output / label / "regression.log").read_text(),
                               label, args.repetitions, events, samples, scenarios)
        for item in cases:
            groups[(version, item["scenario"])].append(item)
        all_cases.extend(cases)
    metrics = ("elapsed_ms", "return_after_signal_ms", "list_total", "list_after_return", "list_after_signal",
               "get_total", "fe_query_cpu_s", "fe_post_return_cpu_s",
               "fe_peak_rss_mb", "fe_rss_change_mb", "host_load1_median")
    comparison = []
    scenarios = SCENARIOS + (("cancel_before_read",) if args.planning_before else ())
    scenarios += ("limit10_steady",) if args.steady_before else ()
    for scenario in scenarios:
        entry = {"scenario": scenario}
        for version in ("before", "after"):
            records = groups[(version, scenario)]
            require(bool(records), "No samples for " + version + "/" + scenario)
            entry[version] = dict(samples=len(records), **{
                metric: {"median": statistics.median(row[metric] for row in records),
                         "min": min(row[metric] for row in records),
                         "max": max(row[metric] for row in records)} for metric in metrics})
        before, after = (entry[version]["elapsed_ms"]["median"] for version in ("before", "after"))
        entry["query_latency_reduction_percent"] = 100 * (before - after) / before if before else None
        before, after = (entry[version]["return_after_signal_ms"]["median"] for version in ("before", "after"))
        entry["cancel_response_reduction_percent"] = 100 * (before - after) / before if before else None
        comparison.append(entry)
    result = {"status": "PASS", "comparison": comparison, "cases": all_cases,
              "notes": ["Real physical files, zero artificial delay, no Java agent; both versions use the same proxy.",
                        "Cold means refreshed catalog metadata, not a cold OS page cache or JVM.",
                        "CPU and RSS use external /proc sampling every 100 ms; RSS is not retained Java heap.",
                        "Full SUM covers 100 partitions and 10000 files; early-stop queries select 10000 partitions."]}
    result["physical_dataset"] = physical_evidence(
        json.loads((args.output / "fixture.json").read_text()),
        json.loads((args.output / "hms-fixture.json").read_text()),
        json.loads((args.output / "physical-manifest.json").read_text()))
    (args.output / "performance-results.json").write_text(json.dumps(result, indent=2) + "\n")
    for item in comparison:
        print("%s: before %.0f ms, after %.0f ms; LIST %.0f -> %.0f" % (
            item["scenario"], item["before"]["elapsed_ms"]["median"], item["after"]["elapsed_ms"]["median"],
            item["before"]["list_total"]["median"], item["after"]["list_total"]["median"]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action", required=True)
    execute = subparsers.add_parser("run")
    execute.add_argument("--output", type=Path, required=True)
    execute.add_argument("--label", required=True)
    execute.add_argument("--config", type=Path, required=True)
    execute.add_argument("--fe-dir", type=Path, default=Path("output/fe"))
    execute.add_argument("--be-dir", type=Path, default=Path("output/be"))
    execute.add_argument("--be2-dir", type=Path, default=Path("output/be-split-followup"))
    execute.add_argument("--generate", action="store_true")
    report = subparsers.add_parser("report")
    report.add_argument("--output", type=Path, required=True)
    report.add_argument("--before", nargs="+", required=True)
    report.add_argument("--after", nargs="+", required=True)
    report.add_argument("--repetitions", type=int, default=3)
    report.add_argument("--planning-before", help="Optional cancel-before-file-read baseline pass")
    report.add_argument("--planning-after", help="Optional cancel-before-file-read changed-version pass")
    report.add_argument("--steady-before", help="Optional consecutive warm LIMIT baseline pass")
    report.add_argument("--steady-after", help="Optional consecutive warm LIMIT changed-version pass")
    args = parser.parse_args()
    if args.action == "run":
        run(args)
    else:
        summarize(args)


if __name__ == "__main__":
    main()
