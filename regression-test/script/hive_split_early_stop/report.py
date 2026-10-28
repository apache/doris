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

"""Validate early-stop deadlines, request cessation and explicit resource disposal."""

import argparse
import csv
import json
import re
from collections import defaultdict
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

PHASES = ("limit", "cancel", "planning_cancel", "timeout", "planning_timeout",
          "first_split_timeout_1000", "first_split_timeout_2500")
COUNTERS = ("queue_batches", "queued_ranges", "listeners", "resources", "active_runs", "live_producers")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def quiescent(row):
    return row["stopped"] and row["terminated"] and all(row[key] == 0 for key in COUNTERS)


def query_key(query):
    # SHOW PROCESSLIST omits leading zeroes; the observer writes fixed-width hexadecimal halves.
    require(re.fullmatch(r"[0-9a-fA-F]{1,16}-[0-9a-fA-F]{1,16}", query) is not None,
            "Invalid query id: " + query)
    return tuple(int(part, 16) for part in query.split("-"))


def physical_evidence(fixture, hms, manifest):
    require(fixture["mode"] == "physical_proxy" and manifest["mode"] == "physical_files"
            and manifest["status"] == "VERIFIED", "Missing verified physical dataset evidence")
    require(all(manifest[key] == hms[key] for key in ("partitions", "files_per_partition", "file_bytes"))
            and manifest["verified_objects"] == hms["physical_files"]
            == hms["partitions"] * hms["files_per_partition"]
            and manifest["bucket"] == fixture["bucket"]
            and manifest["endpoint"] == fixture["upstream"], "Physical evidence does not match the tested dataset")
    return manifest


def read_samples(path):
    samples = defaultdict(list)
    with path.open() as source:
        for row in csv.DictReader(source):
            for key in ("time_ms", "assignment_id") + COUNTERS:
                row[key] = int(row[key])
            for key in ("stopped", "finished", "terminated"):
                require(row[key] in ("true", "false"), "Invalid observer boolean: " + key)
                row[key] = row[key] == "true"
            samples[row["assignment_id"]].append(row)
    return samples


def analyze(log, samples, events, expected_phases=PHASES):
    start = re.search(r"SPLIT_RUN_START (\d+)", log)
    require(start is not None, "Missing SPLIT_RUN_START marker")
    previous = int(start.group(1))
    observations = [json.loads(line.split("SPLIT_STOP ", 1)[1]) for line in log.splitlines()
                    if "SPLIT_STOP {" in line]
    require(tuple(item["phase"] for item in observations) == expected_phases,
            "Missing, repeated or out-of-order stop scenarios")
    cases = []
    for observation in observations:
        phase, signal, end = (observation[key] for key in ("phase", "stop_ms", "observed_ms"))
        require(previous < signal and signal + 11000 <= end, phase + ": incomplete observation window")
        candidates = [rows for rows in samples.values() if previous < rows[0]["time_ms"] <= signal + 1000]
        require(len(candidates) == 1, "%s: expected one assignment, got %d" % (phase, len(candidates)))
        rows = [row for row in candidates[0] if row["time_ms"] <= end + 100]
        require(bool(rows), phase + ": no assignment samples")
        require(rows[-1]["time_ms"] >= end - 1000, phase + ": observer stopped before the observation ended")
        last_busy = max((i for i, row in enumerate(rows) if not quiescent(row)), default=-1)
        require(last_busy + 1 < len(rows), phase + ": resources never became quiescent")
        quiet = rows[last_busy + 1]
        require(quiet["time_ms"] <= signal + 10000, phase + ": producers did not terminate within 10 seconds")
        relevant = [event for event in events if event["phase"] == phase
                    and previous <= event["time_ms"] <= end]
        requests = [event["time_ms"] for event in relevant if event["action"] == "LIST"]
        synthetic = sum(event.get("synthetic_listing", True) for event in relevant if event["action"] == "LIST")
        late = sum(when >= signal + 10000 for when in requests)
        require(late == 0, phase + ": new file requests after the 10-second deadline")
        reads = sum(event["action"] == "GET" and event["time_ms"] <= signal for event in relevant)
        if phase in ("cancel", "timeout"):
            require(reads > 0, phase + ": BE never read a file before the stop signal")
        if phase in ("planning_cancel", "planning_timeout"):
            require(reads == 0, phase + ": BE already read a file before the first-Split stop signal")
        cases.append({"phase": phase, "query_id": rows[0]["query_id"], "signal_ms": signal,
                      "producer_exit_delay_ms": max(0, quiet["time_ms"] - signal),
                      "last_file_request_delay_ms": max(requests) - signal if requests else None,
                      "file_requests_after_10s": late, "file_requests_total": len(requests),
                      "synthetic_directory_requests": synthetic,
                      "file_reads_before_stop": reads, "final_snapshot": rows[-1]})
        previous = end
    waits = [{"configured_ms": int(a), "observed_ms": int(b)} for a, b in
             re.findall(r"FIRST_SPLIT_TIMEOUT configured_ms=(\d+) elapsed_ms=(\d+)", log)]
    expected_waits = [int(phase.rsplit("_", 1)[-1]) for phase in expected_phases
                      if phase.startswith("first_split_timeout_")]
    require([item["configured_ms"] for item in waits] == expected_waits, "Missing timeout timing evidence")
    for item in waits:
        require(item["configured_ms"] <= item["observed_ms"] < item["configured_ms"] + 5000,
                "First-Split timeout did not honor its configured deadline")
        progress = (r"first split after %d ms: partitions_processed=\d+/(?:\d+|unknown), "
                    r"files_enumerated=\d+, splits_generated=\d+, active_producers=\d+"
                    % item["configured_ms"])
        require(re.search(progress, log) is not None, "First-Split timeout has no progress details")
    return {"cases": cases, "first_split_wait": waits}


def analyze_concurrent(log, samples, events):
    observations = [json.loads(line.split("SPLIT_CONCURRENT ", 1)[1]) for line in log.splitlines()
                    if "SPLIT_CONCURRENT {" in line]
    require(len(observations) == 1, "Missing or repeated concurrent-cache observation")
    observation = observations[0]
    signal, end = observation["stop_ms"], observation["observed_ms"]
    require(end >= signal + 11000, "Incomplete concurrent-cache observation window")
    results = {}
    begin = signal
    for role in ("cancelled", "survivor"):
        query = observation[role + "_query_id"]
        candidates = [rows for rows in samples.values() if query_key(rows[0]["query_id"]) == query_key(query)]
        require(len(candidates) == 1, role + ": expected one observed assignment")
        rows = [row for row in candidates[0] if row["time_ms"] <= end + 100]
        require(rows and rows[-1]["time_ms"] >= end - 1000, role + ": observer stopped early")
        last_busy = max((i for i, row in enumerate(rows) if not quiescent(row)), default=-1)
        require(last_busy + 1 < len(rows), role + ": resources never became quiescent")
        quiet = rows[last_busy + 1]
        if role == "cancelled":
            require(quiet["time_ms"] <= signal + 10000, "Cancelled shared-cache reader did not exit in 10 seconds")
        begin = min(begin, rows[0]["time_ms"])
        results[role] = {"query_id": query, "producer_exit_delay_ms": max(0, quiet["time_ms"] - signal),
                         "final_snapshot": rows[-1]}
    relevant = [event for event in events if event["phase"] == observation["phase"]
                and begin - 1000 <= event["time_ms"] <= end]
    listings = [event for event in relevant if event["action"] == "LIST"]
    require(len(listings) >= 2, "Concurrent readers did not issue independent loads")
    prefixes = {parse_qs(urlsplit(event["path"]).query).get("prefix", [""])[0] for event in listings}
    require(len(prefixes) == 1, "Concurrent readers did not load the same directory")
    require(len(listings) == observation["directory_requests"], "The completed cache entry was not reused")
    require(any(event["action"] == "GET" and event["time_ms"] <= observation["survivor_return_ms"]
                for event in relevant), "Surviving query never read a file")
    return {"status": "PASS", "directory_requests": len(listings), "queries": results,
            "control": "Both SDK listings are held at a test barrier, released immediately after cancellation."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="Directory containing fixture and probe outputs")
    parser.add_argument("--log", type=Path, help="Regression log; defaults to OUTPUT/regression.log")
    parser.add_argument("--concurrency-log", type=Path, help="Optional shared-cache cancellation regression log")
    parser.add_argument("--registry", type=Path, help="Optional callback registry CSV captured after all queries")
    args = parser.parse_args()
    root = args.output
    # A failed rerun must not leave an earlier PASS artifact looking like its own result.
    (root / "results.json").unlink(missing_ok=True)
    log = (args.log or root / "regression.log").read_text()
    require("All suites success." in log, "Regression runner did not report success")
    with (root / "s3-requests.jsonl").open() as source:
        events = [json.loads(line) for line in source]
    report = analyze(log, read_samples(root / "split-probe.csv"), events)
    report.update(status="PASS", fixture=json.loads((root / "fixture.json").read_text()),
                  hms=json.loads((root / "hms-fixture.json").read_text()),
                  backends=json.loads((root / "backends.json").read_text()),
                  observation="Test-only observer retains assignments; explicit disposal is checked independently of GC.")
    if report["hms"]["mode"] == "physical_files":
        report["physical_dataset"] = physical_evidence(
            report["fixture"], report["hms"], json.loads((root / "physical-manifest.json").read_text()))
        require(all(case["synthetic_directory_requests"] == 0 for case in report["cases"]),
                "Physical acceptance used synthetic or unverified directory listings")
        report["first_split_wait_control"] = "Real upstream LIST responses are delayed before delivery to FE."
    if args.concurrency_log:
        concurrent_log = args.concurrency_log.read_text()
        require("All suites success." in concurrent_log, "Concurrent-cache regression did not report success")
        report["concurrent_cache"] = analyze_concurrent(
            concurrent_log, read_samples(root / "split-probe.csv"), events)
    if args.registry:
        with args.registry.open() as source:
            require(not list(csv.DictReader(source)), "Query-finish callbacks remain registered")
        report["query_finish_callback_registry_empty"] = True
    (root / "results.json").write_text(json.dumps(report, indent=2) + "\n")
    for case in report["cases"]:
        print("%s: exit=%d ms, late LISTs=%d, retained ranges=%d" % (
            case["phase"], case["producer_exit_delay_ms"], case["file_requests_after_10s"],
            case["final_snapshot"]["queued_ranges"]))


if __name__ == "__main__":
    main()
