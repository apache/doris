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

"""Offline analysis of opt-in READ_IO_TRACE records. All lengths are bytes."""

import argparse
import collections
import gzip
import itertools
import json
import sys


SOURCES = ("foreground", "hole_fill", "other")


def source_group(event):
    source = event["source"]
    return "foreground" if source in ("read_ahead", "sync") else source


def load_events(paths):
    """Read whole log files, retaining sequence continuity checks before query filtering."""
    events = {}
    warnings = []
    for path in paths:
        opener = gzip.open if path.endswith(".gz") else open
        with opener(path, "rt", encoding="utf-8") as stream:
            for number, line in enumerate(stream, 1):
                if "READ_IO_TRACE_LIMIT " in line:
                    warnings.append("Trace event limit reached; capture is incomplete")
                marker = "READ_IO_TRACE "
                if marker not in line:
                    continue
                try:
                    event = json.loads(line.split(marker, 1)[1])
                    if event["v"] != 1 or source_group(event) not in SOURCES:
                        raise ValueError("unsupported trace version or source")
                    key = (event["process"], event["seq"])
                    if key in events and events[key] != event:
                        raise ValueError("conflicting records for the same process/sequence")
                    events[key] = event  # rotated/symlinked log copies must not count twice
                except (ValueError, KeyError) as error:
                    raise ValueError(f"{path}:{number}: {error}") from error
    sequences = collections.defaultdict(list)
    for process, sequence in events:
        sequences[process].append(sequence)
    for process, values in sequences.items():
        missing = max(values) - min(values) + 1 - len(values)
        if missing:
            warnings.append(f"{process}: {missing} missing trace events inside capture")
    if not events:
        warnings.append("No trace events found")
    return list(events.values()), sorted(set(warnings))


def overlap_example(left, right, foreground, hole, lifecycle):
    if foreground["time_ns"] <= hole["start_ns"]:
        order = "foreground_completed_before_hole_get"
    elif hole["time_ns"] <= foreground["start_ns"]:
        order = "hole_completed_before_foreground_get"
    else:
        order = "overlapping_get_lifetimes"
    range_id, task_id = foreground["parent_id"], hole["parent_id"]
    relevant = [
        event for event in lifecycle
        if (event["event"] == "range_writeback" and range_id and event["id"] == range_id)
        or (event["event"] in ("hole_queued", "hole_active", "hole_done")
            and task_id and event["id"] == task_id)
        or (event["event"] == "fragment_ignored_active" and task_id
            and event["id"] == task_id and event["parent_id"] == range_id)
    ]
    fields = ("event", "id", "parent_id", "offset", "size", "time_ns", "outcome")
    return {
        "file": foreground["file"],
        "overlap": [left, right],
        "bytes": right - left,
        "order": order,
        "foreground_get": {key: foreground[key] for key in (
            "id", "parent_id", "source", "offset", "size", "start_ns", "time_ns")},
        "hole_get": {key: hole[key] for key in (
            "id", "parent_id", "offset", "size", "start_ns", "time_ns")},
        "lifecycle": [{key: event[key] for key in fields}
                      for event in sorted(relevant, key=lambda item: (item["time_ns"], item["seq"]))],
    }


def summarize_query(events, max_examples=5):
    """Sweep interval endpoints: O(N log N), no pairwise range comparisons.

    Duplicate bytes count each extra successful transfer once. Within-source duplication plus
    cross-source duplication partitions that total even for three or more overlapping GETs.
    """
    successful = [event for event in events if event["event"] == "s3_get"
                  and event["outcome"] == "success"]
    failed = [event for event in events if event["event"] == "s3_get"
              and event["outcome"] != "success"]
    by_file = collections.defaultdict(list)
    lifecycle = collections.defaultdict(list)
    for event in successful:
        if event["bytes"] != event["size"] or event["size"] <= 0:
            raise ValueError("success GET must have positive size and a full response")
        by_file[event["file"]].append(event)
    for event in events:
        if event["event"] != "s3_get":
            lifecycle[event["file"]].append(event)
    result = {
        "successful_get_requests": len(successful),
        "unlinked_async_get_requests": sum(event["source"] in ("read_ahead", "hole_fill")
                                            and event["parent_id"] == 0 for event in successful),
        "successful_get_bytes": sum(event["bytes"] for event in successful),
        "unique_bytes": 0,
        "duplicate_bytes": 0,
        "within_source_duplicate_bytes": {source: 0 for source in SOURCES},
        "cross_source_duplicate_bytes": 0,
        "foreground_hole_fill_shared_bytes": 0,
        "failed_or_short_get_attempts": len(failed),
        "failed_or_short_reported_bytes_excluded": sum(event["bytes"] for event in failed),
        "event_counts": dict(collections.Counter(event["event"] for event in events)),
        "hole_outcomes": dict(collections.Counter(event["outcome"] for event in events
                                                if event["event"] == "hole_done")),
        "remote_miss_available_bytes": sum(event["available_bytes"] for event in events
                                            if event["event"] == "remote_miss_coverage"),
        "examples": [],
    }
    bytes_by_source = collections.Counter()
    for event in successful:
        bytes_by_source[event["source"]] += event["bytes"]
    result["successful_bytes_by_source"] = dict(bytes_by_source)
    for file, reads in by_file.items():
        endpoints = []
        for index, read in enumerate(reads):
            endpoints.append((read["offset"], 1, index))
            endpoints.append((read["offset"] + read["bytes"], -1, index))
        active = {source: {} for source in SOURCES}
        previous = 0
        example_pairs = set()
        for position, changes in itertools.groupby(sorted(endpoints), key=lambda item: item[0]):
            length = position - previous
            counts = {source: len(ids) for source, ids in active.items()}
            present = sum(count > 0 for count in counts.values())
            if length and present:
                result["unique_bytes"] += length
                for source, count in counts.items():
                    result["within_source_duplicate_bytes"][source] += max(0, count - 1) * length
                result["cross_source_duplicate_bytes"] += (present - 1) * length
                if counts["foreground"] and counts["hole_fill"]:
                    result["foreground_hole_fill_shared_bytes"] += length
                    if len(result["examples"]) < max_examples:
                        foreground_index = next(iter(active["foreground"]))
                        hole_index = next(iter(active["hole_fill"]))
                        pair = (foreground_index, hole_index)
                        if pair not in example_pairs:
                            example_pairs.add(pair)
                            result["examples"].append(overlap_example(
                                previous, position, reads[foreground_index], reads[hole_index],
                                lifecycle[file]))
            for _, delta, index in changes:
                group = active[source_group(reads[index])]
                if delta > 0:
                    group[index] = None
                else:
                    del group[index]
            previous = position
    result["duplicate_bytes"] = result["successful_get_bytes"] - result["unique_bytes"]
    assert result["duplicate_bytes"] == (
        sum(result["within_source_duplicate_bytes"].values())
        + result["cross_source_duplicate_bytes"])
    return result


def analyze(events, query_id=None, max_examples=5, expected_s3_bytes=None, warnings=()):
    groups = collections.defaultdict(list)
    for event in events:
        if query_id is None or event["query"] == query_id:
            groups[event["process"], event["query"]].append(event)
    queries = []
    for (process, query), items in sorted(groups.items()):
        queries.append({"process": process, "query": query,
                        **summarize_query(items, max_examples)})
    captured = sum(item["successful_get_bytes"] for item in queries)
    warnings = list(warnings)
    if not groups:
        warnings.append("No records match the requested query")
    reconciled = None
    if expected_s3_bytes is not None:
        reconciled = captured == expected_s3_bytes
        if not reconciled:
            warnings.append(f"S3 byte reconciliation failed: trace={captured}, "
                            f"expected={expected_s3_bytes}")
    return {
        "units": "bytes and monotonic nanoseconds; half-open intervals",
        "scope": "per BE process + query + immutable object URI; queries are never unioned",
        "capture_warnings": warnings,
        "s3_bytes_reconciled": reconciled,
        "captured_successful_s3_bytes": captured,
        "queries": queries,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="+", help="complete be.INFO log files, optionally .gz")
    parser.add_argument("--query-id", help="exact query field from the trace")
    parser.add_argument("--examples", type=int, default=5, help="maximum overlap examples per query")
    parser.add_argument("--expected-s3-bytes", type=int,
                        help="successful S3 byte delta over the same isolated, drained capture")
    args = parser.parse_args()
    if args.examples < 0:
        parser.error("--examples must be nonnegative")
    try:
        events, warnings = load_events(args.logs)
        report = analyze(events, args.query_id, args.examples, args.expected_s3_bytes, warnings)
    except (OSError, ValueError, KeyError) as error:
        parser.error(str(error))
    json.dump(report, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")
    return 2 if report["capture_warnings"] else 0


if __name__ == "__main__":
    sys.exit(main())
