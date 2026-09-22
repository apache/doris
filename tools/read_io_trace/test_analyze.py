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

import json
import os
import random
import tempfile
import unittest

from analyze import analyze, distribution, load_events, summarize_query


def get(seq, source, offset, size, start=10, end=20, query="q", file="s3://b/f",
        process="be1", outcome="success", parent=0):
    return {
        "v": 1, "process": process, "seq": seq, "event": "s3_get",
        "source": source, "query": query, "file": file, "id": seq,
        "parent_id": parent, "offset": offset, "size": size, "bytes": size,
        "time_ns": end, "start_ns": start, "status": 0, "attempt": 0,
        "outcome": outcome, "available_bytes": 0,
    }


class ReadIOTraceAnalysisTest(unittest.TestCase):
    def test_timing_distribution_and_nested_stage_boundaries(self):
        self.assertEqual(distribution([]), {"count": 0})
        self.assertEqual(distribution(range(1, 101)),
                         {"count": 100, "sum": 5050, "min": 1, "p50": 50,
                          "p95": 95, "p99": 99, "max": 100})
        events = []
        for seq, outcome, end in [(1, "queued", 70), (2, "merged", 90)]:
            events.append({**get(seq, "read_ahead", 0, 10, start=10, end=end),
                           "event": "hole_submit", "outcome": outcome,
                           "details": {"queue_lock_wait_ns": 20,
                                       "fragment_lock_hold_ns": 30, "copy_ns": 10,
                                       "copied_bytes": 10, "queue_size": 100}})
        result = summarize_query(events)
        timing = result["timings"]["hole_submit"]
        self.assertEqual(timing["duration_ns"]["sum"], 140)
        self.assertEqual(timing["details"]["copy_ns"]["sum"], 20)
        self.assertEqual(timing["details"]["fragment_lock_hold_ns"]["sum"], 60)
        self.assertEqual(timing["slowest"][0]["id"], 2)
        self.assertEqual(result["timings"]["hole_submit_by_outcome"]["merged"]
                         ["duration_ns"]["count"], 1)
        self.assertEqual(result["timings"]["range_writeback"]["duration_ns"], {"count": 0})
        self.assertEqual(result["successful_get_bytes"], 0)

    def test_queue_scans_keep_process_scope_with_query_filter(self):
        events = [get(1, "read_ahead", 0, 10, start=10, end=100)]
        for seq, process, start, end, size in [
            (2, "be1", 20, 30, 1000), (3, "be1", 200, 210, 5),
            (1, "be2", 20, 30, 1024), (4, "be1", 5, 15, 32),
        ]:
            events.append({**get(seq, "other", 0, 0, start, end, query="unknown", process=process),
                           "event": "hole_queue_scan", "outcome": "no_runnable_task",
                           "details": {"queue_size": size, "scanned": size, "delayed": size,
                                       "capacity_waits": 0, "discarded": 0}})
        result = analyze(events, query_id="q")
        self.assertEqual(len(result["queries"]), 1)
        self.assertEqual(result["captured_successful_s3_bytes"], 10)
        self.assertEqual(len(result["process_diagnostics"]), 1)
        scans = result["process_diagnostics"][0]["hole_queue_scan"]
        self.assertEqual(scans["duration_ns"]["count"], 2)
        self.assertEqual(scans["details"]["scanned"]["sum"], 1032)
        self.assertEqual(scans["by_queue_size"]["256-1023"]["duration_ns"]["count"], 1)
        self.assertEqual(scans["by_queue_size"]["0-63"]["duration_ns"]["count"], 1)

    def test_old_capture_has_no_submit_or_scan_timing_samples(self):
        result = analyze([get(1, "read_ahead", 0, 10)])
        self.assertEqual(result["queries"][0]["timings"]["hole_submit"]["duration_ns"], {"count": 0})
        self.assertEqual(result["process_diagnostics"][0]["hole_queue_scan"]["duration_ns"], {"count": 0})
        self.assertEqual(result["queries"][0]["timings"]["successful_get_by_source"]
                         ["read_ahead"]["duration_ns"]["sum"], 10)

    def test_three_reads_are_not_pairwise_double_counted(self):
        events = [get(1, "read_ahead", 0, 1024),
                  get(2, "hole_fill", 128, 896),
                  get(3, "hole_fill", 128, 896)]
        summary = summarize_query(events)
        self.assertEqual(summary["successful_get_bytes"], 2816)
        self.assertEqual(summary["unique_bytes"], 1024)
        self.assertEqual(summary["duplicate_bytes"], 1792)
        self.assertEqual(summary["foreground_hole_fill_shared_bytes"], 896)
        self.assertEqual(summary["within_source_duplicate_bytes"]["hole_fill"], 896)

    def test_disjoint_and_adjacent_ranges_have_no_overlap(self):
        result = summarize_query([get(1, "read_ahead", 0, 10), get(2, "hole_fill", 10, 10),
                                  get(3, "read_ahead", 30, 10)])
        self.assertEqual(result["unique_bytes"], 30)
        self.assertEqual(result["duplicate_bytes"], 0)
        self.assertEqual(result["examples"], [])

    def test_failures_do_not_enter_successful_union(self):
        result = summarize_query([get(1, "hole_fill", 0, 10, outcome="failed_or_short"),
                                  get(2, "hole_fill", 0, 10)])
        self.assertEqual(result["successful_get_bytes"], 10)
        self.assertEqual(result["duplicate_bytes"], 0)
        self.assertEqual(result["failed_or_short_get_attempts"], 1)

    def test_query_process_and_object_boundaries(self):
        events = [get(1, "read_ahead", 0, 10), get(2, "hole_fill", 0, 10, query="q2"),
                  get(3, "hole_fill", 0, 10, file="s3://b/f2"),
                  get(1, "hole_fill", 0, 10, process="be2")]
        result = analyze(events)
        self.assertEqual(sum(item["duplicate_bytes"] for item in result["queries"]), 0)
        self.assertEqual(result["captured_successful_s3_bytes"], 40)
        self.assertEqual(len(result["queries"]), 3)

    def test_examples_link_gets_to_consumption_and_active_ignored_fragment(self):
        events = [get(1, "read_ahead", 0, 10, start=10, end=20, parent=50),
                  get(2, "hole_fill", 0, 10, start=30, end=40, parent=60)]
        for seq, name, identity, parent, time in [
            (3, "hole_active", 60, 0, 25), (4, "range_writeback", 50, 0, 35),
            (5, "fragment_ignored_active", 60, 50, 36),
        ]:
            events.append({**get(seq, "read_ahead", 0, 10), "event": name,
                           "id": identity, "parent_id": parent, "time_ns": time, "outcome": ""})
        example = summarize_query(events)["examples"][0]
        self.assertEqual(example["order"], "foreground_completed_before_hole_get")
        self.assertEqual([item["event"] for item in example["lifecycle"]],
                         ["hole_active", "range_writeback", "fragment_ignored_active"])

    def test_concurrent_and_reverse_order(self):
        for foreground_times, expected in [
            ((10, 40), "overlapping_get_lifetimes"),
            ((40, 50), "hole_completed_before_foreground_get"),
        ]:
            events = [get(1, "read_ahead", 0, 10, *foreground_times),
                      get(2, "hole_fill", 0, 10, start=20, end=30)]
            self.assertEqual(summarize_query(events)["examples"][0]["order"], expected)

    def test_random_sweep_matches_bytewise_oracle(self):
        generator = random.Random(31)
        for _ in range(100):
            events = [get(index + 1, generator.choice(["read_ahead", "sync", "hole_fill", "other"]),
                          generator.randrange(32), generator.randrange(1, 20))
                      for index in range(50)]
            result = summarize_query(events, 0)
            coverage = [set(range(event["offset"], event["offset"] + event["bytes"]))
                        for event in events]
            union = set().union(*coverage)
            foreground = set().union(*(coverage[index] for index, event in enumerate(events)
                                       if event["source"] in ("read_ahead", "sync")))
            hole = set().union(*(coverage[index] for index, event in enumerate(events)
                                if event["source"] == "hole_fill"))
            self.assertEqual(result["unique_bytes"], len(union))
            self.assertEqual(result["duplicate_bytes"], sum(map(len, coverage)) - len(union))
            self.assertEqual(result["foreground_hole_fill_shared_bytes"], len(foreground & hole))

    def test_input_dedup_gaps_and_budget_limit_are_reported(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "be.INFO")
            with open(path, "w", encoding="utf-8") as stream:
                for event in [get(1, "sync", 0, 10), get(3, "hole_fill", 0, 10)]:
                    stream.write("I0000 READ_IO_TRACE " + json.dumps(event) + "\n")
                stream.write("W0000 READ_IO_TRACE_LIMIT max_events=3\n")
            events, warnings = load_events([path, path])
        self.assertEqual(len(events), 2)
        self.assertEqual(len(warnings), 2)
        report = analyze(events, expected_s3_bytes=25, warnings=warnings)
        self.assertFalse(report["s3_bytes_reconciled"])
        self.assertEqual(len(report["capture_warnings"]), 3)

    def test_missing_capture_cannot_be_treated_as_zero(self):
        report = analyze([], query_id="q", expected_s3_bytes=10)
        self.assertTrue(report["capture_warnings"])
        self.assertFalse(report["s3_bytes_reconciled"])

    def test_jsonl_checkpoint_and_repeated_files(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "read_io_trace.be1.0.jsonl")
            file = "s3://bucket/READ_IO_TRACE READ_IO_TRACE_LIMIT path"
            records = [get(1, "read_ahead", 0, 10, file=file),
                       get(2, "hole_fill", 0, 10, file=file),
                       {"v": 1, "kind": "read_io_trace_status", "process": "be1",
                        "written_events": 2, "dropped_events": 0}]
            with open(path, "w", encoding="utf-8") as stream:
                for record in records:
                    stream.write(json.dumps(record) + "\n")
            events, warnings = load_events([path, path])
        self.assertEqual(len(events), 2)
        self.assertEqual(warnings, [])
        self.assertEqual(summarize_query(events)["duplicate_bytes"], 10)

    def test_jsonl_detects_missing_prefix_files_and_writer_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "trace.jsonl")
            with open(path, "w", encoding="utf-8") as stream:
                stream.write(json.dumps(get(4, "hole_fill", 0, 10)) + "\n")
                stream.write(json.dumps({"v": 1, "kind": "read_io_trace_status", "process": "be1",
                                         "written_events": 3, "dropped_events": 1}) + "\n")
            events, warnings = load_events([path])
        self.assertEqual(len(events), 1)
        self.assertEqual(len(warnings), 2)
        self.assertTrue(any("lost 1 events" in warning for warning in warnings))
        self.assertTrue(any("reports 3 written events" in warning for warning in warnings))

    def test_jsonl_requires_a_complete_flush_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "trace.jsonl")
            with open(path, "w", encoding="utf-8") as stream:
                stream.write(json.dumps(get(1, "sync", 0, 10)) + "\n")
            _, warnings = load_events([path])
            self.assertTrue(any("missing flush checkpoint" in warning for warning in warnings))
            with open(path, "a", encoding="utf-8") as stream:
                stream.write(json.dumps({"v": 1, "kind": "read_io_trace_status", "process": "be1",
                                         "written_events": 1, "dropped_events": 0}) + "\n")
                stream.write(json.dumps(get(2, "sync", 10, 10)) + "\n")
            _, warnings = load_events([path])
            self.assertTrue(any("files contain 2" in warning for warning in warnings))


if __name__ == "__main__":
    unittest.main()
