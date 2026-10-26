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
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from report import COUNTERS, analyze, analyze_concurrent


class ReportTest(unittest.TestCase):
    def inputs(self, phase="limit"):
        observation = {"phase": phase, "stop_ms": 1000, "observed_ms": 12000}
        log = "SPLIT_RUN_START 10\nSPLIT_STOP " + json.dumps(observation)
        rows = []
        for when in (100, 1100, 12000):
            row = dict.fromkeys(COUNTERS, 0)
            row.update(time_ms=when, assignment_id=1, query_id="test-query",
                       stopped=when > 100, finished=True, terminated=when > 100)
            rows.append(row)
        rows[0]["live_producers"] = 1
        events = [{"phase": phase, "time_ms": 900, "action": "GET"},
                  {"phase": phase, "time_ms": 1050, "action": "LIST"}]
        return log, {1: rows}, events

    def test_sdk_call_can_finish_inside_the_grace_period(self):
        result = analyze(*self.inputs(), expected_phases=("limit",))
        self.assertEqual(100, result["cases"][0]["producer_exit_delay_ms"])

    def test_synthetic_or_unrecorded_listings_are_not_physical_evidence(self):
        for synthetic in (True, False, None):
            with self.subTest(synthetic=synthetic):
                log, samples, events = self.inputs()
                if synthetic is not None:
                    events[1]["synthetic_listing"] = synthetic
                result = analyze(log, samples, events, ("limit",))
                self.assertEqual(0 if synthetic is False else 1,
                                 result["cases"][0]["synthetic_directory_requests"])

    def test_retained_buffers_are_not_successful_termination(self):
        for counter in COUNTERS:
            with self.subTest(counter=counter):
                log, samples, events = self.inputs()
                samples[1][-1][counter] = 1
                with self.assertRaisesRegex(ValueError, "never became quiescent"):
                    analyze(log, samples, events, ("limit",))

    def test_late_file_requests_fail_even_after_producer_termination(self):
        log, samples, events = self.inputs()
        events.append({"phase": "limit", "time_ms": 11001, "action": "LIST"})
        with self.assertRaisesRegex(ValueError, "new file requests"):
            analyze(log, samples, events, ("limit",))

    def test_dead_observer_cannot_claim_success(self):
        log, samples, events = self.inputs()
        samples[1].pop()
        with self.assertRaisesRegex(ValueError, "observer stopped"):
            analyze(log, samples, events, ("limit",))

    def test_cancel_must_happen_after_be_started_reading(self):
        log, samples, events = self.inputs("cancel")
        events[0]["time_ms"] = 1050
        with self.assertRaisesRegex(ValueError, "BE never read"):
            analyze(log, samples, events, ("cancel",))

    def test_missing_scenario_is_not_a_pass(self):
        with self.assertRaisesRegex(ValueError, "Missing, repeated"):
            analyze(*self.inputs())

    def test_resources_that_reactivate_after_ten_seconds_fail(self):
        log, samples, events = self.inputs()
        busy = dict(samples[1][-1], time_ms=11100, live_producers=1)
        samples[1].insert(-1, busy)
        with self.assertRaisesRegex(ValueError, "within 10 seconds"):
            analyze(log, samples, events, ("limit",))

    def test_an_ambiguous_assignment_is_not_a_pass(self):
        log, samples, events = self.inputs()
        samples[2] = [dict(row, assignment_id=2) for row in samples[1]]
        with self.assertRaisesRegex(ValueError, "expected one assignment"):
            analyze(log, samples, events, ("limit",))

    def concurrent_inputs(self):
        _, samples, _ = self.inputs()
        for row in samples[1]:
            row["query_id"] = "0000000000000001-0000000000000002"
        samples[2] = [dict(row, assignment_id=2, query_id="0000000000000003-0000000000000004")
                      for row in samples[1]]
        observation = {"phase": "cache", "stop_ms": 1000, "observed_ms": 12000,
                       "cancelled_query_id": "1-2", "survivor_query_id": "3-4",
                       "survivor_return_ms": 2000, "directory_requests": 2}
        events = [{"phase": "cache", "time_ms": when, "action": "LIST", "path": "/bucket?prefix=data/p=0/"}
                  for when in (500, 600)]
        events.append({"phase": "cache", "time_ms": 1500, "action": "GET"})
        return "SPLIT_CONCURRENT " + json.dumps(observation), samples, events

    def test_cancelled_reader_does_not_fail_the_survivor(self):
        self.assertEqual("PASS", analyze_concurrent(*self.concurrent_inputs())["status"])

    def test_both_concurrent_readers_must_release_resources(self):
        for assignment in (1, 2):
            with self.subTest(assignment=assignment):
                log, samples, events = self.concurrent_inputs()
                samples[assignment][-1]["resources"] = 1
                with self.assertRaisesRegex(ValueError, "never became quiescent"):
                    analyze_concurrent(log, samples, events)

    def test_shared_cache_test_must_read_the_same_directory(self):
        log, samples, events = self.concurrent_inputs()
        events[1]["path"] = "/bucket?prefix=data/p=1/"
        with self.assertRaisesRegex(ValueError, "same directory"):
            analyze_concurrent(log, samples, events)

    def test_shared_cache_entry_must_be_reused(self):
        log, samples, events = self.concurrent_inputs()
        events.append(dict(events[0], time_ms=3000))
        with self.assertRaisesRegex(ValueError, "not reused"):
            analyze_concurrent(log, samples, events)

    def test_failed_rerun_removes_the_old_pass_artifact(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[3] / "output") as directory:
            root = Path(directory)
            (root / "results.json").write_text('{"status":"PASS"}')
            (root / "regression.log").write_text("Some suites failed.")
            result = subprocess.run([sys.executable, str(Path(__file__).with_name("report.py")),
                                     "--output", str(root)], capture_output=True, text=True)
            self.assertNotEqual(0, result.returncode)
            self.assertFalse((root / "results.json").exists())


if __name__ == "__main__":
    unittest.main()
