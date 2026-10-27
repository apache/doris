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
import tempfile
import unittest
from collections import defaultdict
from pathlib import Path

from performance import MILLION_SCENARIO, SCENARIOS, RequestLog, measured_cases


class PerformanceTest(unittest.TestCase):
    def inputs(self):
        observations, events = [], []
        samples = defaultdict(list)
        for index, name in enumerate(SCENARIOS):
            begin, returned = 1000 + index * 20000, 1050 + index * 20000
            if name == "timeout_sum":
                returned = begin + 3050
            end = returned + (1000 if name.startswith("full_") else 10000)
            cached = name.endswith("_cached")
            observation = dict(run="before", scenario=name, repetition=0, phase=name, begin_ms=begin,
                               return_ms=returned, signal_ms=begin + 30, observed_ms=end, elapsed_ms=returned - begin,
                               counts={"LIST": 0 if cached else 1})
            observations.append(observation)
            if not cached:
                events.append(dict(phase=name, time_ms=begin + 10, action="LIST", synthetic_listing=False))
            events.append(dict(phase=name, time_ms=begin + 20, action="GET"))
            for role in ("fe", "be", "be2"):
                for when, cpu in ((begin - 1, index + 1), (returned, index + 1.05), (end + 1, index + 1.07)):
                    samples[role].append(dict(time_ms=when, cpu_seconds=cpu, rss_bytes=1024 ** 3, host_load1=1))
        return observations, events, samples

    def analyze(self, observations, events, samples):
        log = "All suites success.\n" + "\n".join("SPLIT_PERF " + json.dumps(item) for item in observations)
        return measured_cases(log, "before", 1, events, samples)

    def test_cpu_windows_and_real_requests_are_measured(self):
        cases = self.analyze(*self.inputs())
        self.assertAlmostEqual(.05, cases[0]["fe_query_cpu_s"])
        self.assertAlmostEqual(.02, cases[0]["fe_post_return_cpu_s"])
        self.assertEqual(1, cases[0]["list_total"])
        self.assertEqual(0, cases[-1]["list_total"])

    def test_missing_scenario_cannot_be_a_comparison(self):
        observations, events, samples = self.inputs()
        with self.assertRaisesRegex(ValueError, "missing, repeated"):
            self.analyze(observations[:-1], events, samples)

    def test_synthetic_requests_cannot_be_physical_performance(self):
        observations, events, samples = self.inputs()
        events[0]["synthetic_listing"] = True
        with self.assertRaisesRegex(ValueError, "Synthetic"):
            self.analyze(observations, events, samples)

    def test_cache_hit_requires_no_new_listing(self):
        observations, events, samples = self.inputs()
        observations[-1]["counts"]["LIST"] = 1
        events.append(dict(phase=observations[-1]["phase"], time_ms=observations[-1]["begin_ms"] + 10,
                           action="LIST", synthetic_listing=False))
        with self.assertRaisesRegex(ValueError, "cache-hit query missed"):
            self.analyze(observations, events, samples)

    def test_cancel_must_follow_a_real_file_read(self):
        observations, events, samples = self.inputs()
        events = [event for event in events if event["phase"] != "cancel_sum" or event["action"] != "GET"]
        with self.assertRaisesRegex(ValueError, "did not reach BE"):
            self.analyze(observations, events, samples)

    def test_stale_process_samples_cannot_claim_a_cpu_reduction(self):
        observations, events, samples = self.inputs()
        samples["fe"] = []
        with self.assertRaisesRegex(ValueError, "did not cover"):
            self.analyze(observations, events, samples)

    def test_pre_read_cancellation_requires_listing_activity_without_reads(self):
        observations, events, samples = self.inputs()
        observation = dict(observations[0], scenario="cancel_before_read", phase="planning", counts={"LIST": 8})
        listings = [dict(phase="planning", action="LIST", time_ms=1010 + i, synthetic_listing=False)
                    for i in range(8)]
        log = "All suites success.\nSPLIT_PERF " + json.dumps(observation)
        cases = measured_cases(log, "before", 1, listings, samples, ("cancel_before_read",))
        self.assertEqual(20, cases[0]["return_after_signal_ms"])
        listings.append(dict(phase="planning", action="GET", time_ms=1020))
        with self.assertRaisesRegex(ValueError, "did not occur before"):
            measured_cases(log, "before", 1, listings, samples, ("cancel_before_read",))

    def test_indexed_log_matches_in_memory_analysis_with_interleaved_phases(self):
        observations, events, samples = self.inputs()
        # A delayed worker can append a previous phase after a new phase starts.
        events.append(dict(events[0], time_ms=observations[0]["return_ms"] + 11000))
        log = "All suites success.\n" + "\n".join("SPLIT_PERF " + json.dumps(item) for item in observations)
        expected = measured_cases(log, "before", 1, events, samples)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "requests.jsonl"
            path.write_text("".join(json.dumps(event) + "\n" for event in events))
            indexed = RequestLog(path)
            self.assertEqual([], list(indexed.for_phase("absent")))
            actual = measured_cases(log, "before", 1, indexed, samples)
        self.assertEqual(expected, actual)
        self.assertEqual(11000, actual[0]["last_list_after_return_until_idle_ms"])
        self.assertEqual(1, actual[0]["file_requests_after_exit_deadline"])

    def test_unrelated_early_timeout_cannot_count_as_a_query_deadline(self):
        observations, events, samples = self.inputs()
        next(item for item in observations if item["scenario"] == "timeout_sum")["elapsed_ms"] = 1000
        with self.assertRaisesRegex(ValueError, "before its three-second"):
            self.analyze(observations, events, samples)

    def test_million_file_comparison_requires_the_additional_full_scan(self):
        observations, events, samples = self.inputs()
        observation = dict(observations[-1], scenario=MILLION_SCENARIO, phase="million", counts={"LIST": 1})
        observations.append(observation)
        events.extend([dict(phase="million", time_ms=observation["begin_ms"] + 10,
                            action="LIST", synthetic_listing=False),
                       dict(phase="million", time_ms=observation["begin_ms"] + 20, action="GET")])
        log = "All suites success.\n" + "\n".join("SPLIT_PERF " + json.dumps(item) for item in observations)
        cases = measured_cases(log, "before", 1, events, samples, million_file_scan=True)
        self.assertEqual(MILLION_SCENARIO, cases[-1]["scenario"])
        self.assertEqual(1, cases[-1]["get_total"])
        with self.assertRaisesRegex(ValueError, "missing, repeated"):
            measured_cases(log, "before", 1, events, samples)
        with self.assertRaisesRegex(ValueError, "missing, repeated"):
            measured_cases(log.rsplit("\n", 1)[0], "before", 1, events, samples, million_file_scan=True)


if __name__ == "__main__":
    unittest.main()
