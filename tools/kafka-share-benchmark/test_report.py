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

import copy
from pathlib import Path
import tempfile
import unittest

from report import reader_evidence, summarize


def evidence():
    events = [dict(event="environment", readers=[3], repetitions=3)]
    for scenario, partitions, counts in (("balanced", 3, [34, 33, 33]),
                                         ("hot95", 3, [95, 3, 2]), ("single", 1, [100])):
        for repetition in range(4):
            for mode in ("standard", "share"):
                events.append(dict(event="result", passed=True, scenario=scenario, partitions=partitions,
                                   requested_readers=3, effective_readers=3 if mode == "share" else partitions,
                                   repetition=repetition, warmup=repetition == 0, mode=mode,
                                   order=0 if mode == "standard" else 1, topic=f"{scenario}_{repetition}",
                                   expected=100, unique_rows=100, bad_rows=0, duplicates=0,
                                   producer_partition_counts=counts, seconds=1000 if repetition == 0 else
                                   (10 if mode == "standard" else 5), visible_95_percent_seconds=4))
    events.append(dict(event="complete", passed=True))
    return events


class ReportTest(unittest.TestCase):
    def test_excludes_warmup_and_counts_unique_rows_despite_duplicates(self):
        events = evidence()
        events[3]["duplicates"] = 1000
        result = summarize(events)["summary"]
        self.assertEqual(result[0]["median_unique_rows_s"], 10)
        self.assertEqual(result[0]["median_paired_share_over_classic"], 2)
        self.assertEqual(result[0]["measured_runs"], 3)
        self.assertEqual(result[0]["duplicate_rows"], 1000)

    def test_rejects_missing_completion_or_measurement(self):
        for index in (-1, 3):
            events = evidence()
            events.pop(index)
            with self.assertRaises(ValueError):
                summarize(events)

    def test_rejects_incorrect_or_incomparable_results(self):
        for field, value in (("unique_rows", 99), ("bad_rows", 1), ("passed", False),
                             ("topic", "different"), ("effective_readers", 1),
                             ("seconds", float("nan")), ("producer_partition_counts", [90, 5, 5])):
            events = copy.deepcopy(evidence())
            events[3][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                summarize(events)

    def test_rejects_matching_but_incorrect_source_skew(self):
        events = evidence()
        events[9]["producer_partition_counts"] = [90, 5, 5]
        events[10]["producer_partition_counts"] = [90, 5, 5]
        with self.assertRaises(ValueError):
            summarize(events)

    def test_reader_evidence_is_limited_to_measured_jobs_and_keeps_partitions(self):
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "be.INFO"
            log.write_text("kafka_reader_sample job=1 reader=abc mode=C partition=0 records=5\n"
                           "kafka_reader_sample job=1 reader=abc mode=C partition=0 records=3\n"
                           "kafka_reader_sample job=1 reader=abc mode=C partition=1 records=2\n"
                           "kafka_reader_sample job=2 reader=other mode=A partition=0 records=90\n")
            records = reader_evidence([log], [{"job_id": 1}])
            self.assertEqual([r["received_records"] for r in records], [8, 2])
            self.assertEqual([r["partition"] for r in records], [0, 1])


if __name__ == "__main__":
    unittest.main()
