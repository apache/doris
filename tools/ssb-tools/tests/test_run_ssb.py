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

"""Exercise orchestration failures without requiring a running Doris cluster."""

import csv
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SSB_ROOT = Path(__file__).resolve().parents[1]
MYSQL = r'''#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
sql = args[args.index('--execute') + 1] if '--execute' in args else sys.stdin.read()
sql = '\n'.join(line for line in sql.splitlines() if not line.lstrip().startswith('--')).strip()
with open(os.environ['SSB_TEST_CALLS'], 'a') as log:
    log.write(json.dumps(sql) + '\n')
scenario = os.environ.get('SSB_TEST_SCENARIO', '')
if sql.startswith('CREATE DATABASE') and scenario == 'existing_database':
    sys.exit('Database already exists')
if sql.startswith('DESC'):
    print('lo_orderkey\tBIGINT\nlo_linenumber\tINT')
elif 'INSERT INTO' in sql:
    if scenario == 'insert_error':
        sys.exit('Generator exited with 23')
    print("Query OK, 10 rows affected\n{'status':'%s'}" % (
        'COMMITTED' if scenario == 'unpublished' else 'VISIBLE'))
elif sql.startswith('SELECT') and 'VERSION()' not in sql:
    if scenario == 'query_error':
        sys.exit('Query execution failed')
    print('42')
'''


class RunSsbTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="ssb run ")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / "mysql").write_text(MYSQL)
        (self.root / "mysql").chmod(0o755)
        self.config = self.root / "cluster.conf"
        self.config.write_text("FE_HOST=127.0.0.1\nFE_QUERY_PORT=9030\nUSER=root\nPASSWORD=''\nDB=ssb_test\n")
        self.calls = self.root / "calls.jsonl"
        self.results = self.root / "results"

    def run_benchmark(self, scenario="", *options):
        return subprocess.run(
            ["bash", str(SSB_ROOT / "bin/run-ssb.sh"), "-c", str(self.config),
             "--result-dir", str(self.results), *options],
            env=dict(os.environ, PATH=f"{self.root}:{os.environ['PATH']}",
                     SSB_TEST_CALLS=str(self.calls), SSB_TEST_SCENARIO=scenario),
            cwd=self.root, capture_output=True, text=True, timeout=30,
        )

    def statements(self):
        return [json.loads(line) for line in self.calls.read_text().splitlines()]

    def test_complete_workflow_and_distinct_results(self):
        result = self.run_benchmark()
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        calls = self.statements()
        self.assertEqual(12, sum("INSERT INTO" in sql for sql in calls))
        self.assertEqual(1, sum(sql.startswith("ANALYZE") for sql in calls))
        with (self.results / "result.csv").open() as file:
            rows = list(csv.DictReader(file))
        self.assertEqual(26, len(rows))
        self.assertEqual({"ssb", "ssb-flat"}, {row["suite"] for row in rows})
        self.assertEqual(78, len(list(self.results.glob("*/*.out"))))

    def test_existing_database_never_receives_inserts(self):
        result = self.run_benchmark("existing_database")
        self.assertNotEqual(0, result.returncode)
        self.assertEqual(1, len(self.statements()))

    def test_failed_or_unpublished_insert_stops_preparation(self):
        for scenario in ("insert_error", "unpublished"):
            with self.subTest(scenario=scenario):
                self.results = self.root / scenario
                self.calls = self.root / f"{scenario}.jsonl"
                result = self.run_benchmark(scenario)
                self.assertNotEqual(0, result.returncode)
                self.assertEqual(1, sum("INSERT INTO" in sql for sql in self.statements()))
                self.assertFalse((self.results / "result.csv").exists())
                self.assertTrue((self.results / "prepare.log").read_text())

    def test_query_failure_is_reported_and_stops_the_suite(self):
        result = self.run_benchmark("query_error", "--queries-only")
        self.assertNotEqual(0, result.returncode)
        self.assertEqual(2, len(self.statements()))
        self.assertIn("Query execution failed", (self.results / "ssb/q1.1.cold.err").read_text())
        self.assertNotIn("SSB completed", result.stdout)

    def test_queries_only_does_not_modify_tables(self):
        result = self.run_benchmark("", "--queries-only", "--mode", "flat")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertTrue(all(sql.startswith("SELECT") for sql in self.statements()))
        self.assertFalse((self.results / "ssb").exists())
        self.assertEqual(39, len(list((self.results / "ssb-flat").glob("*.out"))))

    def test_invalid_scale_is_rejected_before_connecting(self):
        result = self.run_benchmark("", "-s", "2")
        self.assertNotEqual(0, result.returncode)
        self.assertFalse(self.calls.exists())


if __name__ == "__main__":
    unittest.main()
