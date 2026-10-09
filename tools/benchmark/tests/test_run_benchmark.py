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


TOOLS_ROOT = Path(__file__).resolve().parents[2]
MYSQL = r'''#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
sql = args[args.index('--execute') + 1] if '--execute' in args else sys.stdin.read()
sql = '\n'.join(line for line in sql.splitlines() if not line.lstrip().startswith('--')).strip()
with open(os.environ['BENCHMARK_TEST_CALLS'], 'a') as log:
    log.write(json.dumps({'sql': sql, 'args': args}) + '\n')
scenario = os.environ.get('BENCHMARK_TEST_SCENARIO', '')
if sql.startswith('CREATE DATABASE') and scenario == 'existing_database':
    sys.exit('Database already exists')
if sql.startswith('DESC'):
    if '`promotion`' in sql:
        print('p_promo_sk\tINT\np_response_target\tINT')
    else:
        print('lo_orderkey\tBIGINT\nlo_linenumber\tINT')
elif sql.startswith('CREATE TABLE') and scenario == 'flat_ddl_error':
    sys.exit('Wide table creation failed')
elif 'INSERT INTO' in sql:
    if '-vvv' in args:
        print('--------------\n' + sql + '\n--------------')
    if scenario == 'insert_error' or (scenario == 'flat_insert_error' and '_flat`' in sql):
        sys.exit('Generator exited with 23')
    print('Query OK, 10 rows affected')
    if scenario != 'missing_status':
        status = ('COMMITTED' if scenario == 'unpublished' or
                  (scenario == 'flat_unpublished' and '_flat`' in sql) else
                  'PREPARE' if scenario == 'unexpected_status' else 'VISIBLE')
        label = 'COMMITTED_label' if scenario == 'committed_label' else 'insert_label'
        print("{'label':'%s', 'status':'%s', 'txnId':'123'}" % (label, status))
elif sql.startswith('SELECT (SELECT COUNT(*)'):
    print('10\t9' if scenario == 'flat_row_loss' else '10\t10')
elif sql.startswith('ANALYZE TABLE') and scenario == 'flat_statistics_error':
    sys.exit('Wide table statistics failed')
elif sql.lower().startswith(('select', 'with')) and 'VERSION()' not in sql:
    if scenario == 'query_error':
        sys.exit('Query execution failed')
    print('42')
'''


class RunBenchmarkTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="benchmark run ")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / "mysql").write_text(MYSQL)
        (self.root / "mysql").chmod(0o755)
        self.config = self.root / "cluster.conf"
        self.config.write_text("FE_HOST=127.0.0.1\nFE_QUERY_PORT=9030\nUSER=root\nPASSWORD=''\nDB=benchmark_test\n")
        self.calls = self.root / "calls.jsonl"
        self.results = self.root / "results"

    def run_benchmark(self, scenario="", *options, benchmark="ssb"):
        return subprocess.run(
            ["bash", str(TOOLS_ROOT / f"{benchmark}-tools/bin/run-{benchmark}.sh"), "-c", str(self.config),
             "--result-dir", str(self.results), *options],
            env=dict(os.environ, PATH=f"{self.root}:{os.environ['PATH']}",
                     BENCHMARK_TEST_CALLS=str(self.calls), BENCHMARK_TEST_SCENARIO=scenario),
            cwd=self.root, capture_output=True, text=True, timeout=30,
        )

    def statements(self):
        return [json.loads(line)["sql"] for line in self.calls.read_text().splitlines()]

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
        for scenario in ("insert_error", "unpublished", "missing_status", "unexpected_status"):
            with self.subTest(scenario=scenario):
                self.results = self.root / scenario
                self.calls = self.root / f"{scenario}.jsonl"
                result = self.run_benchmark(scenario)
                self.assertNotEqual(0, result.returncode)
                self.assertEqual(1, sum("INSERT INTO" in sql for sql in self.statements()))
                self.assertFalse((self.results / "result.csv").exists())
                self.assertTrue((self.results / "prepare.log").read_text())

    def test_visible_status_is_not_confused_with_sql_or_label(self):
        for benchmark in ("ssb", "tpch", "tpcds"):
            with self.subTest(benchmark=benchmark):
                self.results = self.root / benchmark
                self.calls = self.root / f"{benchmark}.jsonl"
                result = self.run_benchmark("committed_label", "-d", f"{benchmark}_COMMITTED",
                                            benchmark=benchmark)
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                log = (self.results / "prepare.log").read_text()
                self.assertIn(f"{benchmark}_gen_{benchmark}_COMMITTED", log)
                self.assertIn("'label':'COMMITTED_label'", log)
                self.assertTrue((self.results / "result.csv").exists())

    def test_visible_in_sql_does_not_hide_an_unpublished_insert(self):
        result = self.run_benchmark("unpublished", "-d", "tpch_VISIBLE", benchmark="tpch")
        self.assertNotEqual(0, result.returncode)
        self.assertEqual(1, sum("INSERT INTO" in sql for sql in self.statements()))
        self.assertFalse((self.results / "result.csv").exists())

    def test_generator_splits_scale_with_data_and_allow_override(self):
        for benchmark, property_name in (("tpch", "splits-per-node"), ("tpcds", "split-count")):
            for scale, splits in ((1, 10), (100, 100), (1000, 1000), (10000, 10000)):
                for override in (None, 7):
                    with self.subTest(benchmark=benchmark, scale=scale, override=override):
                        name = f"{benchmark}-{scale}-{override}"
                        self.results = self.root / name
                        self.calls = self.root / f"{name}.jsonl"
                        options = ("-s", str(scale))
                        if override is not None:
                            options += ("--splits", str(override))
                        # Stop at the first data import: this test checks catalog configuration.
                        result = self.run_benchmark("insert_error", *options, benchmark=benchmark)
                        self.assertNotEqual(0, result.returncode)
                        self.assertIn("Generator exited with 23", result.stderr)
                        expected = override if override is not None else splits
                        catalog = self.statements()[1]
                        self.assertIn(f"'trino.{benchmark}.{property_name}'='{expected}'", catalog)

    def test_invalid_splits_are_rejected_before_connecting(self):
        for benchmark in ("tpch", "tpcds"):
            for splits in ("", "0", "-1", "1.5", "abc", "2147483648", "9223372036854775808"):
                with self.subTest(benchmark=benchmark, splits=splits):
                    result = self.run_benchmark("", "--splits", splits, benchmark=benchmark)
                    self.assertNotEqual(0, result.returncode)
                    self.assertFalse(self.calls.exists())
        result = self.run_benchmark("", "--splits", "10", benchmark="ssb")
        self.assertNotEqual(0, result.returncode)
        self.assertFalse(self.calls.exists())

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

    def test_tpch_and_tpcds_complete_workflow(self):
        for benchmark, table_count, query_count in (("tpch", 8, 22), ("tpcds", 24, 103)):
            with self.subTest(benchmark=benchmark):
                self.results = self.root / benchmark
                self.calls = self.root / f"{benchmark}.jsonl"
                result = self.run_benchmark(benchmark=benchmark)
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                calls = self.statements()
                inserts = [sql for sql in calls if "INSERT INTO" in sql]
                flat_count = 1 if benchmark == "tpch" else 3
                self.assertEqual(table_count + flat_count, len(inserts))
                flat_inserts = [sql for sql in inserts if "_flat`" in sql]
                self.assertEqual(flat_count, len(flat_inserts))
                self.assertTrue(all("LEFT JOIN" in sql for sql in flat_inserts))
                self.assertEqual(flat_count + 1, len((self.results / "flat-row-counts.tsv").read_text().splitlines()))
                inserts = [sql for sql in inserts if "_flat`" not in sql]
                self.assertTrue(all(f"`{benchmark}_gen_benchmark_test`.`sf1`." in sql for sql in inserts))
                # Target columns and source projections must use the same explicit names.
                ordinary_inserts = [sql for sql in inserts if "INSERT INTO `promotion`" not in sql]
                self.assertTrue(all("(`lo_orderkey`,`lo_linenumber`)" in sql for sql in ordinary_inserts))
                self.assertTrue(all("SELECT `lo_orderkey`,`lo_linenumber`" in sql for sql in ordinary_inserts))
                analyses = [sql for sql in calls if sql.startswith("ANALYZE")]
                self.assertEqual(1 + flat_count, len(analyses))
                self.assertIn("WITH FULL WITH SYNC", analyses[0])
                self.assertTrue(all("WITH SAMPLE ROWS 100000 WITH SYNC" in sql for sql in analyses[1:]))
                self.assertGreater(max(i for i, sql in enumerate(calls) if sql.startswith("ANALYZE")),
                                   max(i for i, sql in enumerate(calls) if "INSERT INTO" in sql))
                self.assertFalse(any("lineorder_flat" in sql for sql in calls))
                with (self.results / "result.csv").open() as file:
                    rows = list(csv.DictReader(file))
                self.assertEqual(query_count, len(rows))
                self.assertEqual({benchmark}, {row["suite"] for row in rows})
                for row in rows:
                    self.assertEqual(min(int(row["hot1_ms"]), int(row["hot2_ms"])), int(row["best_hot_ms"]))
                self.assertEqual(query_count * 3, len(list(self.results.glob("*/*.out"))))
                entries = [json.loads(line) for line in self.calls.read_text().splitlines()]
                query_entries = entries[-query_count * 3:]
                self.assertTrue(all("--init-command=SET enable_sql_cache=false, enable_query_cache=false"
                                    in entry["args"] for entry in query_entries))
                if benchmark == "tpch":
                    self.assertIn("'trino.tpch.column-naming'='STANDARD'", calls[1])
                    self.assertIn("'trino.tpch.double-type-mapping'='DECIMAL'", calls[1])
                    self.assertIn("create view revenue0", calls[2])
                else:
                    self.assertIn("'trino.tpcds.split-count'='10'", calls[1])
                    promotion = next(sql for sql in inserts if "INSERT INTO `promotion`" in sql)
                    self.assertIn("(`p_promo_sk`,`p_response_target`)", promotion)
                    self.assertIn("SELECT `p_promo_sk`,`p_response_targe`", promotion)
                    for query in (14, 23, 24, 39):
                        self.assertIn(f"q{query}_1", [row["query"] for row in rows])

    def test_tpc_queries_only_uses_requested_scale(self):
        for benchmark in ("tpch", "tpcds"):
            with self.subTest(benchmark=benchmark):
                self.results = self.root / benchmark
                self.calls = self.root / f"{benchmark}.jsonl"
                result = self.run_benchmark("", "--queries-only", "-s", "10000", benchmark=benchmark)
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                calls = self.statements()
                self.assertFalse(any("INSERT INTO" in sql or sql.startswith(("CREATE", "ANALYZE"))
                                     for sql in calls))
                query_path = (TOOLS_ROOT / "tpcds-tools/queries/sf10000/query1.sql" if benchmark == "tpcds"
                              else TOOLS_ROOT / "tpch-tools/queries/q1.sql")
                expected = "\n".join(line for line in query_path.read_text().splitlines()
                                     if not line.lstrip().startswith("--")).strip()
                self.assertEqual(expected, calls[1])

    def test_tpc_preparation_errors_abort_before_queries(self):
        for benchmark in ("tpch", "tpcds"):
            for scenario in ("existing_database", "insert_error", "unpublished"):
                with self.subTest(benchmark=benchmark, scenario=scenario):
                    self.results = self.root / f"{benchmark}-{scenario}"
                    self.calls = self.root / f"{benchmark}-{scenario}.jsonl"
                    result = self.run_benchmark(scenario, benchmark=benchmark)
                    self.assertNotEqual(0, result.returncode)
                    self.assertFalse((self.results / "result.csv").exists())
                    self.assertFalse(any(sql.startswith("ANALYZE") for sql in self.statements()))
                    expected_inserts = 0 if scenario == "existing_database" else 1
                    self.assertEqual(expected_inserts, sum("INSERT INTO" in sql for sql in self.statements()))

    def test_flat_failures_abort_before_measuring_queries(self):
        for benchmark in ("tpch", "tpcds"):
            for scenario in ("flat_ddl_error", "flat_insert_error", "flat_unpublished",
                             "flat_row_loss", "flat_statistics_error"):
                with self.subTest(benchmark=benchmark, scenario=scenario):
                    self.results = self.root / f"{benchmark}-{scenario}"
                    self.calls = self.root / f"{benchmark}-{scenario}.jsonl"
                    result = self.run_benchmark(scenario, benchmark=benchmark)
                    self.assertNotEqual(0, result.returncode)
                    self.assertFalse((self.results / "result.csv").exists())
                    self.assertNotIn("completed", result.stdout)

    def test_tpc_query_error_is_not_recorded_as_success(self):
        for benchmark in ("tpch", "tpcds"):
            with self.subTest(benchmark=benchmark):
                self.results = self.root / benchmark
                self.calls = self.root / f"{benchmark}.jsonl"
                result = self.run_benchmark("query_error", "--queries-only", benchmark=benchmark)
                self.assertNotEqual(0, result.returncode)
                with (self.results / "result.csv").open() as file:
                    self.assertEqual([], list(csv.DictReader(file)))
                self.assertIn("Query execution failed", (self.results / benchmark / "q1.cold.err").read_text())
                self.assertEqual(2, len(self.statements()))

    def test_tpc_rejects_ssb_modes_and_invalid_arguments_before_connecting(self):
        for benchmark in ("tpch", "tpcds"):
            for options in (("--mode", "flat"), ("--mode", "both"), ("-s", "2"),
                            ("-d", "invalid-name"), ("unexpected",)):
                with self.subTest(benchmark=benchmark, options=options):
                    result = self.run_benchmark("", *options, benchmark=benchmark)
                    self.assertNotEqual(0, result.returncode)
                    self.assertFalse(self.calls.exists())

    def test_existing_result_directory_is_not_overwritten(self):
        self.results.mkdir()
        marker = self.results / "result.csv"
        marker.write_text("previous result")
        result = self.run_benchmark("", "--queries-only", benchmark="tpch")
        self.assertNotEqual(0, result.returncode)
        self.assertEqual("previous result", marker.read_text())
        self.assertFalse(self.calls.exists())


if __name__ == "__main__":
    unittest.main()
