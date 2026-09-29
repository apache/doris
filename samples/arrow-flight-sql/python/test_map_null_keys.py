#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import os
import unittest

import adbc_driver_flightsql.dbapi as flight_sql
import adbc_driver_manager
import pyarrow as pa


@unittest.skipUnless(os.getenv("DORIS_FLIGHT_SQL_URI"), "Set DORIS_FLIGHT_SQL_URI to run against Doris")
class MapNullKeyTest(unittest.TestCase):
    def setUp(self):
        self.connection = flight_sql.connect(
            uri=os.environ["DORIS_FLIGHT_SQL_URI"],
            db_kwargs={
                adbc_driver_manager.DatabaseOptions.USERNAME.value: os.getenv("DORIS_USER", "root"),
                adbc_driver_manager.DatabaseOptions.PASSWORD.value: os.getenv("DORIS_PASSWORD", ""),
            },
        )
        self.addCleanup(self.connection.close)
        self.cursor = self.connection.cursor()
        self.addCleanup(self.cursor.close)

    def query(self, sql):
        self.cursor.execute(sql)
        result = self.cursor.fetch_arrow_table()
        result.validate(full=True)
        return result

    def test_default_map_schema_is_preserved(self):
        result = self.query("SELECT map('k', 100) AS m")
        self.assertTrue(pa.types.is_map(result.schema.field("m").type))
        self.assertEqual(result.column("m").to_pylist(), [[("k", 100)]])

    def test_null_keys_and_containers(self):
        self.cursor.execute("SET arrow_flight_sql_map_as_list = true")
        result = self.query("""
            SELECT map(CAST(NULL AS STRING), 100, 'k', CAST(NULL AS INT)) AS m,
                   CAST(NULL AS MAP<STRING, INT>) AS absent,
                   CAST(map() AS MAP<STRING, INT>) AS empty_map,
                   named_struct('maps', array(map(CAST(NULL AS STRING),
                       map(CAST(NULL AS INT), 100)))) AS nested
        """)
        self.assertTrue(pa.types.is_list(result.schema.field("m").type))
        self.assertEqual(result.column("m").to_pylist(),
                         [[{"key": None, "value": 100}, {"key": "k", "value": None}]])
        self.assertEqual(result.column("absent").to_pylist(), [None])
        self.assertEqual(result.column("empty_map").to_pylist(), [[]])
        self.assertEqual(result.column("nested").to_pylist(),
                         [{"maps": [[{"key": None, "value": [{"key": None, "value": 100}]}]]}])

    def test_null_key_in_later_batch(self):
        self.cursor.execute("SET arrow_flight_sql_map_as_list = true")
        self.cursor.execute("SET batch_size = 1024")
        for parallel in (False, True):
            with self.subTest(parallel_result_sink=parallel):
                self.cursor.execute(f"SET enable_parallel_result_sink = {str(parallel).lower()}")
                self.cursor.execute("""
                    SELECT number AS n,
                           map(IF(number = 2048, NULL, CAST(number AS STRING)), number) AS m
                    FROM numbers("number" = "4097") ORDER BY number
                """)
                with self.cursor.fetch_record_batch() as reader:
                    schema = reader.schema
                    self.assertTrue(pa.types.is_list(schema.field("m").type))
                    batches = list(reader)
                self.assertGreater(len(batches), 1)
                for batch in batches:
                    batch.validate(full=True)
                    self.assertEqual(batch.schema, schema)
                rows = pa.Table.from_batches(batches).to_pylist()
                self.assertEqual(len(rows), 4097)
                for i, row in enumerate(rows):
                    self.assertEqual(row, {"n": i, "m": [{"key": None if i == 2048 else str(i), "value": i}]})


if __name__ == "__main__":
    unittest.main()
