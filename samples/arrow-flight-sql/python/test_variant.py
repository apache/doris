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

"""Run against a Variant V2 cluster with DORIS_FLIGHT_URI, DORIS_USER and DORIS_PASSWORD."""

import os
import unittest

import adbc_driver_flightsql
import adbc_driver_manager
import pyarrow as pa


class NativeVariantTest(unittest.TestCase):
    def test_query_and_partitions(self):
        uri = os.environ["DORIS_FLIGHT_URI"]
        options = {
            adbc_driver_manager.DatabaseOptions.USERNAME.value: os.environ.get("DORIS_USER", "root"),
            adbc_driver_manager.DatabaseOptions.PASSWORD.value: os.environ.get("DORIS_PASSWORD", ""),
        }
        query = """SELECT 1 AS id, parse_to_variant('42') AS v
                   UNION ALL SELECT 2, parse_to_variant(CAST(NULL AS STRING))"""
        with adbc_driver_flightsql.connect(uri, db_kwargs=options) as database:
            with adbc_driver_manager.AdbcConnection(database) as connection:
                def execute(sql):
                    with adbc_driver_manager.AdbcStatement(connection) as statement:
                        statement.set_sql_query(sql)
                        stream, _ = statement.execute_query()
                        return pa.RecordBatchReader._import_from_c(stream.address).read_all()

                execute("SET enable_sql_cache=false")
                for parallel in (False, True):
                    execute(f"SET enable_parallel_result_sink={str(parallel).lower()}")
                    table = execute(query)
                    self.check_result(table)
                    # ExecuteSchema and Prepare must agree with the subsequently fetched batches.
                    with adbc_driver_manager.AdbcStatement(connection) as statement:
                        statement.set_sql_query(query)
                        schema_handle = statement.execute_schema()
                        schema = pa.Schema._import_from_c(schema_handle.address)
                        self.assertEqual(schema, table.schema)
                        statement.prepare()
                        stream, _ = statement.execute_query()
                        prepared = pa.RecordBatchReader._import_from_c(stream.address).read_all()
                        self.assertEqual(prepared.schema, schema)
                        self.check_result(prepared)

                    with adbc_driver_manager.AdbcStatement(connection) as statement:
                        statement.set_sql_query(query)
                        partitions, _, _ = statement.execute_partitions()
                        tables = []
                        for partition in partitions:
                            stream = connection.read_partition(partition)
                            tables.append(pa.RecordBatchReader._import_from_c(stream.address).read_all())
                        self.check_result(pa.concat_tables(tables))

    def check_result(self, table):
        table = table.sort_by("id")
        self.assertEqual(table.num_rows, 2)
        field = table.schema.field("v")
        values = table.column("v").combine_chunks()
        # Clients without a registered extension expose its storage plus field metadata.
        if isinstance(values, pa.ExtensionArray):
            self.assertEqual(values.type.extension_name, "arrow.parquet.variant")
            values = values.storage
        else:
            self.assertEqual(field.metadata[b"ARROW:extension:name"], b"arrow.parquet.variant")
        self.assertTrue(pa.types.is_struct(values.type))
        self.assertEqual([child.name for child in values.type], ["metadata", "value"])
        self.assertIsNone(values[1].as_py())
        row = values[0].as_py()
        self.assertTrue(row["metadata"])
        # The Parquet Variant primitive INT8 tag is 3 << 2, followed by its signed byte.
        self.assertEqual(row["value"], bytes([12, 42]))


if __name__ == "__main__":
    unittest.main()
