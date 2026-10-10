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

"""Run with DORIS_FLIGHT_SQL_URI and optional DORIS_USER/DORIS_PASSWORD.

LARGEINT retains its string storage encoding. PyArrow does not interpret custom
field metadata automatically; applications can use doris_type to distinguish
these strings from text and convert them to Python integers without losing range.
"""

import os
import unittest

import adbc_driver_flightsql.dbapi as flight_sql
import adbc_driver_manager
import pyarrow as pa


@unittest.skipUnless(os.getenv("DORIS_FLIGHT_SQL_URI"), "Set DORIS_FLIGHT_SQL_URI to run against Doris")
class NestedTypeMetadataTest(unittest.TestCase):
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

    def assert_logical_type(self, field, name):
        self.assertEqual((field.metadata or {}).get(b"doris_type"), name.encode())

    def query(self, sql):
        self.cursor.execute(sql)
        table = self.cursor.fetch_arrow_table()
        table.validate(full=True)
        return table

    def test_largeint_metadata_and_full_range(self):
        table = self.query("""
            SELECT CAST('170141183460469231731687303715884105727' AS LARGEINT) AS scalar_value,
                   named_struct('number', CAST(17 AS LARGEINT), 'text', '17') AS struct_value,
                   array(CAST('-170141183460469231731687303715884105728' AS LARGEINT),
                         CAST(NULL AS LARGEINT)) AS array_value,
                   map(CAST(17 AS LARGEINT), CAST(19 AS LARGEINT)) AS map_value
        """)
        self.assert_logical_type(table.schema.field("scalar_value"), "LARGEINT")
        structure = table.schema.field("struct_value").type
        self.assert_logical_type(structure.field("number"), "LARGEINT")
        self.assertNotIn(b"doris_type", structure.field("text").metadata or {})
        self.assert_logical_type(table.schema.field("array_value").type.value_field, "LARGEINT")
        mapping = table.schema.field("map_value").type
        self.assert_logical_type(mapping.key_field, "LARGEINT")
        self.assert_logical_type(mapping.item_field, "LARGEINT")
        self.assertFalse(mapping.key_field.nullable)
        self.assertTrue(pa.types.is_string(table.schema.field("scalar_value").type))
        row = table.to_pylist()[0]
        self.assertEqual(row["scalar_value"], str(2**127 - 1))
        self.assertEqual(row["array_value"], [str(-(2**127)), None])
        self.assertEqual(row["struct_value"], {"number": "17", "text": "17"})
        self.assertEqual(row["map_value"], [("17", "19")])

    def test_ipv4_metadata(self):
        table = self.query("""
            SELECT array(CAST('192.0.2.1' AS IPV4)) AS ip4
        """)
        self.assert_logical_type(table.schema.field("ip4").type.value_field, "IPV4")

    def test_ipv6_metadata(self):
        table = self.query("""
            SELECT named_struct('address', CAST('2001:db8::1' AS IPV6)) AS ip6
        """)
        self.assert_logical_type(table.schema.field("ip6").type.field("address"), "IPV6")

    def test_json_metadata(self):
        # ARRAY/MAP constructors reject JSON; ARRAY_REPEAT preserves its element type.
        table = self.query("""
            SELECT array_repeat(CAST('{"n":1}' AS JSON), 1) AS json_value
        """)
        self.assert_logical_type(table.schema.field("json_value").type.value_field, "JSON")

    def test_variant_metadata(self):
        # Preserve the Variant element type when constructing the nested result.
        table = self.query("""
            SELECT array_repeat(CAST('{"n":1}' AS VARIANT), 1) AS variant_value
        """)
        self.assert_logical_type(table.schema.field("variant_value").type.value_field, "VARIANT")


if __name__ == "__main__":
    unittest.main()
