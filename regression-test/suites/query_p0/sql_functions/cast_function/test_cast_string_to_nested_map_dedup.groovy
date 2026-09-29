// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_cast_string_to_nested_map_dedup") {
    // A duplicated key keeps its last value in every map of a string cast, also in the maps
    // inside arrays, structs, map values and map keys, as it does in the top level map.
    def check = {
        qt_top """ SELECT CAST('{"a":1,"a":2}' AS MAP<STRING, INT>) """
        qt_map_in_map """ SELECT CAST('{"outer":{"a":1,"a":2}}' AS MAP<STRING, MAP<STRING, INT>>) """
        qt_array """
            SELECT CAST('[{"a":1,"a":2}]' AS ARRAY<MAP<STRING, INT>>) AS c,
                   size(element_at(CAST('[{"a":1,"a":2}]' AS ARRAY<MAP<STRING, INT>>), 1)),
                   element_at(element_at(CAST('[{"a":1,"a":2}]' AS ARRAY<MAP<STRING, INT>>), 1), 'a')
        """
        qt_struct """
            SELECT CAST('{"m":{"a":1,"a":2}}' AS STRUCT<m:MAP<STRING, INT>>) AS c,
                   size(struct_element(CAST('{"m":{"a":1,"a":2}}' AS STRUCT<m:MAP<STRING, INT>>), 'm'))
        """
        qt_array_in_map """
            SELECT CAST('{"outer":[{"a":1,"a":2}]}' AS MAP<STRING, ARRAY<MAP<STRING, INT>>>) AS c,
                   element_at(element_at(element_at(
                       CAST('{"outer":[{"a":1,"a":2}]}' AS MAP<STRING, ARRAY<MAP<STRING, INT>>>),
                       'outer'), 1), 'a')
        """
        qt_struct_in_map """
            SELECT CAST('{"outer":{"m":{"a":1,"a":2}}}' AS MAP<STRING, STRUCT<m:MAP<STRING, INT>>>)
        """
        qt_map_as_key """ SELECT CAST('{{"a":1,"a":2}:1}' AS MAP<MAP<STRING, INT>, INT>) """
    }

    sql """ set enable_strict_cast = false """
    check()
    sql """ set enable_strict_cast = true """
    check()
    sql """ set enable_strict_cast = false """

    sql """ DROP TABLE IF EXISTS test_cast_string_to_nested_map_dedup """
    sql """
        CREATE TABLE test_cast_string_to_nested_map_dedup (
            id INT,
            s STRING
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_cast_string_to_nested_map_dedup VALUES
            (1, '[{"a":1,"a":2}]'),
            (2, '[{"a":1}, {"b":1,"b":2,"b":3}]'),
            (3, '[{"a":1,"b":2}]'),
            (4, '[]'),
            (5, NULL)
    """
    order_qt_column """
        SELECT id, CAST(s AS ARRAY<MAP<STRING, INT>>) FROM test_cast_string_to_nested_map_dedup
    """
}
