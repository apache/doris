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

suite("test_timestamptz_array_index", "datatype_p0") {
    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"
    sql "SET time_zone = '+00:00'"

    sql "DROP TABLE IF EXISTS test_tz_array_index"
    sql """
        CREATE TABLE test_tz_array_index (
            id    INT,
            arr   ARRAY<TIMESTAMPTZ(6)>,
            probe TIMESTAMPTZ(6)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num' = '1')
    """

    sql """
        INSERT INTO test_tz_array_index VALUES (
            1,
            ARRAY(
                CAST('2024-03-31 01:15:00 +00:00' AS TIMESTAMPTZ(6)),
                CAST('2024-03-31 09:15:00 +08:00' AS TIMESTAMPTZ(6)),
                CAST('2024-03-31 02:15:00 +00:00' AS TIMESTAMPTZ(6))
            ),
            CAST('2024-03-31 01:15:00 +00:00' AS TIMESTAMPTZ(6))
        )
    """
    sql """
        INSERT INTO test_tz_array_index VALUES (
            2,
            ARRAY(
                CAST('2024-06-01 10:00:00 +00:00' AS TIMESTAMPTZ(6)),
                CAST('2024-06-01 20:00:00 +00:00' AS TIMESTAMPTZ(6))
            ),
            CAST('2024-06-01 12:00:00 +00:00' AS TIMESTAMPTZ(6))
        )
    """
    sql """
        INSERT INTO test_tz_array_index VALUES (
            3,
            ARRAY(
                CAST('2024-01-01 00:00:00 +00:00' AS TIMESTAMPTZ(6))
            ),
            NULL
        )
    """

    // array_position: first occurrence (1-based), or 0 if not found
    qt_array_position "SELECT id, array_position(arr, probe) AS pos FROM test_tz_array_index ORDER BY id"

    // array_contains: true/false
    qt_array_contains "SELECT id, array_contains(arr, probe) AS contained FROM test_tz_array_index ORDER BY id"

    // countequal: count of matching elements
    qt_countequal "SELECT id, countequal(arr, probe) AS cnt FROM test_tz_array_index ORDER BY id"

    // array_position with literal probe value (same timezone, equivalent UTC)
    qt_array_position_literal """
        SELECT id, array_position(arr, CAST('2024-03-31 09:15:00 +08:00' AS TIMESTAMPTZ(6))) AS pos
        FROM test_tz_array_index
        WHERE id = 1
    """

    sql "DROP TABLE IF EXISTS test_tz_array_index"
}
