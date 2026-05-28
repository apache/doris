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

suite("test_timestamptz_agg_functions", "datatype_p0") {
    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"
    sql "SET time_zone = '+00:00'"

    sql "DROP TABLE IF EXISTS test_tz_agg"
    sql """
        CREATE TABLE test_tz_agg (
            id    INT,
            ts    TIMESTAMPTZ(6),
            arr   ARRAY<TIMESTAMPTZ(6)>,
            w     INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num' = '1')
    """

    sql """
        INSERT INTO test_tz_agg VALUES
        (1, CAST('2024-01-01 00:00:00 +00:00' AS TIMESTAMPTZ(6)),
            ARRAY(CAST('2024-01-01 00:00:00 +00:00' AS TIMESTAMPTZ(6)), CAST('2024-01-02 00:00:00 +00:00' AS TIMESTAMPTZ(6))), 1),
        (2, CAST('2024-01-02 00:00:00 +00:00' AS TIMESTAMPTZ(6)),
            ARRAY(CAST('2024-01-02 00:00:00 +00:00' AS TIMESTAMPTZ(6)), CAST('2024-01-03 00:00:00 +00:00' AS TIMESTAMPTZ(6))), 2),
        (3, CAST('2024-01-03 00:00:00 +00:00' AS TIMESTAMPTZ(6)),
            ARRAY(CAST('2024-01-01 00:00:00 +00:00' AS TIMESTAMPTZ(6)), CAST('2024-01-02 00:00:00 +00:00' AS TIMESTAMPTZ(6))), 3)
    """

    // map_agg_v2 with TIMESTAMPTZ as key
    qt_map_agg_v2 "SELECT size(map_agg_v2(ts, id)) FROM test_tz_agg"

    // histogram on TIMESTAMPTZ — just check it returns a non-null result
    qt_histogram_not_null "SELECT histogram(ts) IS NOT NULL FROM test_tz_agg"

    // group_array_intersect on Array<TIMESTAMPTZ>
    qt_group_array_intersect "SELECT size(group_array_intersect(arr)) FROM test_tz_agg"

    // group_array_union on Array<TIMESTAMPTZ>
    qt_group_array_union "SELECT size(group_array_union(arr)) FROM test_tz_agg"

    sql "DROP TABLE IF EXISTS test_tz_agg"

    sql "DROP TABLE IF EXISTS tz_group_array_crash"
    sql """
        CREATE TABLE tz_group_array_crash (
            grp INT,
            arr ARRAY<TIMESTAMPTZ(6)>
        )
        DUPLICATE KEY(grp)
        DISTRIBUTED BY HASH(grp) BUCKETS 1
        PROPERTIES('replication_num' = '1')
    """

    sql """
        INSERT INTO tz_group_array_crash VALUES
        (
            1,
            ARRAY(
                CAST('2024-01-01 00:00:00 +00:00' AS TIMESTAMPTZ(6)),
                CAST('2024-01-01 08:00:00 +08:00' AS TIMESTAMPTZ(6)),
                CAST('2024-01-02 00:00:00 +00:00' AS TIMESTAMPTZ(6))
            )
        ),
        (
            1,
            ARRAY(
                CAST('2024-01-01 00:00:00 +00:00' AS TIMESTAMPTZ(6)),
                CAST('2024-01-02 08:00:00 +08:00' AS TIMESTAMPTZ(6)),
                CAST('2024-01-03 00:00:00 +00:00' AS TIMESTAMPTZ(6))
            )
        )
    """

    qt_group_array_nested_timestamptz """
        SELECT CAST(array_sort(group_array(arr)) AS STRING)
        FROM tz_group_array_crash
        GROUP BY grp
    """

    sql "DROP TABLE IF EXISTS tz_group_array_crash"
}
