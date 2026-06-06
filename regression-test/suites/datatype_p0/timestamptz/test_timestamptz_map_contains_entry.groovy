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

suite("test_timestamptz_map_contains_entry") {

    sql "set time_zone = '+08:00';"
    sql "set enable_nereids_planner = true;"
    sql "set enable_fallback_to_original_planner = false;"

    // --- inline literal tests (no table needed) ---

    // TIMESTAMPTZ as map value: hit
    qt_value_hit """
        SELECT map_contains_entry(
            map('a', cast('2024-01-01 00:00:00.000000 +00:00' as timestamptz(6)),
                'b', cast('2024-01-02 03:04:05.123456 +00:00' as timestamptz(6))),
            'a',
            cast('2024-01-01 00:00:00.000000 +00:00' as timestamptz(6))
        );
    """

    // TIMESTAMPTZ as map value: miss (wrong value)
    qt_value_miss """
        SELECT map_contains_entry(
            map('a', cast('2024-01-01 00:00:00.000000 +00:00' as timestamptz(6)),
                'b', cast('2024-01-02 03:04:05.123456 +00:00' as timestamptz(6))),
            'a',
            cast('2024-01-02 03:04:05.123456 +00:00' as timestamptz(6))
        );
    """

    // TIMESTAMPTZ as map value: miss (wrong key)
    qt_value_miss_key """
        SELECT map_contains_entry(
            map('a', cast('2024-01-01 00:00:00.000000 +00:00' as timestamptz(6)),
                'b', cast('2024-01-02 03:04:05.123456 +00:00' as timestamptz(6))),
            'c',
            cast('2024-01-01 00:00:00.000000 +00:00' as timestamptz(6))
        );
    """

    // TIMESTAMPTZ as map key: hit
    qt_key_hit """
        SELECT map_contains_entry(
            map(cast('2024-01-01 00:00:00.000000 +00:00' as timestamptz(6)), 'a',
                cast('2024-01-02 03:04:05.123456 +00:00' as timestamptz(6)), 'b'),
            cast('2024-01-01 00:00:00.000000 +00:00' as timestamptz(6)),
            'a'
        );
    """

    // TIMESTAMPTZ as map key: miss (wrong value)
    qt_key_miss_value """
        SELECT map_contains_entry(
            map(cast('2024-01-01 00:00:00.000000 +00:00' as timestamptz(6)), 'a',
                cast('2024-01-02 03:04:05.123456 +00:00' as timestamptz(6)), 'b'),
            cast('2024-01-01 00:00:00.000000 +00:00' as timestamptz(6)),
            'b'
        );
    """

    // TIMESTAMPTZ as map key: miss (wrong key)
    qt_key_miss_key """
        SELECT map_contains_entry(
            map(cast('2024-01-01 00:00:00.000000 +00:00' as timestamptz(6)), 'a',
                cast('2024-01-02 03:04:05.123456 +00:00' as timestamptz(6)), 'b'),
            cast('2024-01-03 00:00:00.000000 +00:00' as timestamptz(6)),
            'a'
        );
    """

    // --- table-based tests ---

    sql "DROP TABLE IF EXISTS test_timestamptz_map_contains_entry_t;"
    sql """
        CREATE TABLE test_timestamptz_map_contains_entry_t (
            id INT,
            map_s_tz  MAP<STRING, TIMESTAMPTZ(6)>,
            map_tz_s  MAP<TIMESTAMPTZ(6), STRING>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """
        INSERT INTO test_timestamptz_map_contains_entry_t VALUES (
            1,
            map('a', cast('2024-01-01 00:00:00.000000 +00:00' as timestamptz(6)),
                'b', cast('2024-01-02 03:04:05.123456 +00:00' as timestamptz(6))),
            map(cast('2024-01-01 00:00:00.000000 +00:00' as timestamptz(6)), 'a',
                cast('2024-01-02 03:04:05.123456 +00:00' as timestamptz(6)), 'b')
        ), (
            2,
            map('x', cast('2024-06-15 12:00:00.000000 +05:30' as timestamptz(6))),
            map(cast('2024-06-15 12:00:00.000000 +05:30' as timestamptz(6)), 'x')
        );
    """

    // TIMESTAMPTZ as map value, hit
    qt_table_value_hit """
        SELECT id, map_contains_entry(map_s_tz, 'a', cast('2024-01-01 00:00:00.000000 +00:00' as timestamptz(6)))
        FROM test_timestamptz_map_contains_entry_t
        ORDER BY id;
    """

    // TIMESTAMPTZ as map value, miss
    qt_table_value_miss """
        SELECT id, map_contains_entry(map_s_tz, 'a', cast('2024-01-02 03:04:05.123456 +00:00' as timestamptz(6)))
        FROM test_timestamptz_map_contains_entry_t
        ORDER BY id;
    """

    // TIMESTAMPTZ as map key, hit
    qt_table_key_hit """
        SELECT id, map_contains_entry(map_tz_s, cast('2024-01-01 00:00:00.000000 +00:00' as timestamptz(6)), 'a')
        FROM test_timestamptz_map_contains_entry_t
        ORDER BY id;
    """

    // TIMESTAMPTZ as map key, miss
    qt_table_key_miss """
        SELECT id, map_contains_entry(map_tz_s, cast('2024-01-01 00:00:00.000000 +00:00' as timestamptz(6)), 'b')
        FROM test_timestamptz_map_contains_entry_t
        ORDER BY id;
    """

    // NULL search key
    qt_null_search_key """
        SELECT id, map_contains_entry(map_s_tz, NULL, cast('2024-01-01 00:00:00.000000 +00:00' as timestamptz(6)))
        FROM test_timestamptz_map_contains_entry_t
        ORDER BY id;
    """

    // NULL search value
    qt_null_search_value """
        SELECT id, map_contains_entry(map_s_tz, 'a', cast(NULL as timestamptz(6)))
        FROM test_timestamptz_map_contains_entry_t
        ORDER BY id;
    """
}
