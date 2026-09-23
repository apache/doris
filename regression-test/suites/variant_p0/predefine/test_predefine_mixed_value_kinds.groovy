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

// A typed path converts each value to its declared type. The result must not depend on the other
// values the same write batch puts on that path.
suite("test_predefine_mixed_value_kinds", "p0") {
    // Doc mode keeps the unconverted values in its doc column, and typed paths may be stored
    // sparse; pin the ordinary layout so the whole Variant shows the converted values.
    sql """ set default_variant_enable_doc_mode = false """
    sql """ set default_variant_enable_typed_paths_to_sparse = false """

    sql "DROP TABLE IF EXISTS test_predefine_mixed_value_kinds"
    sql """
        CREATE TABLE test_predefine_mixed_value_kinds (
            k INT,
            v VARIANT<'ts': DATETIME(3), 'd': DATE, 'i': INT, 's': STRING, 'arr': ARRAY<DATE>,
                      'tags': ARRAY<STRING>>
        ) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    // Rows 1 and 2 share one batch, which mixes strings with numbers on every typed path. Rows 3
    // and 4 repeat them in batches of their own, so each pair must read the same.
    sql """
        INSERT INTO test_predefine_mixed_value_kinds VALUES
        (1, PARSE_TO_VARIANT('{"ts": "2024-01-01 10:00:00.123456", "d": "2024-01-01", "i": "5", "s": "abc", "arr": ["2024-01-01", 5], "tags": ["a", 1, null]}')),
        (2, PARSE_TO_VARIANT('{"ts": 5, "d": 5, "i": 1.5, "s": 1.5, "arr": ["2024-01-02"], "tags": ["b", null]}'))
    """
    sql """
        INSERT INTO test_predefine_mixed_value_kinds VALUES
        (3, PARSE_TO_VARIANT('{"ts": "2024-01-01 10:00:00.123456", "d": "2024-01-01", "i": "5", "s": "abc", "arr": ["2024-01-01", 5], "tags": ["a", 1, null]}'))
    """
    sql """
        INSERT INTO test_predefine_mixed_value_kinds VALUES
        (4, PARSE_TO_VARIANT('{"ts": 5, "d": 5, "i": 1.5, "s": 1.5, "arr": ["2024-01-02"], "tags": ["b", null]}'))
    """
    order_qt_mixed_variant """ SELECT k, CAST(v AS STRING) FROM test_predefine_mixed_value_kinds """
    order_qt_mixed_paths """
        SELECT k, v['ts'], v['d'], v['i'], v['s'], v['arr'] FROM test_predefine_mixed_value_kinds
    """

    // CAST has no BOOLEAN -> DATE, ARRAY -> INT or INT -> ARRAY conversion. Such values are
    // dropped, whether or not other kinds share their batch.
    sql "DROP TABLE IF EXISTS test_predefine_unconvertible_value_kinds"
    sql """
        CREATE TABLE test_predefine_unconvertible_value_kinds (
            k INT,
            v VARIANT<'d': DATE, 'i': INT, 'arr': ARRAY<INT>>
        ) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """ INSERT INTO test_predefine_unconvertible_value_kinds VALUES (1, PARSE_TO_VARIANT('{"d": true, "i": [1, 2], "arr": 5}')) """
    sql """
        INSERT INTO test_predefine_unconvertible_value_kinds VALUES
        (2, PARSE_TO_VARIANT('{"d": true, "i": [1, 2], "arr": 5}')),
        (3, PARSE_TO_VARIANT('{"d": "2024-01-03", "i": 3, "arr": [3]}'))
    """
    order_qt_unconvertible """
        SELECT k, CAST(v AS STRING), v['d'], v['i'], v['arr'] FROM test_predefine_unconvertible_value_kinds
    """

    // CAST converts only strings to IPV4 and TIMESTAMPTZ; a number has no conversion there and is
    // dropped, while a valid string in the same batch is kept. TIMESTAMPTZ is only checked for
    // NULL, since its text depends on the session time zone.
    sql "DROP TABLE IF EXISTS test_predefine_ip_timestamptz_value_kinds"
    sql """
        CREATE TABLE test_predefine_ip_timestamptz_value_kinds (
            k INT,
            v VARIANT<'ip': IPV4, 'tz': TIMESTAMPTZ(3)>
        ) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_predefine_ip_timestamptz_value_kinds VALUES
        (1, PARSE_TO_VARIANT('{"ip": "12.12.12.12", "tz": "2024-01-01 10:00:00.123 +08:00"}')),
        (2, PARSE_TO_VARIANT('{"ip": 13, "tz": 20240102}'))
    """
    sql """ INSERT INTO test_predefine_ip_timestamptz_value_kinds VALUES (3, PARSE_TO_VARIANT('{"ip": 13, "tz": 20240102}')) """
    order_qt_ip_timestamptz """
        SELECT k, v['ip'], v['tz'] IS NULL FROM test_predefine_ip_timestamptz_value_kinds
    """
    qt_ip_timestamptz_cast """
        SELECT CAST(PARSE_TO_VARIANT('13') AS IPV4), CAST(PARSE_TO_VARIANT('20240102') AS TIMESTAMPTZ)
    """
}
