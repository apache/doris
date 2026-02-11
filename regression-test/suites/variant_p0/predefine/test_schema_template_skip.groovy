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

suite("test_schema_template_skip", "p0") {
    sql """ set describe_extend_variant_column = true """
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    sql """ set default_variant_enable_doc_mode = false """

    // Test 1: Basic SKIP glob
    def tableName1 = "test_skip_basic_glob"
    sql "DROP TABLE IF EXISTS ${tableName1}"
    sql """CREATE TABLE ${tableName1} (
        `id` bigint NULL,
        `data` variant<SKIP 'debug_*'> NOT NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES ( "replication_allocation" = "tag.location.default: 1")"""

    sql """insert into ${tableName1} values(1, '{"debug_info":"secret","debug_trace":"trace_val","normal_field":"visible"}')"""
    sql """insert into ${tableName1} values(2, '{"debug_level":5,"keep_me":"yes"}')"""

    qt_skip_basic_glob_1 """ SELECT id, data['normal_field'] FROM ${tableName1} ORDER BY id """
    qt_skip_basic_glob_2 """ SELECT id, data['debug_info'] FROM ${tableName1} ORDER BY id """
    qt_skip_basic_glob_3 """ SELECT id, data['debug_trace'] FROM ${tableName1} ORDER BY id """
    qt_skip_basic_glob_4 """ SELECT id, data['keep_me'] FROM ${tableName1} ORDER BY id """

    // Test 2: SKIP MATCH_NAME exact match
    def tableName2 = "test_skip_match_name"
    sql "DROP TABLE IF EXISTS ${tableName2}"
    sql """CREATE TABLE ${tableName2} (
        `id` bigint NULL,
        `data` variant<SKIP MATCH_NAME 'secret'> NOT NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES ( "replication_allocation" = "tag.location.default: 1")"""

    sql """insert into ${tableName2} values(1, '{"secret":"hidden","secret_key":"visible","public":"open"}')"""

    qt_skip_match_name_1 """ SELECT id, data['secret'] FROM ${tableName2} ORDER BY id """
    qt_skip_match_name_2 """ SELECT id, data['secret_key'] FROM ${tableName2} ORDER BY id """
    qt_skip_match_name_3 """ SELECT id, data['public'] FROM ${tableName2} ORDER BY id """

    // Test 3: Nested path SKIP
    def tableName3 = "test_skip_nested_path"
    sql "DROP TABLE IF EXISTS ${tableName3}"
    sql """CREATE TABLE ${tableName3} (
        `id` bigint NULL,
        `data` variant<SKIP 'a.b.temp_?'> NOT NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES ( "replication_allocation" = "tag.location.default: 1")"""

    sql """insert into ${tableName3} values(1, '{"a":{"b":{"temp_1":1,"temp_12":10,"keep":2}}}')"""

    qt_skip_nested_1 """ SELECT id, data['a']['b']['temp_1'] FROM ${tableName3} ORDER BY id """
    qt_skip_nested_2 """ SELECT id, data['a']['b']['keep'] FROM ${tableName3} ORDER BY id """
    // temp_12 has 2 chars after temp_, so '?' should NOT match it
    qt_skip_nested_3 """ SELECT id, data['a']['b']['temp_12'] FROM ${tableName3} ORDER BY id """

    // Test 4: SKIP takes priority over typed pattern
    def tableName4 = "test_skip_priority"
    sql "DROP TABLE IF EXISTS ${tableName4}"
    sql """CREATE TABLE ${tableName4} (
        `id` bigint NULL,
        `data` variant<SKIP 'num_*', 'num_*': BIGINT> NOT NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES ( "replication_allocation" = "tag.location.default: 1")"""

    sql """insert into ${tableName4} values(1, '{"num_a":100,"other":"val"}')"""

    qt_skip_priority_1 """ SELECT id, data['num_a'] FROM ${tableName4} ORDER BY id """
    qt_skip_priority_2 """ SELECT id, data['other'] FROM ${tableName4} ORDER BY id """

    // Test 5: Invalid glob DDL rejection
    test {
        sql """CREATE TABLE test_skip_invalid_glob (
            `id` bigint NULL,
            `data` variant<SKIP '[invalid'> NOT NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1")"""
        exception "Invalid glob pattern"
    }

    // Test 6: Glob cross-level matching — pattern spans nested path
    def tableName6 = "test_skip_glob_cross_level"
    sql "DROP TABLE IF EXISTS ${tableName6}"
    sql """CREATE TABLE ${tableName6} (
        `id` bigint NULL,
        `data` variant<SKIP 'a.debug_*'> NOT NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES ( "replication_allocation" = "tag.location.default: 1")"""

    sql """insert into ${tableName6} values(1, '{"a":{"debug_x":1,"keep":2},"debug_y":3}')"""

    qt_skip_glob_cross_1 """ SELECT id, data['a']['debug_x'] FROM ${tableName6} ORDER BY id """
    qt_skip_glob_cross_2 """ SELECT id, data['a']['keep'] FROM ${tableName6} ORDER BY id """
    qt_skip_glob_cross_3 """ SELECT id, data['debug_y'] FROM ${tableName6} ORDER BY id """

    // Test 7: Multiple SKIP patterns
    def tableName7 = "test_skip_multiple"
    sql "DROP TABLE IF EXISTS ${tableName7}"
    sql """CREATE TABLE ${tableName7} (
        `id` bigint NULL,
        `data` variant<SKIP 'temp_*', SKIP 'internal_*', SKIP MATCH_NAME 'password'> NOT NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES ( "replication_allocation" = "tag.location.default: 1")"""

    sql """insert into ${tableName7} values(1, '{"temp_data":"t","internal_id":1,"password":"secret","name":"visible"}')"""

    qt_skip_multi_1 """ SELECT id, data['temp_data'] FROM ${tableName7} ORDER BY id """
    qt_skip_multi_2 """ SELECT id, data['internal_id'] FROM ${tableName7} ORDER BY id """
    qt_skip_multi_3 """ SELECT id, data['password'] FROM ${tableName7} ORDER BY id """
    qt_skip_multi_4 """ SELECT id, data['name'] FROM ${tableName7} ORDER BY id """

    // Test 8: SELECT whole column — skipped fields should not appear in JSON output
    qt_skip_whole_col """ SELECT id, data FROM ${tableName1} ORDER BY id """

    // Test 9: SKIP with non-conflicting typed pattern coexistence
    def tableName9 = "test_skip_coexist"
    sql "DROP TABLE IF EXISTS ${tableName9}"
    sql """CREATE TABLE ${tableName9} (
        `id` bigint NULL,
        `data` variant<SKIP 'debug_*', 'num_*': BIGINT> NOT NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES ( "replication_allocation" = "tag.location.default: 1")"""

    sql """insert into ${tableName9} values(1, '{"debug_x":1,"num_a":100,"other":"val"}')"""

    qt_skip_coexist_1 """ SELECT id, data['debug_x'] FROM ${tableName9} ORDER BY id """
    qt_skip_coexist_2 """ SELECT id, data['num_a'] FROM ${tableName9} ORDER BY id """
    qt_skip_coexist_3 """ SELECT id, data['other'] FROM ${tableName9} ORDER BY id """

    // Test 10: Empty pattern rejection
    test {
        sql """CREATE TABLE test_skip_empty_pattern (
            `id` bigint NULL,
            `data` variant<SKIP ''> NOT NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1")"""
        exception "pattern"
    }

    // Test 11: Bulk data — verify SKIP works correctly with larger dataset
    def tableName11 = "test_skip_bulk"
    sql "DROP TABLE IF EXISTS ${tableName11}"
    sql """CREATE TABLE ${tableName11} (
        `id` bigint NULL,
        `data` variant<SKIP 'debug_*', SKIP MATCH_NAME 'internal'> NOT NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES ( "replication_allocation" = "tag.location.default: 1")"""

    // Insert 100 rows with mixed fields
    for (int i = 1; i <= 100; i++) {
        sql """insert into ${tableName11} values(${i},
            '{"debug_id":${i},"debug_msg":"msg_${i}","internal":"secret_${i}","name":"user_${i}","value":${i * 10}}')"""
    }

    // Skipped fields should all be NULL
    qt_skip_bulk_1 """ SELECT count(*) FROM ${tableName11} WHERE data['debug_id'] IS NOT NULL """
    qt_skip_bulk_2 """ SELECT count(*) FROM ${tableName11} WHERE data['debug_msg'] IS NOT NULL """
    qt_skip_bulk_3 """ SELECT count(*) FROM ${tableName11} WHERE data['internal'] IS NOT NULL """
    // Non-skipped fields should all be present
    qt_skip_bulk_4 """ SELECT count(*) FROM ${tableName11} WHERE data['name'] IS NOT NULL """
    qt_skip_bulk_5 """ SELECT count(*) FROM ${tableName11} WHERE data['value'] IS NOT NULL """
    // Spot check specific rows
    qt_skip_bulk_6 """ SELECT id, data['name'], data['value'] FROM ${tableName11} WHERE id IN (1, 50, 100) ORDER BY id """
}
