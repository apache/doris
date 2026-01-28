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

suite("test_schema_template_auto_cast", "p0") {
    sql """ set describe_extend_variant_column = true """
    sql """ set enable_match_without_inverted_index = false """
    sql """ set enable_common_expr_pushdown = true """
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    sql """ set default_variant_enable_doc_mode = false """

    def tableName = "test_variant_schema_auto_cast"

    // Test 1: WHERE clause with auto-cast
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """CREATE TABLE ${tableName} (
        `id` bigint NULL,
        `data` variant<'num_*': BIGINT, 'str_*': STRING> NOT NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES ( "replication_allocation" = "tag.location.default: 1")"""

    sql """insert into ${tableName} values(1, '{"num_a": 10, "num_b": 20, "str_name": "alice"}')"""
    sql """insert into ${tableName} values(2, '{"num_a": 30, "num_b": 40, "str_name": "bob"}')"""
    sql """insert into ${tableName} values(3, '{"num_a": 50, "num_b": 60, "str_name": "charlie"}')"""
    sql """insert into ${tableName} values(4, '{"num_a": 15, "num_b": 25, "str_name": "alice"}')"""

    // Simple WHERE
    qt_where_simple """ SELECT id FROM ${tableName}
        WHERE data['num_a'] > 20 ORDER BY id """

    // AND condition
    qt_where_and """ SELECT id FROM ${tableName}
        WHERE data['num_a'] > 10 AND data['num_b'] < 50
        ORDER BY id """

    // OR condition
    qt_where_or """ SELECT id FROM ${tableName}
        WHERE data['num_a'] > 40 OR data['str_name'] = 'alice'
        ORDER BY id """

    // Test 2: ORDER BY with auto-cast
    qt_order_by """ SELECT id, data['num_a'] FROM ${tableName}
        ORDER BY data['num_a'] DESC """

    // Test 3: TopN (ORDER BY + LIMIT)
    qt_topn """ SELECT id, data['num_a'] FROM ${tableName}
        ORDER BY data['num_a'] DESC LIMIT 2 """

    // Test 4: SELECT projection with aliases
    qt_select_alias """ SELECT id,
        data['num_a'] as num_a_val,
        data['str_name'] as name
        FROM ${tableName}
        ORDER BY id """

    // Test 5: JOIN ON clause
    def leftTable = "test_variant_join_left"
    def rightTable = "test_variant_join_right"

    sql "DROP TABLE IF EXISTS ${leftTable}"
    sql "DROP TABLE IF EXISTS ${rightTable}"

    sql """CREATE TABLE ${leftTable} (
        `id` bigint NULL,
        `data` variant<'key_*': BIGINT> NOT NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES ( "replication_allocation" = "tag.location.default: 1")"""

    sql """CREATE TABLE ${rightTable} (
        `id` bigint NULL,
        `info` variant<'key_*': BIGINT> NOT NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES ( "replication_allocation" = "tag.location.default: 1")"""

    sql """insert into ${leftTable} values(1, '{"key_val": 100}')"""
    sql """insert into ${leftTable} values(2, '{"key_val": 200}')"""
    sql """insert into ${leftTable} values(3, '{"key_val": 300}')"""

    sql """insert into ${rightTable} values(1, '{"key_val": 100}')"""
    sql """insert into ${rightTable} values(2, '{"key_val": 250}')"""
    sql """insert into ${rightTable} values(3, '{"key_val": 300}')"""

    qt_join_on """ SELECT l.id, l.data['key_val'], r.info['key_val']
        FROM ${leftTable} l JOIN ${rightTable} r
        ON l.data['key_val'] = r.info['key_val']
        ORDER BY l.id """

    sql "DROP TABLE IF EXISTS ${leftTable}"
    sql "DROP TABLE IF EXISTS ${rightTable}"
    sql "DROP TABLE IF EXISTS ${tableName}"
}
