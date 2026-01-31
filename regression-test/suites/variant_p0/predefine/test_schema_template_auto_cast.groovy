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
    sql """ set enable_variant_schema_auto_cast = true """

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
    sql """ set enable_variant_schema_auto_cast_in_select = true """
    qt_order_by_select_on """ SELECT id, data['num_a'] FROM ${tableName}
        ORDER BY data['num_a'] DESC """
    qt_topn_select_on """ SELECT id, data['num_a'] FROM ${tableName}
        ORDER BY data['num_a'] DESC LIMIT 2 """
    sql """ set enable_variant_schema_auto_cast_in_select = false """

    // Test 4: SELECT with auto-cast (arithmetic operations) when enabled
    sql """ set enable_variant_schema_auto_cast_in_select = true """
    qt_select_arithmetic """ SELECT id, data['num_a'] + data['num_b'] as sum_val
        FROM ${tableName} ORDER BY id """
    sql """ set enable_variant_schema_auto_cast_in_select = false """
    test {
        sql """ SELECT id, data['num_a'] + data['num_b'] as sum_val
            FROM ${tableName} ORDER BY id """
        exception "Cannot cast from variant"
    }

    // Test 5: GROUP BY with auto-cast
    sql """ set enable_variant_schema_auto_cast_in_select = true """
    qt_group_by """ SELECT data['str_name'], SUM(data['num_a']) as total
        FROM ${tableName} GROUP BY data['str_name'] ORDER BY data['str_name'] """

    // Test 6: HAVING with auto-cast
    qt_having """ SELECT data['str_name'], SUM(data['num_a']) as total
        FROM ${tableName} GROUP BY data['str_name']
        HAVING SUM(data['num_a']) > 20 ORDER BY data['str_name'] """
    sql """ set enable_variant_schema_auto_cast_in_select = false """
    test {
        sql """ SELECT data['str_name'], SUM(data['num_a']) as total
            FROM ${tableName} GROUP BY data['str_name'] ORDER BY data['str_name'] """
        exception "sum requires a numeric, boolean or string parameter"
    }
    test {
        sql """ SELECT data['str_name'], SUM(data['num_a']) as total
            FROM ${tableName} GROUP BY data['str_name']
            HAVING SUM(data['num_a']) > 20 ORDER BY data['str_name'] """
        exception "sum requires a numeric, boolean or string parameter"
    }

    // Test 7: ORDER BY with alias from project
    sql """ set enable_variant_schema_auto_cast_in_select = false """
    qt_order_by_alias """ SELECT data['num_a'] AS num_a FROM ${tableName}
        ORDER BY num_a """
    sql """ set enable_variant_schema_auto_cast_in_select = true """
    qt_order_by_alias_select_on """ SELECT data['num_a'] AS num_a FROM ${tableName}
        ORDER BY num_a """
    sql """ set enable_variant_schema_auto_cast_in_select = false """

    // Test 8: ORDER BY with alias from subquery
    qt_order_by_alias_subquery """ SELECT * FROM (SELECT id, data['num_a'] AS num_a FROM ${tableName}) t
        ORDER BY num_a, id """
    sql """ set enable_variant_schema_auto_cast_in_select = true """
    qt_order_by_alias_subquery_select_on """ SELECT * FROM (SELECT id, data['num_a'] AS num_a FROM ${tableName}) t
        ORDER BY num_a, id """
    sql """ set enable_variant_schema_auto_cast_in_select = false """

    // Test 9: GROUP BY with alias from subquery
    test {
        sql """ SELECT num_a, COUNT(*) AS cnt
            FROM (SELECT data['num_a'] AS num_a FROM ${tableName}) t
            GROUP BY num_a ORDER BY num_a """
        exception "must appear in the GROUP BY clause"
    }
    sql """ set enable_variant_schema_auto_cast_in_select = true """
    qt_group_by_alias_subquery_select_on """ SELECT num_a, COUNT(*) AS cnt
        FROM (SELECT data['num_a'] AS num_a FROM ${tableName}) t
        GROUP BY num_a ORDER BY num_a """
    sql """ set enable_variant_schema_auto_cast_in_select = false """

    // Test 10: WINDOW partition/order by with auto-cast
    sql """ set enable_variant_schema_auto_cast_in_select = true """
    qt_window_partition_order """ SELECT id,
        row_number() OVER (PARTITION BY data['str_name'] ORDER BY data['num_a']) AS rn
        FROM ${tableName} ORDER BY id """
    sql """ set enable_variant_schema_auto_cast_in_select = false """

    // Test 11: disable auto-cast should error in non-select clauses
    sql """ set enable_variant_schema_auto_cast = false """
    test {
        sql """ SELECT id FROM ${tableName} ORDER BY data['num_a'] """
        exception "Doris hll, bitmap, array, map, struct, jsonb, variant column must use with specific function"
    }
    sql """ set enable_variant_schema_auto_cast = true """

    sql "DROP TABLE IF EXISTS ${tableName}"

    // Test 12: JOIN ON with auto-cast
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
        `info` variant<'key_*': BIGINT, 'name_*': STRING> NOT NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES ( "replication_allocation" = "tag.location.default: 1")"""

    sql """insert into ${leftTable} values(1, '{"key_id": 100}')"""
    sql """insert into ${leftTable} values(2, '{"key_id": 200}')"""
    sql """insert into ${leftTable} values(3, '{"key_id": 300}')"""

    sql """insert into ${rightTable} values(1, '{"key_id": 100, "name_val": "first"}')"""
    sql """insert into ${rightTable} values(2, '{"key_id": 200, "name_val": "second"}')"""
    sql """insert into ${rightTable} values(3, '{"key_id": 400, "name_val": "fourth"}')"""

    qt_join_on """ SELECT l.id, r.info['name_val']
        FROM ${leftTable} l JOIN ${rightTable} r
        ON l.data['key_id'] = r.info['key_id']
        ORDER BY l.id """
    sql """ set enable_variant_schema_auto_cast_in_select = true """
    qt_join_on_select_on """ SELECT l.id, r.info['name_val']
        FROM ${leftTable} l JOIN ${rightTable} r
        ON l.data['key_id'] = r.info['key_id']
        ORDER BY l.id """
    sql """ set enable_variant_schema_auto_cast_in_select = false """

    // Test 13: JOIN ON with alias from subquery
    qt_join_on_alias_subquery """ SELECT l.id, r.name_val
        FROM (SELECT id, data['key_id'] AS key_id FROM ${leftTable}) l
        JOIN (SELECT id, info['key_id'] AS key_id, info['name_val'] AS name_val FROM ${rightTable}) r
        ON l.key_id = r.key_id
        ORDER BY l.id """
    sql """ set enable_variant_schema_auto_cast_in_select = true """
    qt_join_on_alias_subquery_select_on """ SELECT l.id, r.name_val
        FROM (SELECT id, data['key_id'] AS key_id FROM ${leftTable}) l
        JOIN (SELECT id, info['key_id'] AS key_id, info['name_val'] AS name_val FROM ${rightTable}) r
        ON l.key_id = r.key_id
        ORDER BY l.id """
    sql """ set enable_variant_schema_auto_cast_in_select = false """

    sql "DROP TABLE IF EXISTS ${leftTable}"
    sql "DROP TABLE IF EXISTS ${rightTable}"

    // Test 14: MATCH_NAME and MATCH_NAME_GLOB
    def exactTable = "test_variant_schema_auto_cast_exact"
    sql "DROP TABLE IF EXISTS ${exactTable}"
    sql """CREATE TABLE ${exactTable} (
        `id` bigint NULL,
        `data` variant<'exact_key': BIGINT, 'glob_*': BIGINT> NOT NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES ( "replication_allocation" = "tag.location.default: 1")"""

    sql """insert into ${exactTable} values(1, '{"exact_key": 10, "glob_1": 20, "glob_2": 5}')"""
    sql """insert into ${exactTable} values(2, '{"exact_key": 30, "glob_2": 40}')"""

    qt_match_name_exact_where """ SELECT id FROM ${exactTable}
        WHERE data['exact_key'] > 10 ORDER BY id """
    qt_match_name_glob_where """ SELECT id FROM ${exactTable}
        WHERE data['glob_1'] >= 20 ORDER BY id """
    qt_match_name_exact_order """ SELECT id FROM ${exactTable}
        ORDER BY data['exact_key'] """
    qt_match_name_glob_order """ SELECT id FROM ${exactTable}
        ORDER BY data['glob_2'], id """

    sql "DROP TABLE IF EXISTS ${exactTable}"

    // Test 15: leaf vs non-leaf path auto cast limitation
    def leafTable = "test_variant_schema_auto_cast_leaf"
    sql "DROP TABLE IF EXISTS ${leafTable}"
    sql """CREATE TABLE ${leafTable} (
        `id` bigint NULL,
        `data` variant<'int_*': BIGINT> NOT NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES ( "replication_allocation" = "tag.location.default: 1")"""

    sql """insert into ${leafTable} values(
        1,
        '{"int_1": 1, "int_nested": {"level1_num_1": 1011111, "level1_num_2": 102}}'
    )"""

    sql """ set enable_variant_schema_auto_cast_in_select = true """
    qt_leaf_int1_select_on """ SELECT data['int_1'] FROM ${leafTable} ORDER BY id """
    qt_leaf_int1_add_select_on """ SELECT data['int_1'] + 1 FROM ${leafTable} ORDER BY id """
    test {
        // still fails: FE can't distinguish leaf/non-leaf, may cast int_nested to int
        sql """ SELECT data['int_nested'] FROM ${leafTable} """
        exception "Bad cast"
    }
    qt_leaf_int_nested_chain_select_on """ SELECT data['int_nested']['level1_num_1']
        FROM ${leafTable} ORDER BY id """
    qt_leaf_int_nested_dot_select_on """ SELECT data['int_nested.level1_num_1'] FROM ${leafTable} ORDER BY id """
    qt_leaf_int_nested_deref_select_on """ SELECT data.int_nested.level1_num_1 FROM ${leafTable} ORDER BY id """
    qt_leaf_int_nested_chain_add_select_on """ SELECT data['int_nested']['level1_num_1'] + 1
        FROM ${leafTable} ORDER BY id """
    qt_leaf_int_nested_dot_add_select_on """ SELECT data['int_nested.level1_num_1'] + 1
        FROM ${leafTable} ORDER BY id """
    
    sql """ set enable_variant_schema_auto_cast_in_select = false """
    qt_leaf_int1_select_off """ SELECT data['int_1'] FROM ${leafTable} ORDER BY id """
    test {
        sql """ SELECT data['int_1'] + 1 FROM ${leafTable} ORDER BY id """
        exception "Cannot cast from variant"
    }
    sql """ SELECT data['int_nested'] FROM ${leafTable} """
    qt_leaf_int_nested_chain_select_off """ SELECT data['int_nested']['level1_num_1']
        FROM ${leafTable} ORDER BY id """
    qt_leaf_int_nested_dot_select_off """ SELECT data['int_nested.level1_num_1']
        FROM ${leafTable} ORDER BY id """
    qt_leaf_int_nested_deref_select_off """ SELECT data.int_nested.level1_num_1 FROM ${leafTable} ORDER BY id """
    test {
        sql """ SELECT data['int_nested']['level1_num_1'] + 1 FROM ${leafTable} ORDER BY id """
        exception "Cannot cast from variant"
    }
    test {
        sql """ SELECT data['int_nested.level1_num_1'] + 1 FROM ${leafTable} ORDER BY id """
        exception "Cannot cast from variant"
    }

    sql "DROP TABLE IF EXISTS ${leafTable}"
}
