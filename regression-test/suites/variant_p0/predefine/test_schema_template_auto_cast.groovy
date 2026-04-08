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

    // BETWEEN condition
    qt_where_between """ SELECT id FROM ${tableName}
        WHERE data['num_a'] BETWEEN 15 AND 30
        ORDER BY id """

    // IN condition
    qt_where_in """ SELECT id FROM ${tableName}
        WHERE data['str_name'] IN ('alice', 'charlie')
        ORDER BY id """

    // Test 2: ORDER BY with auto-cast
    qt_order_by """ SELECT id, data['num_a'] FROM ${tableName}
        ORDER BY data['num_a'] DESC """

    // ORDER BY expression
    qt_order_by_expr """ SELECT id, data['num_a'] + 1 AS n FROM ${tableName}
        ORDER BY data['num_a'] + 1 DESC """

    // Test 3: TopN (ORDER BY + LIMIT)
    qt_topn """ SELECT id, data['num_a'] FROM ${tableName}
        ORDER BY data['num_a'] DESC LIMIT 2 """

    // Test 4: SELECT with auto-cast (arithmetic operations)
    qt_select_arithmetic """ SELECT id, data['num_a'] + data['num_b'] as sum_val
        FROM ${tableName} ORDER BY id """

    // CASE WHEN with auto-cast
    qt_case_when """ SELECT id,
        CASE WHEN data['num_a'] > 20 THEN 'high' ELSE 'low' END AS level
        FROM ${tableName} ORDER BY id """

    // ORDER BY alias from expression
    qt_order_by_alias_expr """ SELECT data['num_a'] + data['num_b'] AS sum_val FROM ${tableName}
        ORDER BY sum_val """

    // Explicit CAST should still trigger schema template auto cast
    qt_explicit_cast_select """ SELECT CAST(data['num_a'] AS INT) FROM ${tableName} ORDER BY id """
    qt_explicit_cast_where """ SELECT id FROM ${tableName}
        WHERE CAST(data['num_a'] AS INT) > 20 ORDER BY id """
    qt_explicit_cast_order_by """ SELECT id FROM ${tableName}
        ORDER BY CAST(data['num_a'] AS INT) DESC """

    // Test 5: GROUP BY with auto-cast
    qt_group_by """ SELECT data['str_name'], SUM(data['num_a']) as total
        FROM ${tableName} GROUP BY data['str_name'] ORDER BY data['str_name'] """

    // GROUP BY with multiple aggregates
    qt_group_by_multi_agg """ SELECT data['str_name'],
        MIN(data['num_a']) AS min_a, MAX(data['num_a']) AS max_a, COUNT(*) AS cnt
        FROM ${tableName} GROUP BY data['str_name'] ORDER BY data['str_name'] """

    // Test 6: HAVING with auto-cast
    qt_having """ SELECT data['str_name'], SUM(data['num_a']) as total
        FROM ${tableName} GROUP BY data['str_name']
        HAVING SUM(data['num_a']) > 20 ORDER BY data['str_name'] """

    // HAVING with MIN
    qt_having_min """ SELECT data['str_name'], MIN(data['num_a']) AS min_a
        FROM ${tableName} GROUP BY data['str_name']
        HAVING MIN(data['num_a']) >= 15 ORDER BY data['str_name'] """

    // HAVING with non-aggregate expression on group key
    qt_having_non_agg """ SELECT data['str_name'], SUM(data['num_a']) AS total
        FROM ${tableName} GROUP BY data['str_name']
        HAVING data['str_name'] != 'alice' ORDER BY data['str_name'] """

    // Test 7: ORDER BY with alias from project
    qt_order_by_alias """ SELECT data['num_a'] AS num_a FROM ${tableName}
        ORDER BY num_a """

    // Test 8: ORDER BY with alias from subquery
    qt_order_by_alias_subquery """ SELECT * FROM (SELECT id, data['num_a'] AS num_a FROM ${tableName}) t
        ORDER BY num_a, id """

    // Test 9: GROUP BY with alias from subquery
    qt_group_by_alias_subquery """ SELECT num_a, COUNT(*) AS cnt
        FROM (SELECT data['num_a'] AS num_a FROM ${tableName}) t
        GROUP BY num_a ORDER BY num_a """

    // ORDER BY with nested alias
    qt_order_by_alias_nested """ SELECT * FROM (
        SELECT num_a FROM (SELECT data['num_a'] AS num_a FROM ${tableName}) s1
    ) s2 ORDER BY num_a """

    // GROUP BY with nested alias
    qt_group_by_alias_nested """ SELECT num_a, COUNT(*) AS cnt FROM (
        SELECT num_a FROM (SELECT data['num_a'] AS num_a FROM ${tableName}) s1
    ) s2 GROUP BY num_a ORDER BY num_a """

    // Test 10: WINDOW partition/order by with auto-cast
    qt_window_partition_order """ SELECT id,
        row_number() OVER (PARTITION BY data['str_name'] ORDER BY data['num_a']) AS rn
        FROM ${tableName} ORDER BY id """

    // WINDOW aggregate
    qt_window_sum """ SELECT id,
        SUM(data['num_a']) OVER (PARTITION BY data['str_name']) AS s
        FROM ${tableName} ORDER BY id """

    // WINDOW partition + order by with both paths
    qt_window_sum_order """ SELECT id,
        SUM(data['num_a']) OVER (PARTITION BY data['str_name'] ORDER BY data['num_a']) AS s
        FROM ${tableName} ORDER BY id """

    // Aggregates without GROUP BY
    qt_agg_min_max """ SELECT MIN(data['num_a']), MAX(data['num_a']) FROM ${tableName} """
    qt_agg_count_distinct """ SELECT COUNT(DISTINCT data['str_name']) FROM ${tableName} """

    // Test 11: disable auto-cast should error in ORDER BY
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

    // Test 13: JOIN ON with alias from subquery
    qt_join_on_alias_subquery """ SELECT l.id, r.name_val
        FROM (SELECT id, data['key_id'] AS key_id FROM ${leftTable}) l
        JOIN (SELECT id, info['key_id'] AS key_id, info['name_val'] AS name_val FROM ${rightTable}) r
        ON l.key_id = r.key_id
        ORDER BY l.id """

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

    sql """insert into ${leafTable} values
        (1, '{"int_1": 1, "int_nested": {"level1_num_1": 1011111, "level1_num_2": 102}}'),
        (2, '{"int_1": 2, "int_nested": {"level1_num_1": 2022222, "level1_num_2": 202}}'),
        (3, '{"int_1": 1, "int_nested": {"level1_num_1": 3033333, "level1_num_2": 302}}'),
        (4, '{"int_1": 3, "int_nested": {"level1_num_1": 4044444, "level1_num_2": 402}}')"""

    qt_leaf_int1_select """ SELECT data['int_1'] FROM ${leafTable} ORDER BY id """
    qt_leaf_int1_add """ SELECT data['int_1'] + 1 FROM ${leafTable} ORDER BY id """
    // still fails: FE can't distinguish leaf/non-leaf, may cast int_nested to int
    qt_leaf_int_nested_nonleaf """ SELECT data['int_nested'] FROM ${leafTable} ORDER BY id """
    qt_leaf_int_nested_chain_select """ SELECT data['int_nested']['level1_num_1']
        FROM ${leafTable} ORDER BY id """
    qt_leaf_int_nested_dot_select """ SELECT data['int_nested.level1_num_1'] FROM ${leafTable} ORDER BY id """
    qt_leaf_int_nested_deref_select """ SELECT data.int_nested.level1_num_1 FROM ${leafTable} ORDER BY id """
    qt_leaf_int_nested_chain_add """ SELECT data['int_nested']['level1_num_1'] + 1
        FROM ${leafTable} ORDER BY id """
    qt_leaf_int_nested_dot_add """ SELECT data['int_nested.level1_num_1'] + 1
        FROM ${leafTable} ORDER BY id """
    qt_leaf_int_nested_deref_add """ SELECT data.int_nested.level1_num_1 + 1
        FROM ${leafTable} ORDER BY id """

    // Non-select clauses: leaf vs non-leaf
    qt_leaf_where_ok """ SELECT id FROM ${leafTable}
        WHERE data['int_1'] > 0 ORDER BY id """
    qt_leaf_where_nonleaf """ SELECT id FROM ${leafTable}
        WHERE data['int_nested'] > 0 ORDER BY id """
    qt_leaf_where_mixed_1 """ SELECT id FROM ${leafTable}
        WHERE data['int_nested']['level1_num_1'] > 2000000 ORDER BY id """
    qt_leaf_where_mixed_2 """ SELECT id FROM ${leafTable}
        WHERE data['int_nested.level1_num_1'] > 2000000 ORDER BY id """
    qt_leaf_where_mixed_3 """ SELECT id FROM ${leafTable}
        WHERE data.int_nested.level1_num_1 > 2000000 ORDER BY id """
    qt_leaf_order_by_ok """ SELECT id FROM ${leafTable}
        ORDER BY data['int_1'], id """
    qt_leaf_order_by_nonleaf """ SELECT id FROM ${leafTable}
        ORDER BY data['int_nested'], id """
    qt_leaf_order_by_mixed_1 """ SELECT id FROM ${leafTable}
        ORDER BY data['int_nested']['level1_num_1'] """
    qt_leaf_order_by_mixed_2 """ SELECT id FROM ${leafTable}
        ORDER BY data['int_nested.level1_num_1'] """
    qt_leaf_order_by_paren_root """ SELECT id FROM ${leafTable}
        ORDER BY data.int_nested.level1_num_1 """
    qt_leaf_group_by_ok """ SELECT data['int_1'], COUNT(*) AS cnt
        FROM ${leafTable} GROUP BY data['int_1'] ORDER BY data['int_1'] """
    qt_leaf_group_by_nonleaf """ SELECT data['int_nested'], COUNT(*) AS cnt
        FROM ${leafTable} GROUP BY data['int_nested'] ORDER BY data['int_nested'] """
    qt_leaf_group_by_mixed """ SELECT data['int_nested.level1_num_1'], COUNT(*) AS cnt
        FROM ${leafTable} GROUP BY data['int_nested.level1_num_1']
        ORDER BY data['int_nested.level1_num_1'] """
    qt_leaf_having_ok """ SELECT data['int_1'], SUM(data['int_1']) AS total
        FROM ${leafTable} GROUP BY data['int_1']
        HAVING SUM(data['int_1']) > 0 ORDER BY data['int_1'] """
    qt_leaf_having_mixed """ SELECT data['int_nested.level1_num_1'], SUM(data['int_nested.level1_num_1']) AS total
        FROM ${leafTable} GROUP BY data['int_nested.level1_num_1']
        HAVING SUM(data['int_nested.level1_num_1']) > 3000000
        ORDER BY data['int_nested.level1_num_1'] """

    sql "DROP TABLE IF EXISTS ${leafTable}"

    // Test 16: backslash escaping in schema template pattern
    def globWildTable = "test_variant_schema_auto_cast_glob_wild"
    def globLiteralTable = "test_variant_schema_auto_cast_glob_literal"
    def globLiteralPattern = "a\\*b" // SQL sees a\*b, glob sees a\*b (literal *)

    sql "DROP TABLE IF EXISTS ${globWildTable}"
    sql "DROP TABLE IF EXISTS ${globLiteralTable}"

    sql """CREATE TABLE ${globWildTable} (
        `id` bigint NULL,
        `data` variant<'a*b': BIGINT> NOT NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES ( "replication_allocation" = "tag.location.default: 1")"""

    sql """CREATE TABLE ${globLiteralTable} (
        `id` bigint NULL,
        `data` variant<'${globLiteralPattern}': BIGINT> NOT NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES ( "replication_allocation" = "tag.location.default: 1")"""

    sql """insert into ${globWildTable} values(1, '{\"a*b\": 1, \"axb\": 2}')"""
    sql """insert into ${globLiteralTable} values(1, '{\"a*b\": 1, \"axb\": 2}')"""

    // wildcard a*b matches both a*b and axb
    qt_glob_wild_match """ SELECT data['a*b'] + 1 AS v1, data['axb'] + 1 AS v2
        FROM ${globWildTable} ORDER BY id """

    // literal a\*b matches only a*b
    qt_glob_literal_match """ SELECT data['a*b'] + 1 AS v1 FROM ${globLiteralTable} ORDER BY id """
    test {
        sql """ SELECT data['axb'] + 1 FROM ${globLiteralTable} """
        exception "Cannot cast from variant"
    }

    sql "DROP TABLE IF EXISTS ${globWildTable}"
    sql "DROP TABLE IF EXISTS ${globLiteralTable}"


    // Test 17: non-leaf path auto cast limitation
    def nonleafTable = "test_variant_schema_auto_cast_nonleaf_limit"
    sql "DROP TABLE IF EXISTS ${nonleafTable}"
    sql """CREATE TABLE ${nonleafTable} (
        `id` int NULL,
        `data` variant<'int_*': INT> NOT NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES ( "replication_allocation" = "tag.location.default: 1")"""

    sql """insert into ${nonleafTable} values(
        1, '{"int_1": 1, "int_nested": {"level1_num_1": 1011111, "level1_num_2": 102}}')"""

    // auto cast enabled: non-leaf path matches int_* and returns NULL
    sql "set enable_variant_schema_auto_cast = true"
    qt_nonleaf_auto_cast_on """ SELECT data['int_nested'] FROM ${nonleafTable} ORDER BY id """

    // auto cast disabled: return original object
    sql "set enable_variant_schema_auto_cast = false"
    qt_nonleaf_auto_cast_off """ SELECT data['int_nested'] FROM ${nonleafTable} ORDER BY id """

    // restore default
    sql "set enable_variant_schema_auto_cast = true"
    sql "DROP TABLE IF EXISTS ${nonleafTable}"


    // Test 18: multi-layer explicit cast chain (2~4), including MATCH clause
    def castChainTable = "test_variant_schema_auto_cast_cast_chain"
    sql "DROP TABLE IF EXISTS ${castChainTable}"
    sql """CREATE TABLE ${castChainTable} (
        `id` bigint NULL,
        `data` variant<'num_*': BIGINT, 'str_*': STRING> NOT NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 1
    PROPERTIES ( "replication_allocation" = "tag.location.default: 1")"""

    sql """insert into ${castChainTable} values(1, '{\"num_a\": 10, \"num_b\": 20, \"str_name\": \"alice\"}')"""
    sql """insert into ${castChainTable} values(2, '{\"num_a\": 30, \"num_b\": 40, \"str_name\": \"bob\"}')"""
    sql """insert into ${castChainTable} values(3, '{\"num_a\": 50, \"num_b\": 60, \"str_name\": \"charlie\"}')"""
    sql """insert into ${castChainTable} values(4, '{\"num_a\": 15, \"num_b\": 25, \"str_name\": \"alice\"}')"""

    qt_explicit_cast_chain_select_2 """ SELECT CAST(CAST(data['num_a'] AS BIGINT) AS BIGINT)
        FROM ${castChainTable} ORDER BY id """
    qt_explicit_cast_chain_where_3 """ SELECT id FROM ${castChainTable}
        WHERE CAST(CAST(CAST(data['num_a'] AS BIGINT) AS BIGINT) AS BIGINT) > 20 ORDER BY id """
    qt_explicit_cast_chain_order_by_4 """ SELECT id FROM ${castChainTable}
        ORDER BY CAST(CAST(CAST(CAST(data['num_b'] AS BIGINT) AS BIGINT) AS BIGINT) AS BIGINT) DESC, id """
    qt_explicit_cast_chain_group_having_4 """ SELECT
        CAST(CAST(CAST(CAST(data['num_a'] AS BIGINT) AS BIGINT) AS BIGINT) AS BIGINT) AS v, COUNT(*)
        FROM ${castChainTable}
        GROUP BY CAST(CAST(CAST(CAST(data['num_a'] AS BIGINT) AS BIGINT) AS BIGINT) AS BIGINT)
        HAVING CAST(CAST(CAST(CAST(data['num_a'] AS BIGINT) AS BIGINT) AS BIGINT) AS BIGINT) >= 15
        ORDER BY v """

    sql """ set enable_match_without_inverted_index = true """
    qt_explicit_cast_chain_match_2 """ SELECT id FROM ${castChainTable}
        WHERE CAST(CAST(data['str_name'] AS STRING) AS VARCHAR) MATCH 'alice' ORDER BY id """
    qt_explicit_cast_chain_match_4 """ SELECT id FROM ${castChainTable}
        WHERE CAST(CAST(CAST(CAST(data['str_name'] AS STRING) AS VARCHAR) AS STRING) AS VARCHAR) MATCH 'alice' ORDER BY id """
    sql """ set enable_match_without_inverted_index = false """

    sql "DROP TABLE IF EXISTS ${castChainTable}"

}