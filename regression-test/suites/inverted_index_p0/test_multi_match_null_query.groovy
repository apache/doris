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

// Regression test for: NULL query string reaches inverted index evaluate_inverted_index.
// Without FE constant folding, NULL propagates to the BE inverted index path.
// Expected: returns 0 rows without error (null query matches nothing).

suite("test_multi_match_null_query", "p0") {
    sql "DROP TABLE IF EXISTS test_multi_match_null_query"
    sql """
        CREATE TABLE test_multi_match_null_query (
            id INT,
            body TEXT,
            INDEX body_idx (body) USING INVERTED PROPERTIES("parser" = "english")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql "INSERT INTO test_multi_match_null_query VALUES (1, 'hello world'), (2, 'foo bar')"

    // Disable FE constant folding so NULL is not optimized to VEMPTYSET,
    // forcing the predicate to reach the BE inverted index path (pushAggOp=COUNT_ON_INDEX).
    sql "SET disable_nereids_expression_rules = 'FOLD_CONSTANT_ON_FE'"
    sql "SET enable_fold_constant_by_be = true"
    sql "SET enable_sql_cache = false"
    sql "SET enable_common_expr_pushdown = true"
    sql "SET enable_common_expr_pushdown_for_inverted_index = true"

    qt_sql "SELECT count() FROM test_multi_match_null_query WHERE body MATCH_ALL NULL"
    qt_sql "SELECT count() FROM test_multi_match_null_query WHERE body MATCH_ANY NULL"
    qt_sql "SELECT count() FROM test_multi_match_null_query WHERE body MATCH_PHRASE NULL"
    qt_sql "SELECT count() FROM test_multi_match_null_query WHERE multi_match(body, 'phrase_prefix', NULL)"
    qt_sql "SELECT count() FROM test_multi_match_null_query WHERE multi_match(body, 'phrase', NULL)"
    qt_sql "SELECT count() FROM test_multi_match_null_query WHERE multi_match(body, 'any', NULL)"
    qt_sql "SELECT count() FROM test_multi_match_null_query WHERE multi_match(body, 'all', NULL)"

    test {
        sql "SELECT count() FROM test_multi_match_null_query WHERE multi_match(body, NULL, 'abc')"
        exception "query_type can not be NULL"
    }

    sql "DROP TABLE IF EXISTS test_multi_match_null_query_parser_none"
    sql """
        CREATE TABLE test_multi_match_null_query_parser_none (
            id INT,
            body TEXT,
            INDEX body_idx (body) USING INVERTED PROPERTIES("parser" = "none")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql "INSERT INTO test_multi_match_null_query_parser_none VALUES (1, ''), (2, NULL), (3, 'hello')"
    qt_sql """
        SELECT id FROM test_multi_match_null_query_parser_none
        WHERE multi_match(body, 'any', '')
        ORDER BY id
    """
    qt_sql """
        SELECT count() FROM test_multi_match_null_query_parser_none
        WHERE multi_match(body, 'any', NULL)
    """
}
