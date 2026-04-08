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

suite("test_disable_root_variant_match", "p0") {
    sql """ set enable_match_without_inverted_index = false """
    sql """ set enable_common_expr_pushdown = true """
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    sql """ set default_variant_enable_doc_mode = false """

    sql "DROP TABLE IF EXISTS test_disable_root_variant_match_tbl"

    sql """
        CREATE TABLE test_disable_root_variant_match_tbl (
            `id` INT NOT NULL,
            `response` variant<
                MATCH_NAME 'msg' : string,
                properties("variant_max_subcolumns_count" = "16")
            > NULL,
            INDEX idx_response (response) USING INVERTED PROPERTIES(
                "parser" = "unicode",
                "field_pattern" = "msg",
                "lower_case" = "true"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        )
    """

    sql """INSERT INTO test_disable_root_variant_match_tbl VALUES
        (1, '{"msg": "doris community"}'),
        (2, '{"msg": "apache software"}'),
        (3, '{"msg": "doris variant index"}')
    """

    sql "sync"
    Thread.sleep(5000)

    test {
        sql """
            SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id
            FROM test_disable_root_variant_match_tbl
            WHERE response MATCH 'doris'
            ORDER BY id
        """
        exception "VARIANT root column does not support MATCH"
    }

    def variantSubcolumnMatchResult = sql """
        SELECT /*+SET_VAR(enable_common_expr_pushdown=true)*/ id
        FROM test_disable_root_variant_match_tbl
        WHERE response['msg'] MATCH 'doris'
        ORDER BY id
    """
    assertEquals([[1], [3]], variantSubcolumnMatchResult)
}
