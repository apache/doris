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

suite("test_datasketches_hll_union_agg_null_ignoring") {
    // The same lgK=8 sketches as test_datasketches_hll_union_agg: {0..6} and {20..29}.
    def sk1Base64 = "AgEHCAMIBwjL18IEK/L7BoYv+Q11gWYHgbxdBntl5gj8LUIK"
    def sk2Base64 = "AwEHCAUIAAkKAAAAIjvrBcS1nwfGGWoEyHokBO8t9wc1qTEENkcJB7hWqQxZf9QNnuSbGA=="

    sql "DROP TABLE IF EXISTS test_datasketches_hll_union_agg_null_ignoring"
    sql """
        CREATE TABLE test_datasketches_hll_union_agg_null_ignoring (
            id INT,
            sk STRING
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_datasketches_hll_union_agg_null_ignoring VALUES
            (1, from_base64('${sk1Base64}')),
            (2, from_base64('${sk2Base64}')),
            (3, NULL),
            (NULL, from_base64('${sk2Base64}'))
    """

    order_qt_mixed_nullable """
        SELECT CAST(ROUND(datasketches_hll_union_agg(sk)) AS BIGINT),
               CAST(ROUND(datasketches_hll_union_agg(sk, 21)) AS BIGINT),
               CAST(ROUND(datasketches_hll_union_agg(sk, 7)) AS BIGINT)
        FROM test_datasketches_hll_union_agg_null_ignoring
    """
    order_qt_all_null """
        SELECT CAST(ROUND(datasketches_hll_union_agg(sk)) AS BIGINT),
               CAST(ROUND(datasketches_hll_union_agg(sk, 21)) AS BIGINT),
               CAST(ROUND(datasketches_hll_union_agg(sk, 7)) AS BIGINT)
        FROM test_datasketches_hll_union_agg_null_ignoring
        WHERE id = 3
    """
    order_qt_empty_input """
        SELECT CAST(ROUND(datasketches_hll_union_agg(sk)) AS BIGINT),
               CAST(ROUND(datasketches_hll_union_agg(sk, 21)) AS BIGINT),
               CAST(ROUND(datasketches_hll_union_agg(sk, 7)) AS BIGINT)
        FROM test_datasketches_hll_union_agg_null_ignoring
        WHERE id = 4
    """

    test {
        sql """
            SELECT datasketches_hll_union_agg(sk, 22)
            FROM test_datasketches_hll_union_agg_null_ignoring
            WHERE id = 3
        """
        exception "requires lg_max_k to be between 7 and 21"
    }
    test {
        sql """
            SELECT datasketches_hll_union_agg(sk, 6)
            FROM test_datasketches_hll_union_agg_null_ignoring
            WHERE id = 4
        """
        exception "requires lg_max_k to be between 7 and 21"
    }

    // Each query has one aggregate output so the single-argument rewrite can apply.
    def queries = [
        single_case: """
            SELECT CAST(ROUND(datasketches_hll_union_agg(CASE WHEN id <= 2 THEN sk END)) AS BIGINT)
            FROM test_datasketches_hll_union_agg_null_ignoring
        """,
        single_if: """
            SELECT CAST(ROUND(datasketches_hll_union_agg(IF(id = 1, sk, NULL))) AS BIGINT)
            FROM test_datasketches_hll_union_agg_null_ignoring
        """,
        single_case_all_null: """
            SELECT CAST(ROUND(datasketches_hll_union_agg(CASE WHEN id = 3 THEN sk END)) AS BIGINT)
            FROM test_datasketches_hll_union_agg_null_ignoring
        """,
        single_case_empty: """
            SELECT CAST(ROUND(datasketches_hll_union_agg(CASE WHEN id = 4 THEN sk END)) AS BIGINT)
            FROM test_datasketches_hll_union_agg_null_ignoring
        """,
        // This union rounds to 18 with cap 7, but 17 if the precision argument is lost.
        two_argument_case: """
            SELECT CAST(ROUND(datasketches_hll_union_agg(CASE WHEN id <= 2 THEN sk END, 7)) AS BIGINT)
            FROM test_datasketches_hll_union_agg_null_ignoring
        """,
        two_argument_if: """
            SELECT CAST(ROUND(datasketches_hll_union_agg(IF(id = 1, sk, NULL), 21)) AS BIGINT)
            FROM test_datasketches_hll_union_agg_null_ignoring
        """,
        two_argument_case_all_null: """
            SELECT CAST(ROUND(datasketches_hll_union_agg(CASE WHEN id = 3 THEN sk END, 7)) AS BIGINT)
            FROM test_datasketches_hll_union_agg_null_ignoring
        """
    ]
    queries.each { tag, query ->
        "order_qt_${tag}" query
        // SET_VAR scopes the comparison to this query and leaves the session unchanged.
        "order_qt_${tag}_without_rewrite" query.replaceFirst(
                "SELECT", "SELECT /*+ SET_VAR(disable_nereids_rules=ELIMINATE_AGG_CASE_WHEN) */")
    }
}
