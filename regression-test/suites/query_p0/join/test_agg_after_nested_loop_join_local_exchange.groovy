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

suite("test_agg_after_nested_loop_join_local_exchange", "query_p0") {
    sql "DROP TABLE IF EXISTS test_agg_after_nlj_local_exchange_t1"
    sql "DROP TABLE IF EXISTS test_agg_after_nlj_local_exchange_t2"

    sql """
        CREATE TABLE test_agg_after_nlj_local_exchange_t1 (
            col_bigint_undef_signed BIGINT,
            col_varchar_10__undef_signed VARCHAR(10),
            col_varchar_64__undef_signed VARCHAR(64),
            pk INT
        )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(pk) BUCKETS 10
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO test_agg_after_nlj_local_exchange_t1
        (pk, col_bigint_undef_signed, col_varchar_10__undef_signed, col_varchar_64__undef_signed)
        VALUES
            (0, -94, 'had', 'y'),
            (1, 672609, 'k', 'h'),
            (2, -3766684, 'a', 'p'),
            (3, 5070261, 'on', 'x'),
            (4, NULL, 'u', 'at'),
            (5, -86, 'v', 'c'),
            (6, 21910, 'how', 'm'),
            (7, -63, 'that''s', 'go'),
            (8, -8276281, 's', 'a'),
            (9, -101, 'w', 'y')
    """

    sql """
        CREATE TABLE test_agg_after_nlj_local_exchange_t2 (
            pk INT,
            col_varchar_10__undef_signed VARCHAR(10),
            col_bigint_undef_signed BIGINT,
            col_varchar_64__undef_signed VARCHAR(64)
        )
        ENGINE=OLAP
        DUPLICATE KEY(pk, col_varchar_10__undef_signed)
        DISTRIBUTED BY HASH(pk) BUCKETS 10
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO test_agg_after_nlj_local_exchange_t2
        (pk, col_bigint_undef_signed, col_varchar_10__undef_signed, col_varchar_64__undef_signed)
        VALUES
            (0, NULL, 'right', 'g'),
            (1, -486256, 'on', 'on'),
            (2, -1, 'I''ll', 'at'),
            (3, 29263, 'h', 'don''t'),
            (4, 5453, 'a', 's'),
            (5, -119, 'j', 'can''t'),
            (6, 89, 'one', 'n'),
            (7, -7227, 's', 'u'),
            (8, 94, 'time', 'b'),
            (9, 1816630, 'yes', 'yes')
    """

    sql "SYNC"

    sql "SET default_variant_doc_hash_shard_count = 0"
    sql "SET default_variant_max_subcolumns_count = 4"
    sql "SET default_variant_sparse_hash_shard_count = 4"
    sql "SET disable_join_reorder = true"
    sql "SET disable_streaming_preaggregations = true"
    sql "SET enable_segment_limit_pushdown = false"
    sql "SET enable_distinct_streaming_agg_force_passthrough = false"
    sql "SET enable_function_pushdown = true"
    sql "SET enable_local_exchange_before_agg = false"
    sql "SET enable_runtime_filter_partition_prune = false"
    sql "SET enable_runtime_filter_prune = false"
    sql "SET enable_strong_consistency_read = true"
    sql "SET enable_sync_runtime_filter_size = false"
    sql "SET exchange_multi_blocks_byte_size = 5563624"
    sql "SET experimental_enable_parallel_scan = false"
    sql "SET parallel_pipeline_task_num = 4"
    sql "SET parallel_prepare_threshold = 28"
    sql "SET query_timeout = 600"
    sql "SET runtime_filter_type = 'IN,MIN_MAX'"
    sql "SET runtime_filter_wait_time_ms = 5000"
    sql "SET topn_opt_limit_threshold = 1000"
    sql "SET agg_phase = 4"

    order_qt_agg_after_nlj_local_exchange """
        SELECT
            COUNT(DISTINCT table1.`pk`) AS field1,
            MAX(table1.col_bigint_undef_signed) AS field2
        FROM
            test_agg_after_nlj_local_exchange_t1 AS table1
        LEFT OUTER JOIN test_agg_after_nlj_local_exchange_t2 AS table2
            ON table2.col_varchar_10__undef_signed = table2.col_varchar_64__undef_signed
        LEFT JOIN test_agg_after_nlj_local_exchange_t1 AS table3
            ON table2.col_varchar_10__undef_signed = table2.col_varchar_64__undef_signed
        WHERE
            table1.`pk` > 3
            AND table1.`pk` < (3 + 25)
            OR table1.col_varchar_64__undef_signed > 'cnvUBxJyCp'
            AND table1.col_varchar_64__undef_signed <= 'z'
            OR table1.col_bigint_undef_signed != 2
            OR table1.`pk` NOT BETWEEN 2 AND (2 + 1)
            AND table1.`pk` > 7
            AND table1.`pk` < (7 + 2)
            AND table1.`pk` IN (2, 10, 2)
        ORDER BY
            field1,
            field2
        LIMIT 1000
    """
}
