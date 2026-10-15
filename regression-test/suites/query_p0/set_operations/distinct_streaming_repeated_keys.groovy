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

suite("distinct_streaming_repeated_keys", "query,p0") {
    sql "DROP TABLE IF EXISTS distinct_streaming_repeated_keys"
    sql """
        CREATE TABLE distinct_streaming_repeated_keys (
            id INT NOT NULL,
            s STRING NULL,
            d DATE NOT NULL
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO distinct_streaming_repeated_keys
        SELECT number, IF(number % 5 = 0, NULL, CAST(number AS STRING)), DATE '2026-10-01'
        FROM numbers("number" = "8192")
    """

    // Favor local distinct under the set operation, with a shuffle because id is not a key.
    sql """
        ALTER TABLE distinct_streaming_repeated_keys MODIFY COLUMN s
        SET STATS ('row_count' = '8192', 'ndv' = '1', 'num_nulls' = '1639',
                   'min_value' = '0', 'max_value' = '8191')
    """
    sql """
        ALTER TABLE distinct_streaming_repeated_keys MODIFY COLUMN d
        SET STATS ('row_count' = '8192', 'ndv' = '1', 'num_nulls' = '0',
                   'min_value' = '2026-10-01', 'max_value' = '2026-10-01')
    """
    sql "SET agg_phase = 2"
    sql "SET disable_streaming_preaggregations = false"
    sql "SET enable_distinct_streaming_aggregation = true"
    sql "SET parallel_pipeline_task_num = 1"
    sql "SET batch_size = 256"
    sql "SET enable_spill = true"
    // Switch to pass-through after the first batch has populated the hash table.
    sql "SET spill_streaming_agg_mem_limit = 1"

    explain {
        sql """
            SELECT s, d, d FROM distinct_streaming_repeated_keys WHERE id < 4096
            EXCEPT DISTINCT
            SELECT s, d, d FROM distinct_streaming_repeated_keys WHERE id % 2 = 0
        """
        contains "STREAMING"
    }

    order_qt_except """
        SELECT COUNT(*), MIN(d1), MAX(d2) FROM (
            SELECT s, d AS d1, d AS d2
            FROM distinct_streaming_repeated_keys WHERE id < 4096
            EXCEPT DISTINCT
            SELECT s, d, d FROM distinct_streaming_repeated_keys WHERE id % 2 = 0
        ) t
    """

    // The first date key becomes nullable, exercising the output path without memory reuse.
    order_qt_except_nullable """
        SELECT COUNT(*), COUNT(d1), COUNT(d2), MIN(d1), MAX(d2) FROM (
            SELECT s, d AS d1, d AS d2
            FROM distinct_streaming_repeated_keys WHERE id < 4096
            EXCEPT DISTINCT
            SELECT s, IF(id % 4 = 0, NULL, d), d
            FROM distinct_streaming_repeated_keys WHERE id % 2 = 0
        ) t
    """

    order_qt_intersect """
        SELECT COUNT(*), MIN(d1), MAX(d2) FROM (
            SELECT s, d AS d1, d AS d2
            FROM distinct_streaming_repeated_keys WHERE id < 4096
            INTERSECT DISTINCT
            SELECT s, d, d FROM distinct_streaming_repeated_keys WHERE id % 2 = 0
        ) t
    """

    order_qt_union """
        SELECT COUNT(*), MIN(d1), MAX(d2) FROM (
            SELECT s, d AS d1, d AS d2
            FROM distinct_streaming_repeated_keys WHERE id < 4096
            UNION DISTINCT
            SELECT s, d, d FROM distinct_streaming_repeated_keys WHERE id % 2 = 0
        ) t
    """

    // NormalizeAggregate removes repeated keys in a direct GROUP BY. Keep this as
    // a control case, rather than assuming that repeated SQL text repeats input slots.
    check_sqls_result_equal("""
        SELECT s, d, d, d FROM distinct_streaming_repeated_keys
        GROUP BY s, d, d, d
        ORDER BY 1, 2, 3, 4
    """, """
        SELECT s, d, d, d FROM distinct_streaming_repeated_keys
        GROUP BY s, d
        ORDER BY 1, 2, 3, 4
    """)

    // PushDownLimitDistinctThroughUnion maps distinct union output slots back to
    // the same branch slot after aggregate normalization. Three references to d
    // exercise shared ownership even when the output Block is reused.
    // Use ordinals to group all four union columns despite their repeated names.
    // The limit covers every input row, so it cannot make the result nondeterministic.
    def groupByUnionAllLimit = """
        SELECT * FROM (
            SELECT * FROM (
                SELECT s, d, d, d FROM distinct_streaming_repeated_keys WHERE id < 4096
                UNION ALL
                SELECT s, d, d, d FROM distinct_streaming_repeated_keys WHERE id % 2 = 0
            ) u
            GROUP BY 1, 2, 3, 4
            LIMIT 8192
        ) grouped
        ORDER BY 1, 2, 3, 4
    """

    explain {
        sql groupByUnionAllLimit
        verbose true
        check { plan ->
            // Require three references to one input slot in a streaming aggregate,
            // not merely different slots projecting identical column values.
            (plan =~ /STREAMING\s*\|\s*group by: s\[#\d+\], d\[#(\d+)\], d\[#\1\], d\[#\1\]/).find()
        }
    }

    check_sqls_result_equal(groupByUnionAllLimit, """
        SELECT s, d, d, d FROM distinct_streaming_repeated_keys
        WHERE id < 4096 OR id % 2 = 0
        GROUP BY s, d
        ORDER BY 1, 2, 3, 4
    """)
}
