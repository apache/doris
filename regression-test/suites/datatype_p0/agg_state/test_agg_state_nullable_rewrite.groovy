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

suite("test_agg_state_nullable_rewrite") {
    sql "set enable_agg_state=true"

    // Folding the string casts must preserve the nullable inputs in the state layout.
    for (def producer : ["state", "combine"]) {
        "order_qt_${producer}_compatible"("""
            SELECT percentile_reservoir_merge(s)
            FROM (
                SELECT percentile_reservoir_${producer}(cast('7' AS double), 0.25) s
                UNION ALL
                SELECT percentile_reservoir_${producer}(cast('8' AS double), 0.25) s
            ) states
        """)
        "order_qt_${producer}_union"("""
            SELECT percentile_reservoir_merge(s)
            FROM (
                SELECT percentile_reservoir_union(s) s
                FROM (
                    SELECT percentile_reservoir_${producer}(cast('7' AS double), 0.25) s
                    UNION ALL
                    SELECT percentile_reservoir_${producer}(cast('8' AS double), 0.25) s
                ) states
            ) united
        """)
        for (def levels : [["0.25", "0.75"], ["0.75", "0.25"]]) {
            for (def consumer : ["merge", "union"]) {
                test {
                    sql """
                        SELECT percentile_reservoir_${consumer}(s)
                        FROM (
                            SELECT percentile_reservoir_${producer}(cast('7' AS double), ${levels[0]}) s
                            UNION ALL
                            SELECT percentile_reservoir_${producer}(cast('8' AS double), ${levels[1]}) s
                        ) states
                    """
                    exception "incompatible"
                }
            }
        }
        // NaN contributes no sample, so an empty state cannot constrain the quantile.
        for (def value : ["NaN", "7"]) {
            for (def emptyFirst : [true, false]) {
                def empty = "SELECT percentile_reservoir_${producer}(cast('NaN' AS double), 0.0) s"
                def other = "SELECT percentile_reservoir_${producer}(cast('${value}' AS double), 0.75) s"
                def branches = emptyFirst ? "${empty} UNION ALL ${other}" : "${other} UNION ALL ${empty}"
                "order_qt_${producer}_empty_${value}_${emptyFirst}"("""
                    SELECT percentile_reservoir_merge(s) FROM (${branches}) states
                """)
                "order_qt_${producer}_empty_union_${value}_${emptyFirst}"("""
                    SELECT percentile_reservoir_merge(s)
                    FROM (SELECT percentile_reservoir_union(s) s FROM (${branches}) states) united
                """)
            }
        }
    }

    sql "DROP TABLE IF EXISTS test_agg_state_nullable_input"
    sql """
        CREATE TABLE test_agg_state_nullable_input (id INT, v DOUBLE NULL)
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO test_agg_state_nullable_input VALUES (1, 8), (2, NULL)"
    for (def producer : ["state", "combine"]) {
        for (def constantFirst : [true, false]) {
            def constant = "SELECT percentile_reservoir_${producer}(cast('7' AS double), 0.25) s"
            def table = """
                SELECT percentile_reservoir_${producer}(v, 0.25) s
                FROM test_agg_state_nullable_input
            """
            def branches = constantFirst ? "${constant} UNION ALL ${table}" : "${table} UNION ALL ${constant}"
            "order_qt_${producer}_mixed_${constantFirst}"("""
                SELECT percentile_reservoir_merge(s) FROM (${branches}) states
            """)
        }
    }

    for (def phase : [1, 2]) {
        "order_qt_combine_phase_${phase}"("""
            SELECT /*+ SET_VAR(agg_phase=${phase}) */ percentile_reservoir_merge(s)
            FROM (
                SELECT id, percentile_reservoir_combine(cast('7' AS double), 0.25) s
                FROM test_agg_state_nullable_input GROUP BY id
            ) states
        """)
    }
    order_qt_combine_parameter_cast """
        SELECT topn_merge(s) FROM (
            SELECT topn_combine('a', cast('1' AS int)) s
            FROM test_agg_state_nullable_input
        ) states
    """

    // The folded arguments are equal, but the two state layouts must not be deduplicated.
    for (def producer : ["state", "combine"]) {
        "order_qt_${producer}_different_layouts"("""
            SELECT percentile_reservoir_merge(nullable_state), percentile_reservoir_merge(nonnull_state)
            FROM (
                SELECT percentile_reservoir_${producer}(cast('7' AS double), 0.25) nullable_state,
                       percentile_reservoir_${producer}(cast(7 AS double), 0.25) nonnull_state
                UNION ALL
                SELECT percentile_reservoir_${producer}(cast('8' AS double), 0.25) nullable_state,
                       percentile_reservoir_${producer}(cast(8 AS double), 0.25) nonnull_state
            ) states
        """)
    }

    order_qt_null_input """
        SELECT percentile_reservoir_merge(s)
        FROM (
            SELECT percentile_reservoir_state(cast(NULL AS double), 0.25) s
            UNION ALL
            SELECT percentile_reservoir_state(cast('7' AS double), 0.25) s
        ) states
    """
    order_qt_sum """
        SELECT sum_merge(s) FROM (
            SELECT sum_state(cast('7' AS int)) s
            UNION ALL
            SELECT sum_state(cast('8' AS int)) s
        ) states
    """

    // Explicit casts must still retarget the producer, including when inserting into a state column.
    sql "DROP TABLE IF EXISTS test_agg_state_nullable_cast"
    sql """
        CREATE TABLE test_agg_state_nullable_cast (
            id INT,
            nullable_state AGG_STATE<percentile_reservoir(DOUBLE NULL, DOUBLE NOT NULL)> GENERIC,
            nonnull_state AGG_STATE<percentile_reservoir(DOUBLE NOT NULL, DOUBLE NOT NULL)> GENERIC,
            widened_state AGG_STATE<sum(BIGINT NOT NULL)> GENERIC
        ) AGGREGATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_agg_state_nullable_cast
        SELECT 1, percentile_reservoir_state(cast(7 AS double), 0.25),
               percentile_reservoir_state(cast('7' AS double), 0.25), sum_state(cast(7 AS int))
    """
    sql """
        INSERT INTO test_agg_state_nullable_cast
        SELECT 2, percentile_reservoir_state(cast(8 AS double), 0.25),
               percentile_reservoir_state(cast('8' AS double), 0.25), sum_state(cast(8 AS int))
    """
    order_qt_stored_cast """
        SELECT percentile_reservoir_merge(nullable_state), percentile_reservoir_merge(nonnull_state),
               sum_merge(widened_state)
        FROM test_agg_state_nullable_cast
    """
}
