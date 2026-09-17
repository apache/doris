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

// Sliding ROWS frames over floating-point columns must not carry the rounding
// loss of a value that already left the frame into the current result.
suite("test_window_float_sliding_frame") {
    sql "DROP TABLE IF EXISTS test_window_float_sliding_frame"
    sql """
        CREATE TABLE test_window_float_sliding_frame (
            id INT,
            grp INT,
            v_double DOUBLE,
            v_float FLOAT,
            v_nullable DOUBLE NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    // grp 1: a large value (2^54) leaves the frame, then only small values remain.
    // grp 2: sign flip of a large value around small values.
    // grp 3: consecutive small values after a large one, with NULL in the nullable column.
    sql """
        INSERT INTO test_window_float_sliding_frame VALUES
        (1, 1, 18014398509481984, 16777216, 18014398509481984),
        (2, 1, 1, 1, 1),
        (3, 1, 1, 1, 1),
        (4, 1, 1, 1, NULL),
        (5, 2, 18014398509481984, 16777216, 18014398509481984),
        (6, 2, 1, 1, 1),
        (7, 2, -18014398509481984, -16777216, -18014398509481984),
        (8, 2, 1, 1, 1),
        (9, 2, 1, 1, 1),
        (10, 3, 18014398509481984, 16777216, NULL),
        (11, 3, 1, 1, 1),
        (12, 3, 1, 1, 1),
        (13, 3, 1, 1, 1),
        (14, 3, 1, 1, 1)
    """

    // The reported case: after row 1 leaves the frame, avg over [1, 1] must be 1.
    order_qt_avg_double_preceding_1 """
        SELECT id, v_double,
               avg(v_double) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS got
        FROM test_window_float_sliding_frame
        WHERE grp = 1
    """

    order_qt_sum_double_preceding_1 """
        SELECT id, v_double,
               sum(v_double) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS got
        FROM test_window_float_sliding_frame
        WHERE grp = 1
    """

    order_qt_avg_sum_float_preceding_1 """
        SELECT id, v_float,
               avg(v_float) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS got_avg,
               sum(v_float) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS got_sum
        FROM test_window_float_sliding_frame
        WHERE grp = 1
    """

    // Sign flip: the large values cancel out only while both are inside the frame.
    order_qt_avg_sum_double_sign_flip """
        SELECT id, v_double,
               avg(v_double) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS got_avg,
               sum(v_double) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS got_sum
        FROM test_window_float_sliding_frame
        WHERE grp = 2
    """

    // Consecutive small values after the large one, wider frames and following bounds.
    order_qt_avg_sum_double_wider_frames """
        SELECT id, v_double,
               avg(v_double) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avg_p2,
               sum(v_double) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sum_p2,
               avg(v_double) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS avg_p1f1,
               sum(v_double) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS sum_p1f1,
               avg(v_double) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS avg_f2
        FROM test_window_float_sliding_frame
        WHERE grp = 3
    """

    // Nullable input: NULL rows are skipped while the frame keeps sliding.
    order_qt_avg_sum_nullable_partitioned """
        SELECT id, grp, v_nullable,
               avg(v_nullable) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS got_avg,
               sum(v_nullable) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS got_sum
        FROM test_window_float_sliding_frame
    """

    // Frames that only grow keep the incremental path and stay unchanged.
    order_qt_avg_sum_double_unbounded """
        SELECT id, v_double,
               avg(v_double) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS got_avg,
               sum(v_double) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS got_sum
        FROM test_window_float_sliding_frame
        WHERE grp = 1
    """

    // Exact accumulators (integer / decimal) still use the incremental path and stay exact.
    order_qt_avg_sum_exact_types """
        SELECT id, v_double,
               avg(CAST(v_double AS BIGINT)) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_bigint,
               sum(CAST(v_double AS BIGINT)) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS sum_bigint,
               avg(CAST(v_double AS DECIMAL(27, 3))) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_decimal,
               sum(CAST(v_double AS DECIMAL(27, 3))) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS sum_decimal
        FROM test_window_float_sliding_frame
        WHERE grp = 1
    """

    // The reported shape without a table.
    order_qt_avg_double_cte """
        WITH t AS (
            SELECT 1 AS id, CAST(18014398509481984 AS DOUBLE) AS v
            UNION ALL SELECT 2, CAST(1 AS DOUBLE)
            UNION ALL SELECT 3, CAST(1 AS DOUBLE)
        )
        SELECT id, v,
               avg(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS got
        FROM t
    """
}
