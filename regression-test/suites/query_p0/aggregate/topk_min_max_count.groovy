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

suite("topk_min_max_count") {
    sql """drop table if exists topk_min_max_count_t"""

    sql """
        CREATE TABLE topk_min_max_count_t (
            id INT NOT NULL,
            grp VARCHAR(8) NOT NULL,
            i INT,
            ni INT NOT NULL,
            s VARCHAR(16),
            d DECIMAL(10, 2),
            dt DATETIME,
            f DOUBLE
        )
        ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    // grp a: i -> 1 x2, 2 x1, 3 x2, NULL x1; ni has an extra 9
    // grp b: single non-NULL value
    // grp c: only NULL i
    // grp d: 10..17, i.e. 8 distinct values
    // grp e: FLOAT special values, NaN must be distinct from every number and sort greatest
    sql """INSERT INTO topk_min_max_count_t VALUES
        (1,  'a', 1,    1,  'b',  1.10, '2024-01-01 10:00:00', 1.5),
        (2,  'a', 1,    1,  'a',  1.10, '2024-01-01 09:00:00', 2.5),
        (3,  'a', 2,    2,  'c',  2.20, '2024-01-02 10:00:00', 3.5),
        (4,  'a', 3,    3,  'c',  3.30, '2024-01-03 10:00:00', NULL),
        (5,  'a', 3,    3,  'c',  3.30, '2024-01-03 10:00:00', NULL),
        (6,  'a', NULL, 9,  'z',  NULL, NULL, NULL),
        (7,  'b', 5,    5,  'x',  5.50, '2024-02-01 10:00:00', 4.5),
        (8,  'c', NULL, 0,  NULL, NULL, NULL, NULL),
        (9,  'c', NULL, 0,  NULL, NULL, NULL, NULL),
        (10, 'd', 10,   10, 'j', 10.10, '2024-03-01 10:00:00', 10.5),
        (11, 'd', 11,   11, 'k', 11.10, '2024-03-02 10:00:00', 11.5),
        (12, 'd', 12,   12, 'l', 12.10, '2024-03-03 10:00:00', 12.5),
        (13, 'd', 13,   13, 'm', 13.10, '2024-03-04 10:00:00', 13.5),
        (14, 'd', 14,   14, 'n', 14.10, '2024-03-05 10:00:00', 14.5),
        (15, 'd', 15,   15, 'o', 15.10, '2024-03-06 10:00:00', 15.5),
        (16, 'd', 16,   16, 'p', 16.10, '2024-03-07 10:00:00', 16.5),
        (17, 'd', 17,   17, 'q', 17.10, '2024-03-08 10:00:00', 17.5),
        (18, 'e', 0,    0,  's',  0.00, NULL, 0.0),
        (19, 'e', 0,    0,  's',  0.00, NULL, 0.0),
        (20, 'e', 0,    0,  's',  0.00, NULL, CAST('NaN' AS DOUBLE)),
        (21, 'e', 0,    0,  's',  0.00, NULL, 1.0),
        (22, 'e', 0,    0,  's',  0.00, NULL, CAST('NaN' AS DOUBLE))"""

    // Bounds are ordinary per-row columns: the scalar must read each row's bound at that row's
    // index, not at index 0 of the block.
    sql """drop table if exists topk_merge_rows_t"""

    sql """
        CREATE TABLE topk_merge_rows_t (
            id INT NOT NULL,
            s ARRAY<STRUCT<value:INT, count:BIGINT>>,
            b INT,
            n BIGINT
        )
        ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """INSERT INTO topk_merge_rows_t VALUES
        (1, CAST(array(named_struct('value', CAST(1 AS INT), 'count', CAST(1 AS BIGINT)),
                       named_struct('value', CAST(2 AS INT), 'count', CAST(1 AS BIGINT)))
                 AS ARRAY<STRUCT<value:INT, count:BIGINT>>), CAST(2 AS INT), CAST(2 AS BIGINT)),
        (2, CAST(array(named_struct('value', CAST(10 AS INT), 'count', CAST(1 AS BIGINT)),
                       named_struct('value', CAST(20 AS INT), 'count', CAST(1 AS BIGINT)))
                 AS ARRAY<STRUCT<value:INT, count:BIGINT>>), CAST(20 AS INT), CAST(2 AS BIGINT))"""

    // k = 0: no list, bound is the extreme value of this side (NULL when no non-NULL input).
    order_qt_min_k0 """
        SELECT grp, topk_min_count(i, 0) FROM topk_min_max_count_t GROUP BY grp
    """

    order_qt_max_k0 """
        SELECT grp, topk_max_count(i, 0) FROM topk_min_max_count_t GROUP BY grp
    """

    // k = 1 is "extreme + count".
    order_qt_min_k1 """
        SELECT grp, topk_min_count(i, 1) FROM topk_min_max_count_t GROUP BY grp
    """

    order_qt_max_k1 """
        SELECT grp, topk_max_count(i, 1) FROM topk_min_max_count_t GROUP BY grp
    """

    // k = 2: grp a/b are truncated, grp c has no values, grp d keeps 10 and 11.
    order_qt_min_k2 """
        SELECT grp, topk_min_count(i, 2) FROM topk_min_max_count_t GROUP BY grp
    """

    order_qt_max_k2 """
        SELECT grp, topk_max_count(i, 2) FROM topk_min_max_count_t GROUP BY grp
    """

    // k larger than the distinct count: bound stays NULL (not truncated).
    order_qt_min_k8 """
        SELECT grp, topk_min_count(i, 8) FROM topk_min_max_count_t GROUP BY grp
    """

    order_qt_max_k8 """
        SELECT grp, topk_max_count(i, 8) FROM topk_min_max_count_t GROUP BY grp
    """

    // Empty input and all-NULL groups return (empty list, NULL bound), never NULL.
    order_qt_min_empty_input """
        SELECT topk_min_count(i, 2) FROM topk_min_max_count_t WHERE id < 0
    """

    order_qt_min_all_null_group """
        SELECT grp, topk_min_count(i, 2) FROM topk_min_max_count_t WHERE grp = 'c' GROUP BY grp
    """

    // A NOT NULL input type still has a nullable bound.
    order_qt_min_not_null_type """
        SELECT grp, topk_min_count(ni, 2) FROM topk_min_max_count_t GROUP BY grp
    """

    order_qt_min_not_null_type_empty """
        SELECT topk_min_count(ni, 2) FROM topk_min_max_count_t WHERE id < 0
    """

    // The value list and bound follow MIN/MAX for strings, decimals and datetimes.
    order_qt_min_string """
        SELECT grp, topk_min_count(s, 2), topk_max_count(s, 2)
        FROM topk_min_max_count_t GROUP BY grp
    """

    order_qt_min_decimal """
        SELECT grp, topk_min_count(d, 2), topk_max_count(d, 2)
        FROM topk_min_max_count_t GROUP BY grp
    """

    order_qt_min_datetime """
        SELECT grp, topk_min_count(dt, 2), topk_max_count(dt, 2)
        FROM topk_min_max_count_t GROUP BY grp
    """

    // NaN is a distinct value greater than every number, -0.0 and 0.0 are the same value.
    order_qt_double_special_values """
        SELECT grp, topk_min_count(f, 8), topk_max_count(f, 8)
        FROM topk_min_max_count_t WHERE grp = 'e' GROUP BY grp
    """

    // min/max of the state list agrees with MIN/MAX for groups that fit in k.
    order_qt_agree_with_min_max """
        SELECT grp, MIN(i), MAX(i), topk_min_count(i, 100), topk_max_count(i, 100)
        FROM topk_min_max_count_t GROUP BY grp
    """

    // Two-phase aggregation (partial states are serialized, shipped and merged by the final agg)
    // must agree with the single-pass result for both MIN and MAX.
    sql "set parallel_pipeline_task_num = 8"
    order_qt_two_phase_merge """
        SELECT topk_min_count(i, 2), topk_max_count(i, 2),
               topk_min_count(ni, 8), topk_max_count(ni, 8)
        FROM topk_min_max_count_t
    """
    sql "set parallel_pipeline_task_num = 0"

    // A large k is accepted; MAX_TOPK_K + 1 is rejected.
    order_qt_max_k_accepted """
        SELECT topk_min_count(i, 65536), topk_max_count(i, 65536)
        FROM topk_min_max_count_t WHERE id = 1
    """

    test {
        sql "SELECT topk_min_count(i, 65537) FROM topk_min_max_count_t"
        exception "topk_min_count requires 0 <= count <= 65536"
    }

    test {
        sql "SELECT topk_max_count(i, -1) FROM topk_min_max_count_t"
        exception "topk_max_count requires 0 <= count <= 65536"
    }

    test {
        sql "SELECT topk_min_count(i, id) FROM topk_min_max_count_t"
        exception "topk_min_count requires second parameter must be a constant"
    }

    test {
        sql "SELECT topk_min_count(i, NULL) FROM topk_min_max_count_t"
        exception "topk_min_count requires a non-null constant count"
    }

    test {
        sql "SELECT topk_min_count(DISTINCT i, 2) FROM topk_min_max_count_t"
        exception "topk_min_count does not support DISTINCT"
    }

    test {
        sql "SELECT topk_max_count(DISTINCT i, 2) FROM topk_min_max_count_t"
        exception "topk_max_count does not support DISTINCT"
    }

    // ---- topk_merge_min / topk_merge_max ----

    // Complete state, no truncation anywhere: S + I - D, net-zero and negative values dropped.
    order_qt_merge_min_complete """
        SELECT topk_merge_min(
            CAST(array(named_struct('value', 1, 'count', 2), named_struct('value', 3, 'count', 1))
                 AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(array(named_struct('value', 2, 'count', 1), named_struct('value', 4, 'count', 1))
                 AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(array(named_struct('value', 3, 'count', 1))
                 AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(3 AS BIGINT), CAST(1 AS BIGINT), CAST(8 AS BIGINT))
    """

    order_qt_merge_max_complete """
        SELECT topk_merge_max(
            CAST(array(named_struct('value', 5, 'count', 2), named_struct('value', 3, 'count', 1))
                 AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(array(named_struct('value', 4, 'count', 1), named_struct('value', 2, 'count', 1))
                 AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(array(named_struct('value', 5, 'count', 1))
                 AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(3 AS BIGINT), CAST(1 AS BIGINT), CAST(8 AS BIGINT))
    """

    // The pre-image is truncated at b = 2: values above 2 are not trusted and the new state
    // keeps bound = 2.
    order_qt_merge_min_pre_image_bound """
        SELECT topk_merge_min(
            CAST(array(named_struct('value', 1, 'count', 1), named_struct('value', 2, 'count', 1))
                 AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(2 AS INT),
            CAST(array(named_struct('value', 3, 'count', 1))
                 AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(array() AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(2 AS BIGINT), CAST(1 AS BIGINT), CAST(8 AS BIGINT))
    """

    // The insert side is truncated at 2, so only values <= 2 are trusted.
    order_qt_merge_min_insert_bound """
        SELECT topk_merge_min(
            CAST(array(named_struct('value', 1, 'count', 1)) AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(2 AS INT),
            CAST(array(named_struct('value', 2, 'count', 1), named_struct('value', 7, 'count', 1))
                 AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(2 AS INT),
            CAST(array() AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(1 AS BIGINT), CAST(2 AS BIGINT), CAST(8 AS BIGINT))
    """

    // The delete side is truncated at 2: values above 2 are no longer trusted, so the pre-image
    // entry 5 is dropped and the state keeps bound = 2.
    order_qt_merge_min_delete_bound """
        SELECT topk_merge_min(
            CAST(array(named_struct('value', 1, 'count', 2), named_struct('value', 5, 'count', 1))
                 AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(array() AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(array(named_struct('value', 1, 'count', 1))
                 AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(2 AS INT),
            CAST(3 AS BIGINT), CAST(-1 AS BIGINT), CAST(8 AS BIGINT))
    """

    // n' > 0 but the trusted range is used up: exhausted must be true.
    order_qt_merge_min_exhausted """
        SELECT topk_merge_min(
            CAST(array(named_struct('value', 2, 'count', 1)) AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(2 AS INT),
            CAST(array() AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(array(named_struct('value', 2, 'count', 1))
                 AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(1 AS BIGINT), CAST(0 AS BIGINT), CAST(8 AS BIGINT))
    """

    // n' == 0: the list is empty and the bound becomes NULL again.
    order_qt_merge_min_empty_after_delete """
        SELECT topk_merge_min(
            CAST(array(named_struct('value', 2, 'count', 1)) AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(2 AS INT),
            CAST(array() AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(array(named_struct('value', 2, 'count', 1))
                 AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(1 AS BIGINT), CAST(-1 AS BIGINT), CAST(8 AS BIGINT))
    """

    // More than k surviving values: only the first k are kept, bound is the k-th one.
    order_qt_merge_min_truncate_to_k """
        SELECT topk_merge_min(
            CAST(array(named_struct('value', 1, 'count', 1), named_struct('value', 2, 'count', 1),
                       named_struct('value', 3, 'count', 1), named_struct('value', 4, 'count', 1))
                 AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(array() AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(array() AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(4 AS BIGINT), CAST(0 AS BIGINT), CAST(2 AS BIGINT))
    """

    // pos == k is not truncation: every known value fits, so the state keeps bE (5) instead of
    // shrinking the trusted range to max(S') (2).
    order_qt_merge_min_pos_equals_k """
        SELECT topk_merge_min(
            CAST(array(named_struct('value', 1, 'count', 1))
                 AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(array(named_struct('value', 2, 'count', 1))
                 AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(5 AS INT),
            CAST(array() AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(1 AS BIGINT), CAST(1 AS BIGINT), CAST(2 AS BIGINT))
    """

    // MAX is symmetric.
    order_qt_merge_max_truncate_to_k """
        SELECT topk_merge_max(
            CAST(array(named_struct('value', 4, 'count', 1), named_struct('value', 3, 'count', 1),
                       named_struct('value', 2, 'count', 1), named_struct('value', 1, 'count', 1))
                 AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(array() AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(array() AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(4 AS BIGINT), CAST(0 AS BIGINT), CAST(2 AS BIGINT))
    """

    // Each row keeps its own bound: row 1 is truncated at 2, row 2 at 20.
    order_qt_merge_multi_row_bounds """
        SELECT id, topk_merge_min(
            s,
            b,
            CAST(array() AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(array() AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            n, CAST(0 AS BIGINT), CAST(2 AS BIGINT)) AS merged
        FROM topk_merge_rows_t
    """

    // -0.0 and 0.0 are the same value: they merge into a single entry with the summed count.
    // Written as an explicit merge so the retained representative (-0.0, the left side) and the
    // result do not depend on the scan order of the aggregate test above.
    order_qt_merge_double_signed_zero """
        SELECT topk_merge_min(
            CAST(array(named_struct('value', CAST('-0' AS DOUBLE), 'count', CAST(1 AS BIGINT)))
                 AS ARRAY<STRUCT<value:DOUBLE, count:BIGINT>>),
            CAST(NULL AS DOUBLE),
            CAST(array(named_struct('value', CAST(0 AS DOUBLE), 'count', CAST(1 AS BIGINT)))
                 AS ARRAY<STRUCT<value:DOUBLE, count:BIGINT>>),
            CAST(NULL AS DOUBLE),
            CAST(array() AS ARRAY<STRUCT<value:DOUBLE, count:BIGINT>>),
            CAST(NULL AS DOUBLE),
            CAST(2 AS BIGINT), CAST(0 AS BIGINT), CAST(8 AS BIGINT))
    """

    // Every argument NULL: NULL lists are empty, NULL bounds are complete, the result struct is
    // NOT NULL and only its bound field is NULL.
    order_qt_merge_all_null """
        SELECT topk_merge_min(
            CAST(NULL AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(NULL AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(NULL AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(NULL AS INT),
            CAST(0 AS BIGINT), CAST(0 AS BIGINT), CAST(8 AS BIGINT))
    """

    test {
        sql """SELECT topk_merge_min(
                   CAST(array() AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
                   CAST(NULL AS INT),
                   CAST(array() AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
                   CAST(NULL AS INT),
                   CAST(array() AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
                   CAST(NULL AS INT),
                   0, 0, 0)"""
        exception "topk_merge_min requires 1 <= k <= 65536"
    }

    test {
        sql """SELECT topk_merge_max(
                   CAST(array() AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
                   CAST(NULL AS INT),
                   CAST(array() AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
                   CAST(NULL AS INT),
                   CAST(array() AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
                   CAST(NULL AS INT),
                   0, 0, 65537)"""
        exception "topk_merge_max requires 1 <= k <= 65536"
    }

    // Untyped NULL bounds must not narrow the nested decimal type: 1.23 has to survive.
    order_qt_merge_decimal_null_bounds """
        SELECT topk_merge_min(
            CAST(array(named_struct('value', CAST(1.23 AS DECIMAL(4, 2)), 'count', CAST(1 AS BIGINT)))
                 AS ARRAY<STRUCT<value:DECIMAL(4,2), count:BIGINT>>),
            CAST(NULL AS DECIMAL(4, 2)),
            CAST(array() AS ARRAY<STRUCT<value:DECIMAL(4,2), count:BIGINT>>),
            CAST(NULL AS DECIMAL(4, 2)),
            CAST(array() AS ARRAY<STRUCT<value:DECIMAL(4,2), count:BIGINT>>),
            CAST(NULL AS DECIMAL(4, 2)),
            CAST(1 AS BIGINT), CAST(0 AS BIGINT), CAST(2 AS BIGINT))
    """

    test {
        sql """SELECT topk_merge_min(
                   CAST(array() AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
                   CAST(NULL AS INT),
                   CAST(array(named_struct('value', CAST(1.1 AS DOUBLE), 'count', CAST(1 AS BIGINT)))
                        AS ARRAY<STRUCT<value:DOUBLE, count:BIGINT>>),
                   CAST(NULL AS DOUBLE),
                   CAST(array() AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
                   CAST(NULL AS INT),
                   0, 2, 2)"""
        exception "requires every list and bound to have the same value type"
    }

    test {
        sql """SELECT topk_merge_min(
                   CAST(array(named_struct('value', CAST(NULL AS INT), 'count', CAST(1 AS BIGINT)))
                        AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
                   CAST(NULL AS INT),
                   CAST(array() AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
                   CAST(NULL AS INT),
                   CAST(array() AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
                   CAST(NULL AS INT),
                   1, 0, 1)"""
        exception "must not contain NULL entries, values or counts"
    }

    // An evaluated-but-NULL count fails the query even though it is not a literal FE can fold.
    test {
        sql """SELECT grp, topk_min_count(v, bitmap_min(bitmap_from_string('')))
               FROM (SELECT CAST(1 AS INT) AS grp, CAST(1 AS INT) AS v
                     UNION ALL SELECT CAST(1 AS INT), CAST(2 AS INT)) t
               GROUP BY grp"""
        exception "must be a non-null constant"
    }
}
