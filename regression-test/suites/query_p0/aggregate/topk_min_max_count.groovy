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

    // k = 0: the array holds the extreme value of this side and its row count; it is empty when
    // no non-NULL input was seen.
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

    // k larger than the distinct count: the array holds every distinct value.
    order_qt_min_k8 """
        SELECT grp, topk_min_count(i, 8) FROM topk_min_max_count_t GROUP BY grp
    """

    order_qt_max_k8 """
        SELECT grp, topk_max_count(i, 8) FROM topk_min_max_count_t GROUP BY grp
    """

    // Empty input and all-NULL groups return an empty array, never NULL.
    order_qt_min_empty_input """
        SELECT topk_min_count(i, 2) FROM topk_min_max_count_t WHERE id < 0
    """

    order_qt_min_all_null_group """
        SELECT grp, topk_min_count(i, 2) FROM topk_min_max_count_t WHERE grp = 'c' GROUP BY grp
    """

    // A NOT NULL input type behaves the same.
    order_qt_min_not_null_type """
        SELECT grp, topk_min_count(ni, 2) FROM topk_min_max_count_t GROUP BY grp
    """

    order_qt_min_not_null_type_empty """
        SELECT topk_min_count(ni, 2) FROM topk_min_max_count_t WHERE id < 0
    """

    // The array follows MIN/MAX for strings, decimals and datetimes.
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

    // The array can be inspected with the ordinary array functions, so its shape is
    // ARRAY<STRUCT<value,count>> rather than a dedicated type.
    order_qt_result_shape """
        SELECT grp,
               cardinality(topk_min_count(i, 2)),
               element_at(topk_min_count(i, 2), 1).value,
               element_at(topk_min_count(i, 2), 1).count
        FROM topk_min_max_count_t GROUP BY grp
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
}
