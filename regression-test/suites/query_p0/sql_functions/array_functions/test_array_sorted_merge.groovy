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

suite("test_array_sorted_merge") {
    // Both inputs are already sorted; the merge lambda returns the merged element for equal
    // keys and NULL when the two elements must be kept separately.
    order_qt_dedup_asc """
        SELECT array_sorted_merge((a, b) -> IF(a = b, a, NULL), [1, 3, 5], [2, 3, 6], true)
    """

    // The same merge over two descending inputs.
    order_qt_dedup_desc """
        SELECT array_sorted_merge((a, b) -> IF(a = b, a, NULL), [5, 3, 1], [6, 3, 2], false)
    """

    // A struct element keeps its key (`value`) and combines the payload (`count`), which is the
    // shape the incremental MIN/MAX top-K state uses.
    order_qt_struct_asc """
        SELECT array_sorted_merge(
            (a, b) -> IF(element_at(a, 'value') = element_at(b, 'value'),
                         CAST(named_struct('value', element_at(a, 'value'),
                                           'count', element_at(a, 'count') + element_at(b, 'count'))
                              AS STRUCT<value:INT, count:BIGINT>),
                         CAST(NULL AS STRUCT<value:INT, count:BIGINT>)),
            CAST(array(named_struct('value', CAST(1 AS INT), 'count', CAST(2 AS BIGINT)),
                       named_struct('value', CAST(3 AS INT), 'count', CAST(1 AS BIGINT)))
                 AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(array(named_struct('value', CAST(2 AS INT), 'count', CAST(1 AS BIGINT)),
                       named_struct('value', CAST(3 AS INT), 'count', CAST(1 AS BIGINT)))
                 AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            true)
    """

    order_qt_struct_desc """
        SELECT array_sorted_merge(
            (a, b) -> IF(element_at(a, 'value') = element_at(b, 'value'),
                         CAST(named_struct('value', element_at(a, 'value'),
                                           'count', element_at(a, 'count') + element_at(b, 'count'))
                              AS STRUCT<value:INT, count:BIGINT>),
                         CAST(NULL AS STRUCT<value:INT, count:BIGINT>)),
            CAST(array(named_struct('value', CAST(5 AS INT), 'count', CAST(1 AS BIGINT)),
                       named_struct('value', CAST(3 AS INT), 'count', CAST(1 AS BIGINT)))
                 AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            CAST(array(named_struct('value', CAST(4 AS INT), 'count', CAST(1 AS BIGINT)),
                       named_struct('value', CAST(3 AS INT), 'count', CAST(1 AS BIGINT)))
                 AS ARRAY<STRUCT<value:INT, count:BIGINT>>),
            false)
    """

    // Empty and NULL inputs: an empty array contributes nothing, a NULL array makes the result
    // NULL like the other higher-order array functions.
    order_qt_empty_and_null """
        SELECT array_sorted_merge((a, b) -> IF(a = b, a, NULL),
                                  CAST([] AS ARRAY<INT>), CAST([1] AS ARRAY<INT>), true),
               array_sorted_merge((a, b) -> IF(a = b, a, NULL),
                                  CAST(NULL AS ARRAY<INT>), CAST([1] AS ARRAY<INT>), true),
               array_sorted_merge((a, b) -> IF(a = b, a, NULL),
                                  CAST([] AS ARRAY<INT>), CAST([] AS ARRAY<INT>), true)
    """

    // NULLs order like array_sort/array_reverse_sort: least in an ascending merge, greatest in a
    // descending one, so the merged list keeps the inputs' own ordering.
    order_qt_null_ordering """
        SELECT array_sorted_merge((a, b) -> IF(a = b, a, NULL),
                                  array_sort(CAST([NULL, 1] AS ARRAY<INT>)),
                                  CAST([2] AS ARRAY<INT>), true),
               array_sorted_merge((a, b) -> IF(a = b, a, NULL),
                                  array_reverse_sort(CAST([NULL, 1] AS ARRAY<INT>)),
                                  CAST([2] AS ARRAY<INT>), false)
    """

    // Under short-circuit evaluation the branch only sees the selected rows, so a captured column
    // must be read at its original row rather than at the compacted index. Row id=1 has lim=2,
    // so the merge lambda keeps a single value; reading row 0's lim=0 would keep both.
    sql """SET short_circuit_evaluation = true"""
    sql """DROP TABLE IF EXISTS array_sorted_merge_selector"""
    sql """
        CREATE TABLE array_sorted_merge_selector (
            id INT,
            lim INT,
            l ARRAY<INT>,
            r ARRAY<INT>
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO array_sorted_merge_selector VALUES (0, 0, [1], [1]), (1, 2, [1], [1])"""
    order_qt_short_circuit_selector """
        SELECT id, IF(id = 1,
                      array_sorted_merge((a, b) -> IF(a = b AND a < lim, a, NULL), l, r, true),
                      CAST([] AS ARRAY<INT>))
        FROM array_sorted_merge_selector ORDER BY id
    """
    sql """SET short_circuit_evaluation = false"""

    // A captured input column must be read at the current row: with the capture at column
    // index 0 the predicate compares against a candidate unless it is rebound, and with the
    // capture at index 2 an unbound read reports a missing column.
    sql """DROP TABLE IF EXISTS array_sorted_merge_capture_0"""
    sql """
        CREATE TABLE array_sorted_merge_capture_0 (
            lim INT,
            l ARRAY<INT>,
            r ARRAY<INT>
        )
        DISTRIBUTED BY HASH(lim) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO array_sorted_merge_capture_0 VALUES (2, [1], [1])"""
    order_qt_capture_column_zero """
        SELECT array_sorted_merge((a, b) -> IF(a = b AND a < lim, a, NULL), l, r, true)
        FROM array_sorted_merge_capture_0
    """

    sql """DROP TABLE IF EXISTS array_sorted_merge_capture_2"""
    sql """
        CREATE TABLE array_sorted_merge_capture_2 (
            id INT,
            l ARRAY<INT>,
            r ARRAY<INT>,
            lim INT
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO array_sorted_merge_capture_2 VALUES (0, [1], [1], 2)"""
    order_qt_capture_column_two """
        SELECT array_sorted_merge((a, b) -> IF(a = b AND a < lim, a, NULL), l, r, true)
        FROM array_sorted_merge_capture_2
    """

    // The merge lambda may also reference an enclosing lambda's argument. array_map binds x by
    // name, while this function executes the merge lambda against a block of its own, so the
    // outer binding is published under its name there. x=0 keeps both values, x=2 merges them.
    order_qt_capture_enclosing_lambda """
        SELECT array_map(x -> array_sorted_merge((a, b) -> IF(a = b AND a < x, a, NULL),
                                                 CAST([1] AS ARRAY<INT>),
                                                 CAST([1] AS ARRAY<INT>), true),
                         [0, 2])
    """

    // The same outer argument referenced from a lambda nested inside the merge body. The merge
    // frame rebases the outer binding onto its private block, so the inner array_map resolves x
    // there instead of walking into the parent frames, whose positions do not describe that block.
    order_qt_capture_enclosing_lambda_nested """
        SELECT array_map(x -> array_sorted_merge(
                   (a, b) -> IF(a = b
                               AND a < element_at(array_map(y -> x, CAST([1] AS ARRAY<INT>)), 1),
                               a, NULL),
                   CAST([1] AS ARRAY<INT>), CAST([1] AS ARRAY<INT>), true),
               [0, 2])
    """

    // A non-nullable lambda body (COALESCE) must be wrapped into the nullable element
    // representation instead of being inserted directly.
    order_qt_nonnullable_body """
        SELECT array_sorted_merge((a, b) -> COALESCE(a, b, CAST(0 AS INT)),
                                  CAST([1, 2] AS ARRAY<INT>),
                                  CAST([1, 2] AS ARRAY<INT>), true)
    """

    // The most direct composition: the aggregate output is fed back into the merge lambda that
    // rebuilds the pair with named_struct. The pair fields are nullable in the type, which is what
    // element_at produces, so the element-type check accepts the lambda without any cast.
    order_qt_topk_output_merge """
        SELECT array_sorted_merge(
                   (a, b) -> IF(element_at(a, 'value') = element_at(b, 'value'),
                                named_struct('value', element_at(a, 'value'),
                                             'count', element_at(a, 'count')
                                                      + element_at(b, 'count')),
                                NULL),
                   s, s, true)
        FROM (SELECT min_topk(i, 2) AS s FROM (SELECT 1 AS i) t) x
    """

    test {
        sql """SELECT array_sorted_merge(
                   (a, b) -> CAST(a + element_at(array_map(x -> x + a, [1]), 1) AS INT),
                   CAST([1] AS ARRAY<INT>), CAST([2] AS ARRAY<INT>), true)"""
        exception "cannot be captured by a nested lambda"
    }

    test {
        sql """SELECT array_sorted_merge((a, b) -> IF(a = b, a, NULL), [1], [2], 'asc')"""
        exception "ascending flag"
    }

    test {
        sql """SELECT array_sorted_merge((a, b) -> IF(a = b, a, NULL), [1], [2],
                   CAST(NULL AS BOOLEAN))"""
        exception "non-null constant ascending flag"
    }

    // A flag the FE cannot fold is validated per row by BE. A NULL input array must not short
    // circuit that check: the flag is an argument contract, not data.
    test {
        sql """SELECT array_sorted_merge((a, b) -> IF(a = b, a, NULL),
                   CAST(NULL AS ARRAY<INT>), CAST([1] AS ARRAY<INT>),
                   CAST(bitmap_min(bitmap_from_string('')) AS BOOLEAN))"""
        exception "non-null ascending flag"
    }

    test {
        sql """SELECT array_sorted_merge(a -> a, [1], [2], true)"""
        exception "requires two arrays and an ascending flag"
    }

    test {
        sql """SELECT array_sorted_merge((a, b) -> IF(a = b, a, NULL), [1], [1.5], true)"""
        exception "same element type"
    }
}
