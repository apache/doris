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

suite("test_array_sort_lambda_comparator") {
    // Arithmetic and explicit nullable comparators return nullable integers, even
    // when this comparison produces a non-NULL value.
    order_qt_arithmetic_desc """
        SELECT ARRAY_SORT((x, y) -> y - x, ARRAY(1, 3, 2)) AS sorted_desc
    """
    order_qt_arithmetic_asc """
        SELECT array_sort((x, y) -> x - y, [3, 1, 2, 1])
    """
    order_qt_integer_comparator_types """
        SELECT array_sort((x, y) -> CAST(y - x AS TINYINT), [1, 3, 2]),
               array_sort((x, y) -> CAST(y - x AS SMALLINT), [1, 3, 2]),
               array_sort((x, y) -> CAST(y - x AS INT), [1, 3, 2]),
               array_sort((x, y) -> CAST(y - x AS BIGINT), [1, 3, 2]),
               array_sort((x, y) -> CAST(y - x AS LARGEINT), [1, 3, 2])
    """
    order_qt_largeint_comparator_sign """
        SELECT array_sort((x, y) -> IF(x < y, CAST('-1267650600228229401496703205376' AS LARGEINT),
                                       IF(x = y, CAST(0 AS LARGEINT),
                                          CAST('1267650600228229401496703205376' AS LARGEINT))),
                          [3, 1, 2])
    """
    test {
        sql "SELECT array_sort((x, y) -> 'invalid', [1, 3, 2])"
        exception "array_sort comparator must return an integer"
    }
    order_qt_nullable_comparator """
        SELECT array_sort((x, y) -> IF(x IS NULL OR y IS NULL, NULL, y - x), [1, 3, 2])
    """
    order_qt_const_nullable_comparator """
        SELECT array_sort((x, y) -> nullable(CAST(0 AS TINYINT)), [1, 3, 2])
    """
    order_qt_nullable_comparator_empty_singleton """
        SELECT array_sort((x, y) -> CAST(NULL AS TINYINT), CAST([] AS ARRAY<INT>)),
               array_sort((x, y) -> CAST(NULL AS TINYINT), [1])
    """
    test {
        sql "SELECT array_sort((x, y) -> CAST(NULL AS TINYINT), [1, 3, 2])"
        exception "array_sort comparator returned NULL"
    }
    test {
        sql "SELECT array_sort((x, y) -> y - x, [1, NULL, 2])"
        exception "array_sort comparator returned NULL"
    }

    // A comparator that is not a strict weak ordering must not crash BE. Every pair of values
    // above 100 compares as "less" in both directions, and there are far more than the
    // insertion-sort threshold of such values. Only the cardinality is asserted because the
    // resulting order is unspecified for such a comparator.
    order_qt_inconsistent_comparator_literal """
        SELECT cardinality(array_sort(
            (x, y) -> IF(x > 100 AND y > 100, -1, IF(x < y, -1, IF(x = y, 0, 1))),
            [1,2,3,4,5,6,7,8,9,10,101,102,103,104,105,106,107,108,109,110,
             111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,
             131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,
             151,152,153,154,155,156,157,158,159,160]))
    """

    // A comparator that says "less" for every pair.
    order_qt_always_less_comparator """
        SELECT cardinality(array_sort((x, y) -> -1, array_range(1, 200)))
    """

    // A non-deterministic comparator changes its answer between calls on the same pair.
    order_qt_random_comparator """
        SELECT cardinality(array_sort((x, y) -> IF(random() < 0.5, -1, 1), array_range(1, 200)))
    """

    // Consistent comparators on arrays larger than the insertion-sort threshold still sort.
    order_qt_large_desc """
        SELECT array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), array_range(1, 100))
    """
    // NULLs sort first and compare equal to each other, so the comparator stays a strict weak
    // ordering with several NULL elements.
    order_qt_large_with_null """
        SELECT array_sort((x, y) -> CASE WHEN x IS NULL AND y IS NULL THEN 0
                                         WHEN x IS NULL THEN -1
                                         WHEN y IS NULL THEN 1
                                         WHEN x < y THEN -1
                                         WHEN x = y THEN 0
                                         ELSE 1 END,
                          [20, null, 19, 18, 17, null, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, null])
    """

    // Same inconsistent comparator over a table column (non-constant input path).
    sql "DROP TABLE IF EXISTS test_array_sort_lambda_comparator_tbl"
    sql """
        CREATE TABLE test_array_sort_lambda_comparator_tbl (
            id INT,
            arr ARRAY<INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_array_sort_lambda_comparator_tbl VALUES
            (1, [1,2,3,4,5,6,7,8,9,10,101,102,103,104,105,106,107,108,109,110,
                 111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,
                 131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,
                 151,152,153,154,155,156,157,158,159,160]),
            (2, [3, 1, 2]),
            (3, []),
            (4, NULL)
    """
    order_qt_inconsistent_comparator_table """
        SELECT id, cardinality(array_sort(
            (x, y) -> IF(x > 100 AND y > 100, -1, IF(x < y, -1, IF(x = y, 0, 1))), arr))
        FROM test_array_sort_lambda_comparator_tbl
    """
    order_qt_consistent_comparator_table """
        SELECT id, array_sort((x, y) -> IF(x < y, 1, IF(x = y, 0, -1)), arr)
        FROM test_array_sort_lambda_comparator_tbl
    """
    order_qt_arithmetic_comparator_table """
        SELECT id, array_sort((x, y) -> y - x, arr)
        FROM test_array_sort_lambda_comparator_tbl WHERE id >= 2
    """
    order_qt_nullable_comparator_uncompared_table """
        SELECT id, array_sort((x, y) -> CAST(NULL AS TINYINT), arr)
        FROM test_array_sort_lambda_comparator_tbl WHERE id IN (3, 4)
    """
    test {
        sql """
            SELECT array_sort((x, y) -> CAST(NULL AS TINYINT), arr)
            FROM test_array_sort_lambda_comparator_tbl WHERE id = 2
        """
        exception "array_sort comparator returned NULL"
    }
}
