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
    order_qt_large_with_null """
        SELECT array_sort((x, y) -> CASE WHEN x IS NULL THEN -1
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
}
