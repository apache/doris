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

suite("test_array_sort_comparator_type") {
    // The comparator of array_sort returns -1, 0 or 1 for less than, equal to or greater than.
    // A comparator that does not return an integer is rejected when the query is analyzed.
    test {
        sql "SELECT array_sort((x, y) -> x < y, [3, 2, 1])"
        exception "the lambda must return -1, 0 or 1 for less than, equal to or greater than, but it returns BOOLEAN"
    }
    test {
        sql "SELECT array_sort((x, y) -> x - y, [3.5, 2.5, 1.5])"
        exception "the lambda must return -1, 0 or 1"
    }
    test {
        sql "SELECT array_sort((x, y) -> concat(x, y), ['b', 'a'])"
        exception "the lambda must return -1, 0 or 1"
    }

    // Any integer type works, and only the sign of the result is used. The differences of
    // [256, 128, 0] do not fit in TINYINT.
    order_qt_integer_comparator """
        SELECT array_sort((x, y) -> x - y, [3, 1, 2]),
               array_sort((x, y) -> y - x, [3, 1, 2]),
               array_sort((x, y) -> x - y, [256, 128, 0]),
               array_sort((x, y) -> IF(x < y, -1000, 1000), [3, 1, 2]),
               array_sort((x, y) -> cast(x - y AS LARGEINT), [3, 1, 2])
    """

    // A comparator that returns NULL fails the query.
    test {
        sql "SELECT array_sort((x, y) -> x - y, [3, NULL, 1])"
        exception "array_sort comparator returns NULL"
    }

    sql "DROP TABLE IF EXISTS test_array_sort_comparator_type"
    sql """
        CREATE TABLE test_array_sort_comparator_type (
            id INT,
            a ARRAY<INT>,
            b ARRAY<BIGINT>
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_array_sort_comparator_type VALUES
            (1, [3, 1, 2], [9000000000, -9000000000, 0]),
            (2, [256, 128, 0], [1, 2]),
            (3, [], []),
            (4, NULL, NULL)
    """
    // The elements of a table column are nullable, so the comparator result is nullable too. It
    // works as long as the comparator does not return NULL.
    order_qt_integer_comparator_table """
        SELECT id, array_sort((x, y) -> x - y, a), array_sort((x, y) -> y - x, b)
        FROM test_array_sort_comparator_type
    """
}
