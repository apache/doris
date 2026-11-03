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

suite("test_array_cum_sum_leading_null") {
    sql "DROP TABLE IF EXISTS test_array_cum_sum_leading_null"
    sql """
        CREATE TABLE test_array_cum_sum_leading_null (
            id INT,
            a ARRAY<INT> NULL
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    // The rows are loaded together and read in one block, so every row after the first one checks
    // that the NULLs at the start of its own array stay NULL.
    sql """
        INSERT INTO test_array_cum_sum_leading_null VALUES
            (1, [1, 2]),
            (2, [NULL, 2]),
            (3, [NULL, NULL, 3]),
            (4, [5, NULL, 1]),
            (5, [NULL, NULL]),
            (6, [NULL, 1, NULL, 2, 3]),
            (7, []),
            (8, NULL)
    """

    // The NULLs before the first non-NULL element of each array stay NULL, and a later NULL keeps
    // the running sum.
    order_qt_column "SELECT id, a, array_cum_sum(a) FROM test_array_cum_sum_leading_null"
    // Each row gives the same result as the same array written as a literal.
    order_qt_literal """
        SELECT array_cum_sum([1, 2]), array_cum_sum([NULL, 2]), array_cum_sum([NULL, NULL, 3]),
               array_cum_sum([5, NULL, 1]), array_cum_sum([NULL, NULL]),
               array_cum_sum([NULL, 1, NULL, 2, 3])
    """
}
