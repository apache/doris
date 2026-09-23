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

suite("test_array_shuffle_seed") {
    sql "DROP TABLE IF EXISTS test_array_shuffle_seed"
    sql """
        CREATE TABLE test_array_shuffle_seed (
            k INT,
            a ARRAY<INT> NULL,
            s BIGINT NULL
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    // All rows go into one block, so every row after the first must still use its own seed.
    sql """
        INSERT INTO test_array_shuffle_seed VALUES
            (1, [1, 2, 3, 4, 5], 1),
            (2, [1, 2, 3, 4, 5], 2),
            (3, [1, 2, 3, 4, 5], 1),
            (4, [1, 2, 3, 4, 5], 2),
            (5, [1, 2, 3, 4, 5], NULL),
            (6, NULL, 1),
            (7, [], 1),
            (8, [1, 2, 3, 4, 5, 6, 7, 8], 1)
    """

    // Rows with the same seed and array give the same result.
    order_qt_column_seed "SELECT k, s, array_shuffle(a, s), shuffle(a, s) FROM test_array_shuffle_seed"
    // A row gives the same result as the constant call with the same seed.
    order_qt_const_call "SELECT array_shuffle([1, 2, 3, 4, 5], 1), array_shuffle([1, 2, 3, 4, 5], 2), array_shuffle([1, 2, 3, 4, 5, 6, 7, 8], 1)"
    order_qt_match_const """
        SELECT k, array_shuffle(a, s) = array_shuffle([1, 2, 3, 4, 5], s)
        FROM test_array_shuffle_seed WHERE k <= 4
    """
    // A constant seed gives the same result on every row.
    order_qt_const_seed "SELECT k, array_shuffle(a, 1) FROM test_array_shuffle_seed"
}
