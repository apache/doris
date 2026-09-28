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
    sql """
        INSERT INTO test_array_shuffle_seed VALUES
            (1, [1, 2, 3, 4, 5], 1),
            (2, [1, 2, 3, 4, 5], 2),
            (3, NULL, 1),
            (4, [], 1),
            (5, [42], 1)
    """

    // The rows of a block draw from one random sequence that starts from the seed, so a per-row
    // seed would be ignored. A non-constant seed is rejected instead.
    test {
        sql "SELECT k, array_shuffle(a, s) FROM test_array_shuffle_seed"
        exception "must be a constant"
    }
    test {
        sql "SELECT k, shuffle(a, k) FROM test_array_shuffle_seed"
        exception "must be a constant"
    }
    test {
        sql """
            WITH t AS (SELECT [1, 2, 3, 4, 5] a, 1 seed UNION ALL SELECT [1, 2, 3, 4, 5], 2)
            SELECT seed, array_shuffle(a, seed) FROM t ORDER BY seed
        """
        exception "must be a constant"
    }
    test {
        sql "SELECT array_shuffle([1, 2, 3, 4, 5], cast(random() * 10 as bigint))"
        exception "must be a constant"
    }

    // A constant seed gives a fixed result. A constant expression works as a seed too.
    order_qt_const_seed """
        SELECT array_shuffle([1, 2, 3, 4, 5], 1), array_shuffle([1, 2, 3, 4, 5], 2),
               shuffle([1, 2, 3, 4, 5], 1), array_shuffle([1, 2, 3, 4, 5], 1 + 1)
    """
    // Any BIGINT is a valid seed, a negative one too. All 64 bits are used, so -1 and 4294967295
    // (same low 32 bits) give different results, and so do -9223372036854775808 and 0.
    order_qt_bigint_seed """
        SELECT array_shuffle(array_range(10), -1), array_shuffle(array_range(10), 4294967295),
               array_shuffle(array_range(10), -9223372036854775808),
               array_shuffle(array_range(10), 0),
               array_shuffle(array_range(10), 9223372036854775807)
    """
    // A NULL seed gives NULL.
    order_qt_null_seed "SELECT array_shuffle([1, 2, 3, 4, 5], NULL)"
    // Shuffling keeps the elements, so sorting them back gives a stable result.
    order_qt_table """
        SELECT k, array_sort(array_shuffle(a, 1)), array_size(shuffle(a, -1))
        FROM test_array_shuffle_seed
    """

    // A constant array is still shuffled on each row, so the rows do not all get the same order.
    order_qt_const_array """
        SELECT count(DISTINCT cast(array_shuffle(array_range(20), 1) AS string)),
               count(DISTINCT cast(array_shuffle(array_range(20)) AS string))
        FROM numbers("number" = "100")
    """
    // array_shuffle is not folded into one constant, even when BE folds the constants.
    order_qt_const_array_fold """
        SELECT /*+ SET_VAR(enable_fold_constant_by_be = true) */
               count(DISTINCT cast(array_shuffle(array_range(20)) AS string))
        FROM numbers("number" = "100")
    """
    // Without a seed, each block gets its own random seed, so the blocks do not repeat the
    // same orders.
    order_qt_no_seed_blocks """
        SELECT count(DISTINCT cast(array_shuffle(array_range(20 + number * 0)) AS string))
        FROM numbers("number" = "100000")
    """
}
