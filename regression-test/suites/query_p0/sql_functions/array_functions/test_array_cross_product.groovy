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

suite("test_array_cross_product") {
    qt_array_cross_product_1 """
        SELECT array_cross_product([2.5, -3.0, 4.25], [-7.5, 0.5, 1.25])
    """

    qt_array_cross_product_2 """
        SELECT cross_product([-0.125, 8.5, -16.0], [32.0, -0.5, 0.25])
    """

    qt_array_cross_product_4 """
        SELECT array_cross_product(CAST(NULL AS ARRAY<FLOAT>), [4.0, 5.0, 6.0])
    """

    qt_array_cross_product_5 """
        SELECT array_cross_product(CAST([-128, 0, 127] AS ARRAY<TINYINT>),
                                   CAST([3, -5, 7] AS ARRAY<TINYINT>))
    """

    qt_array_cross_product_6 """
        SELECT array_cross_product(CAST([-32768, 123, -456] AS ARRAY<SMALLINT>),
                                   CAST([789, -1011, 1213] AS ARRAY<SMALLINT>))
    """

    qt_array_cross_product_7 """
        SELECT array_cross_product(CAST([123456, -7890, 42] AS ARRAY<INT>),
                                   CAST([-314, 15926, -5358] AS ARRAY<INT>))
    """

    qt_array_cross_product_8 """
        SELECT array_cross_product(CAST([-9000000, 12345, 67890] AS ARRAY<BIGINT>),
                                   CAST([24680, -13579, 112233] AS ARRAY<BIGINT>))
    """

    qt_array_cross_product_9 """
        SELECT array_cross_product(CAST([1000000001, -2000000002, 3000000003] AS ARRAY<LARGEINT>),
                                   CAST([-4000000004, 5000000005, -6000000006] AS ARRAY<LARGEINT>))
    """

    testFoldConst("SELECT array_cross_product([2.5, -3.0, 4.25], [-7.5, 0.5, 1.25])")
    testFoldConst("SELECT cross_product([-11, 0, 13], [17, -19, 23])")

    sql "DROP TABLE IF EXISTS test_array_cross_product_table"
    sql """
        CREATE TABLE test_array_cross_product_table (
            id INT,
            lhs ARRAY<FLOAT>,
            rhs ARRAY<FLOAT>,
            tiny_lhs ARRAY<TINYINT>,
            tiny_rhs ARRAY<TINYINT>,
            small_lhs ARRAY<SMALLINT>,
            small_rhs ARRAY<SMALLINT>,
            int_lhs ARRAY<INT>,
            int_rhs ARRAY<INT>,
            big_lhs ARRAY<BIGINT>,
            big_rhs ARRAY<BIGINT>,
            large_lhs ARRAY<LARGEINT>,
            large_rhs ARRAY<LARGEINT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """
        INSERT INTO test_array_cross_product_table VALUES
        (1, [2.5, -3.0, 4.25], [-7.5, 0.5, 1.25],
            [-128, 0, 127], [3, -5, 7],
            [-32768, 123, -456], [789, -1011, 1213],
            [123456, -7890, 42], [-314, 15926, -5358],
            [-9000000, 12345, 67890], [24680, -13579, 112233],
            [1000000001, -2000000002, 3000000003], [-4000000004, 5000000005, -6000000006]),
        (2, [1.0, NULL, -3.0], [-4.0, 0.0, 6.5],
            [1, NULL, -3], [-4, 0, 6],
            [1, NULL, -3], [-4, 0, 6],
            [1, NULL, -3], [-4, 0, 6],
            [1, NULL, -3], [-4, 0, 6],
            [1, NULL, -3], [-4, 0, 6]),
        (3, NULL, [-4.0, 0.0, 6.5],
            NULL, [-4, 0, 6],
            NULL, [-4, 0, 6],
            NULL, [-4, 0, 6],
            NULL, [-4, 0, 6],
            NULL, [-4, 0, 6]),
        (4, [-0.0, 1000.5, -0.25], [3.5, -2.0, 0.125],
            [7, -8, 9], [-10, 11, -12],
            [30000, -20000, 10000], [-12345, 23456, -32768],
            [-7654321, 1234567, -999], [333, -444, 555],
            [2147483647, -2147483648, 4096], [-8192, 16384, -32768],
            [-9000000000, 8000000000, -7000000000], [6000000000, -5000000000, 4000000000])
    """

    qt_array_cross_product_10 """
        SELECT id, array_cross_product(lhs, rhs), cross_product(large_lhs, large_rhs)
        FROM test_array_cross_product_table
        WHERE id IN (1, 3, 4)
        ORDER BY id
    """

    qt_array_cross_product_11 """
        SELECT id,
               array_cross_product(tiny_lhs, tiny_rhs),
               array_cross_product(small_lhs, small_rhs),
               array_cross_product(int_lhs, int_rhs),
               array_cross_product(big_lhs, big_rhs),
               array_cross_product(large_lhs, large_rhs)
        FROM test_array_cross_product_table
        WHERE id IN (1, 3, 4)
        ORDER BY id
    """

    test {
        sql "SELECT array_cross_product([-11, NULL, 13], [17, -19, 23])"
        exception "cannot have null"
    }

    test {
        sql "SELECT cross_product([-11, NULL, 13], [17, -19, 23])"
        exception "cannot have null"
    }

    test {
        sql "SELECT array_cross_product(lhs, rhs) FROM test_array_cross_product_table WHERE id = 2"
        exception "cannot have null"
    }

    test {
        sql "SELECT array_cross_product([1.0, -2.0], [3.0, -4.0, 5.0])"
        exception "requires both input arrays to have exactly 3 elements"
    }
}
