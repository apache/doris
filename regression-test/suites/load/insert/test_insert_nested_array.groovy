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

suite("test_insert_nested_array", "load") {
    def test_nested_array_2_depths = {
        def tableName = "nested_array_test_2_vectorized"

        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `key` INT,
                value ARRAY<ARRAY<INT>>
            ) DUPLICATE KEY (`key`) DISTRIBUTED BY HASH (`key`) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
        """

        sql "INSERT INTO ${tableName} VALUES (1, [])"
        sql "INSERT INTO ${tableName} VALUES (2, [null])"
        sql "INSERT INTO ${tableName} VALUES (3, [[]])"
        sql "INSERT INTO ${tableName} VALUES (4, [[1, 2, 3], [4, 5, 6]])"
        sql "INSERT INTO ${tableName} VALUES (5, [[1, 2, 3], null, [4, 5, 6]])"
        sql "INSERT INTO ${tableName} VALUES (6, [[1, 2, null], null, [4, null, 6], null, [null, 8, 9]])"
        sql """
            INSERT INTO ${tableName} VALUES
                (1, []),
                (2, [null]),
                (3, [[]]),
                (4, [[1, 2, 3], [4, 5, 6]]),
                (5, [[1, 2, 3], null, [4, 5, 6]]),
                (6, [[1, 2, null], null, [4, null, 6], null, [null, 8, 9]])
        """
        qt_select "select * from ${tableName} order by `key`"
    }

    def test_nested_array_3_depths = {
        def tableName = "nested_array_test_3_vectorized"

        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `key` INT,
                value ARRAY<ARRAY<ARRAY<INT>>>
            ) DUPLICATE KEY (`key`) DISTRIBUTED BY HASH (`key`) BUCKETS 1
            PROPERTIES ('replication_num' = '1')
        """

        sql "INSERT INTO ${tableName} VALUES (1, [])"
        sql "INSERT INTO ${tableName} VALUES (2, [null])"
        sql "INSERT INTO ${tableName} VALUES (3, [[]])"
        sql "INSERT INTO ${tableName} VALUES (4, [[null]])"
        sql "INSERT INTO ${tableName} VALUES (5, [[[]]])"
        sql "INSERT INTO ${tableName} VALUES (6, [[[null]], [[1], [2, 3]], [[4, 5, 6], null, null]])"
        sql """
            INSERT INTO ${tableName} VALUES
                (1, []),
                (2, [null]),
                (3, [[]]),
                (4, [[null]]),
                (5, [[[]]]),
                (6, [[[null]], [[1], [2, 3]], [[4, 5, 6], null, null]])
        """
        qt_select "select * from ${tableName} order by `key`"
    }

    test_nested_array_2_depths.call()

    test_nested_array_3_depths.call()
}
