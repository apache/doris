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

suite("fix_array_type_and_lambda_func") {
    sql """DROP TABLE IF EXISTS fix_array_type_and_lambda_func"""
    sql """
        CREATE TABLE
            fix_array_type_and_lambda_func (id INT, arr_col ARRAY<INT>, int_col INT, str_col VARCHAR(50))
            DISTRIBUTED BY HASH(id) BUCKETS 4 PROPERTIES ("replication_num" = "1");
    """
    sql """
    INSERT INTO fix_array_type_and_lambda_func (id, arr_col, int_col, str_col)
        VALUES (1, [1,2,3], 5, 'normal'), (2, [], 0, 'empty'), (3, [2147483647, -2147483648], 100, 'boundary'), (4, NULL, NULL, 'null'),
            (5, [1,2,3], 3, 'special$char');
    """
    // return type of array_with_constant is array.
    // do not optimizate
    qt_0 """
        SELECT array_with_constant(int_col, NULL) AS arr1, array_with_constant(int_col, NULL) AS arr2 FROM fix_array_type_and_lambda_func WHERE id = 4
    """

    sql """
        CREATE TABLE test_array_filter (id INT, arr_col ARRAY<INT>, str_col VARCHAR(20), int_col INT) DISTRIBUTED BY HASH(id) BUCKETS 4 PROPERTIES ("replication_num" = "1");
    """

    sql """
        INSERT INTO test_array_filter VALUES (1, [1,2,3,4,5], 'test1', 10), (2, [10,20,30], 'test2', 25), (3, [-5,0,5], 'test3', -3), (4, [], 'empty', 0), (5, [100,200,300], 'large', 150), (6, [1,1,2,3,5], 'fibonacci', 8), (7, [NULL,1,2], 'with_null', 1), (8, [2147483647,-2147483648], 'boundary', 0), (9, [1,2,3], 'normal', 5), (10, [0,0,0], 'zeros', 0); 
    """

    qt_1 """
        SELECT array_filter(x -> x > int_col + 5, arr_col) AS expr1, array_filter(x -> x > int_col + 5, arr_col) AS expr2 FROM test_array_filter WHERE array_filter(x -> x > int_col + 5, arr_col) IS NOT NULL;
    """
}