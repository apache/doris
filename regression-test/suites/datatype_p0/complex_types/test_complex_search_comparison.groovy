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

suite("test_complex_search_comparison") {
    order_qt_constant_search """
        SELECT number,
            complex_equals(named_struct('a', CAST('[]' AS ARRAY<INT>), 'd', CAST(number AS DOUBLE)),
                           named_struct('a', [1, 2, 3], 'd', CAST(0 AS DOUBLE))),
            complex_equals(named_struct('a', [1, 2, 3], 'd', CAST(0 AS DOUBLE)),
                           named_struct('a', CAST('[]' AS ARRAY<INT>), 'd', CAST(number AS DOUBLE)))
        FROM numbers('number' = '4') ORDER BY number
    """
    order_qt_nested_maps """
        SELECT complex_equals(
            CAST('{{1:1,1:2},{2:3}}' AS STRUCT<m1:MAP<INT,INT>,m2:MAP<INT,INT>>),
            CAST('{{1:2},{2:3}}' AS STRUCT<m1:MAP<INT,INT>,m2:MAP<INT,INT>>))
    """
    order_qt_nulls """
        SELECT complex_equals(CAST(NULL AS STRUCT<v:INT>), CAST(NULL AS STRUCT<v:INT>)),
               complex_equals(CAST(NULL AS STRUCT<v:INT>), named_struct('v', CAST(NULL AS INT))),
               complex_equals(named_struct('v', CAST(NULL AS INT)), named_struct('v', CAST(NULL AS INT)))
    """

    test {
        sql "SELECT complex_equals(to_bitmap(1), to_bitmap(1))"
        exception "complex_equals does not support argument type BITMAP"
    }
    test {
        sql "SELECT complex_equals(named_struct('v', to_bitmap(1)), named_struct('v', to_bitmap(1)))"
        exception "complex_equals does not support argument type"
    }
}
