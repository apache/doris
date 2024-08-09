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

suite("test_array_functions_array_intersect_sort", "p0") {
    // ========= array_intersect ===========
    // with sort
    qt_nereid_sql "SELECT 'array_intersect-array-sort';"
    sql "drop table if exists tbl_array_intersect;"
    sql "create table tbl_array_intersect (date Date, arr Array<Int>)  ENGINE=OLAP DISTRIBUTED BY HASH(date) BUCKETS 1 PROPERTIES('replication_num' = '1');"

    sql "insert into tbl_array_intersect values ('2019-01-01', [1,2,3]);"
    sql "insert into tbl_array_intersect values ('2019-01-02', [1,2]);"
    sql "insert into tbl_array_intersect values ('2019-01-03', [1]);"
    sql "insert into tbl_array_intersect values ('2019-01-04', []);"

    order_qt_nereid_sql "SELECT array_sort(array_intersect(arr, [1,2])) from tbl_array_intersect order by date;"
    order_qt_nereid_sql "SELECT array_sort(array_intersect(arr, [])) from tbl_array_intersect order by date;"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([], arr)) from tbl_array_intersect order by date;"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([1,2], arr)) from tbl_array_intersect order by date;"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([1,2], [1,2,3,4])) from tbl_array_intersect order by date;"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([], [])) from tbl_array_intersect order by date;"


    order_qt_nereid_sql "SELECT array_sort(array_intersect(arr, [1,2])) from tbl_array_intersect order by date;"
    order_qt_nereid_sql "SELECT array_sort(array_intersect(arr, [])) from tbl_array_intersect order by date;"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([], arr)) from tbl_array_intersect order by date;"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([1,2], arr)) from tbl_array_intersect order by date;"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([1,2], [1,2,3,4])) from tbl_array_intersect order by date;"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([], [])) from tbl_array_intersect order by date;"


    order_qt_nereid_sql "SELECT array_sort(array_intersect([-100], [156]));"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([1], [257]));"

    order_qt_nereid_sql "SELECT array_sort(array_intersect(['a', 'b', 'c'], ['a', 'a']));"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([1, 1], [2, 2]));"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([1, 1], [1, 2]));"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([1, 1, 1], [3], [2, 2, 2]));"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([1, 2], [1, 2], [2]));"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([1, 1], [2, 1], [2, 2], [1]));"
    order_qt_nereid_sql "SELECT array_sort(array_intersect([1, 1], [2, 1], [2, 2], [2, 2, 2]));"

    test {
        sql "SELECT array_sort(array_intersect([]))"
        exception "Can not found function 'array_intersect' which has 1 arity"
    }
    test {
        sql "SELECT array_sort(array_intersect([1, 2, 3]))"
        exception "Can not found function 'array_intersect' which has 1 arity"
    }
}
