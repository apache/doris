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

suite("test_array_split") {
    sql " set parallel_pipeline_task_num = 1; "
    qt_sql1 " select array_split([1,2,3,4,5], [1,0,0,0,0]); "
    qt_sql2 " select array_split(nullable([1,2,3,4,5]), [1,0,0,0,0]); "
    qt_sql3 " select array_split([1,2,3,4,5], nullable([1,0,0,0,0])); "
    qt_sql4 " select array_split(nullable([1,2,3,4,5]), nullable([1,0,0,0,0])); "
    qt_sql5 " select array_split(cast(null as array<int>), [1,0,0]); "
    qt_sql6 " select array_split([1,2,3], cast(null as array<tinyint>)); "
    qt_sql7 " select array_split(cast(null as array<int>), cast(null as array<tinyint>)); "
    qt_sql8 " select array_reverse_split([1,2,3,4,5], [0,0,0,1,0]); "
    qt_lambda1 " select array_split((x,y)->y, [1,2,3,4,5], [1,0,0,0,0]); "
    qt_lambda2 " select array_reverse_split((x,y)->(y+1), ['a', 'b', 'c', 'd'], [-1, -1, 0, -1]); "
    qt_lambda3 " select array_reverse_split(x->(x>'a'), ['a', 'b', 'c', 'd']); "
    qt_null1 " select array_split([1,2,3,4,5], [null,null,1,0,0]); "
    qt_null2 " select array_reverse_split([1,null,null,4,5], [null,null,1,0,0]); "

    sql " drop table if exists arr_int; "
    sql """
        create table arr_int(
            x int,
            a0 array<int> NULL,
            s0 array<int> NULL,
            a1 array<int> NOT NULL,
            s1 array<int> NOT NULL
        )
        DISTRIBUTED BY HASH(`x`) BUCKETS auto
        properties("replication_num" = "1");
    """
    sql """
        insert into arr_int values
                (1, [1,2,3,4,5], [1,1,1,1,1], [1,2,3,4,5], [1,1,1,1,1]),
                (2, [2,3,4], [1,0,1], [2,3,4], [1,0,1]),
                (3, NULL, [1,1,1,1,1], [1,2,3,4,5], [1,1,1,1,1]),
                (4, [1,2,3,4,5], NULL, [1,2,3,4,5], [1,1,1,1,1]);
    """
    qt_table1 " select x, array_split(a0, s0), array_split(a1, s1) from arr_int order by x; "
    qt_table2 " select x, array_reverse_split(a0, s0), array_reverse_split(a1, s1) from arr_int order by x; "

    sql " drop table if exists dt; "
    sql """
        create table dt(
            x int,
            k0 array<datetime(6)>
        )
        DISTRIBUTED BY HASH(`x`) BUCKETS auto
        properties("replication_num" = "1");
    """
    sql """ insert into dt values
        (1, ["2020-12-12", "2013-12-12", "2015-12-12", null]), 
        (2, ["2020-12-12", "2013-12-12", "2015-12-12", null, "2200-12-12 12:12:12.123456"]); """

    qt_dt1 """ select x, array_split(x->(year(x)>2013), k0), array_reverse_split(x->(year(x)>2013), k0)
            from dt order by x; """
    qt_dt_null """ select x, array_reverse_split(x->(null_or_empty(x)), k0) from dt order by x; """

    test {
        sql " select array_split([1,2,3,4,5], [1,1,1]); "
        exception "function array_split has uneven arguments on row 0"
    }
    test {
        sql " select array_reverse_split((x,y)->(y), [1,2,3,4,5], [1,1,1]); "
        exception "in array map function, the input column size are not equal completely"
    }
}