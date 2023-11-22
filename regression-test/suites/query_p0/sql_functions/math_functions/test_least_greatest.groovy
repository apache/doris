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

suite("test_least_greatest") {
    qt_select "SELECT LEAST(1)"
    qt_select "SELECT LEAST(1,2,3)"
    qt_select "SELECT LEAST(1111,2222,3333)"
    qt_select "SELECT LEAST(1111111,2222222,3333333)"
    qt_select "SELECT LEAST(1.1,1.2,1.3)"
    qt_select "SELECT LEAST(1.1,10,2)"
    qt_select "SELECT LEAST('2020-01-01 01:01:01', '2020-01-02 01:01:01', '2020-01-03 01:01:01')"
    qt_select "SELECT LEAST('2020-01-01', '2020-01-02', '2020-01-03')"
    qt_select "SELECT LEAST('aaa', 'bbb', 'ccc')"

    qt_select "SELECT GREATEST(1)"
    qt_select "SELECT GREATEST(1,2,3)"
    qt_select "SELECT GREATEST(1111,2222,3333)"
    qt_select "SELECT GREATEST(1111111,2222222,3333333)"
    qt_select "SELECT GREATEST(1.1,1.2,1.3)"
    qt_select "SELECT GREATEST(1.1,10,2)"
    qt_select "SELECT GREATEST('2020-01-01 01:01:01', '2020-01-02 01:01:01', '2020-01-03 01:01:01')"
    qt_select "SELECT GREATEST('2020-01-01', '2020-01-02', '2020-01-03')"
    qt_select "SELECT GREATEST('aaa', 'bbb', 'ccc')"

    sql """ drop table if exists test_least_greatest; """
    sql """ create table test_least_greatest(
        k1 INT,
        v1 DOUBLE,
        v2 DECIMALV3,
        v3 DATEV2,
        v4 DATETIMEV2,
        v5 STRING
    ) distributed by hash (k1) buckets 1
    properties ("replication_num"="1");
    """
    sql """ insert into test_least_greatest values
        (1, 1.1, 1.111111, '2020-01-01', '2020-01-01 01:01:01', 'aaa'),
        (2, 2.2, 2.222222, '2020-02-01', '2020-02-01 01:01:01', 'bbb'),
        (3, 3.3, 3.333333, '2020-03-01', '2020-03-01 01:01:01', 'ccc')
    """

    qt_select "SELECT LEAST(1) FROM test_least_greatest order by k1"
    qt_select "SELECT LEAST(1,2,3) FROM test_least_greatest order by k1"
    qt_select "SELECT LEAST(1111,2222,3333) FROM test_least_greatest order by k1"
    qt_select "SELECT LEAST(1111111,2222222,3333333) FROM test_least_greatest order by k1"
    qt_select "SELECT LEAST(1.1,1.2,1.3) FROM test_least_greatest order by k1"
    qt_select "SELECT LEAST(1.1,10,2) FROM test_least_greatest order by k1"
    qt_select "SELECT LEAST('2020-01-01 01:01:01', '2020-01-02 01:01:01', '2020-01-03 01:01:01') FROM test_least_greatest order by k1"
    qt_select "SELECT LEAST('2020-01-01', '2020-01-02', '2020-01-03') FROM test_least_greatest order by k1"
    qt_select "SELECT LEAST('aaa', 'bbb', 'ccc') FROM test_least_greatest order by k1"

    qt_select "SELECT LEAST(k1) FROM test_least_greatest order by k1"
    qt_select "SELECT LEAST(v1) FROM test_least_greatest order by k1"
    qt_select "SELECT LEAST(v2) FROM test_least_greatest order by k1"
    qt_select "SELECT LEAST(v3) FROM test_least_greatest order by k1"
    qt_select "SELECT LEAST(v4) FROM test_least_greatest order by k1"
    qt_select "SELECT LEAST(v5) FROM test_least_greatest order by k1"

    qt_select "SELECT LEAST(k1, k1) FROM test_least_greatest order by k1"
    qt_select "SELECT LEAST(v1, v1) FROM test_least_greatest order by k1"
    qt_select "SELECT LEAST(v2, v2) FROM test_least_greatest order by k1"
    qt_select "SELECT LEAST(v3, v3) FROM test_least_greatest order by k1"
    qt_select "SELECT LEAST(v4, v4) FROM test_least_greatest order by k1"
    qt_select "SELECT LEAST(v5, v5) FROM test_least_greatest order by k1"

    qt_select "SELECT LEAST(100, k1) FROM test_least_greatest order by k1"
    qt_select "SELECT LEAST(100.100, v1) FROM test_least_greatest order by k1"
    qt_select "SELECT LEAST(100.00001, v2) FROM test_least_greatest order by k1"
    qt_select "SELECT LEAST('2021-01-01', v3) FROM test_least_greatest order by k1"
    qt_select "SELECT LEAST('2021-01-01 01:01:01', v4) FROM test_least_greatest order by k1"
    qt_select "SELECT LEAST('zzz', v5) FROM test_least_greatest order by k1"

    qt_select "SELECT GREATEST(1) FROM test_least_greatest order by k1"
    qt_select "SELECT GREATEST(1,2,3) FROM test_least_greatest order by k1"
    qt_select "SELECT GREATEST(1111,2222,3333) FROM test_least_greatest order by k1"
    qt_select "SELECT GREATEST(1111111,2222222,3333333) FROM test_least_greatest order by k1"
    qt_select "SELECT GREATEST(1.1,1.2,1.3) FROM test_least_greatest order by k1"
    qt_select "SELECT GREATEST(1.1,10,2) FROM test_least_greatest order by k1"
    qt_select "SELECT GREATEST('2020-01-01 01:01:01', '2020-01-02 01:01:01', '2020-01-03 01:01:01') FROM test_least_greatest order by k1"
    qt_select "SELECT GREATEST('2020-01-01', '2020-01-02', '2020-01-03') FROM test_least_greatest order by k1"
    qt_select "SELECT GREATEST('aaa', 'bbb', 'ccc') FROM test_least_greatest order by k1"

    qt_select "SELECT GREATEST(k1) FROM test_least_greatest order by k1"
    qt_select "SELECT GREATEST(v1) FROM test_least_greatest order by k1"
    qt_select "SELECT GREATEST(v2) FROM test_least_greatest order by k1"
    qt_select "SELECT GREATEST(v3) FROM test_least_greatest order by k1"
    qt_select "SELECT GREATEST(v4) FROM test_least_greatest order by k1"
    qt_select "SELECT GREATEST(v5) FROM test_least_greatest order by k1"

    qt_select "SELECT GREATEST(k1, k1) FROM test_least_greatest order by k1"
    qt_select "SELECT GREATEST(v1, v1) FROM test_least_greatest order by k1"
    qt_select "SELECT GREATEST(v2, v2) FROM test_least_greatest order by k1"
    qt_select "SELECT GREATEST(v3, v3) FROM test_least_greatest order by k1"
    qt_select "SELECT GREATEST(v4, v4) FROM test_least_greatest order by k1"
    qt_select "SELECT GREATEST(v5, v5) FROM test_least_greatest order by k1"

    qt_select "SELECT GREATEST(100, k1) FROM test_least_greatest order by k1"
    qt_select "SELECT GREATEST(100.100, v1) FROM test_least_greatest order by k1"
    qt_select "SELECT GREATEST(100.00001, v2) FROM test_least_greatest order by k1"
    qt_select "SELECT GREATEST('2021-01-01', v3) FROM test_least_greatest order by k1"
    qt_select "SELECT GREATEST('2021-01-01 01:01:01', v4) FROM test_least_greatest order by k1"
    qt_select "SELECT GREATEST('zzz', v5) FROM test_least_greatest order by k1"

    sql """ drop table if exists test_least_greatest; """

    sql """ drop table if exists test_least_greatest2; """
    sql """
        CREATE TABLE `test_least_greatest2` (
            id int,
            name1 decimalv3(9,2),
            name2 decimalv3(9,2)
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1");
    """
    sql """insert into test_least_greatest2 values(1, 12.34, 23.45), (2, 34.45, 45.56);"""
    sql "sync"
    qt_decimalv3_scale0 " select least(name1, name2) from test_least_greatest2 order by name1, name2;"
    qt_decimalv3_scale1 " select greatest(name1, name2) from test_least_greatest2 order by name1, name2;"
}


