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

suite("test_coalesce", "query,p0") {
    sql "use test_query_db"

    def tableName1 = "test"
    def tableName2 = "baseall"

    for (k in range(1, 12)) {
        qt_coalesce1 "select k1, coalesce(k${k}) from ${tableName2} order by 1"
        qt_coalesce2 "select k1, coalesce(k${k}, k${k}) from ${tableName2} order by 1"
        qt_coalesce3 "select k1, coalesce(k${k}, null) from ${tableName2} order by 1"
        qt_coalesce4 "select k1, coalesce(null, k${k}) from ${tableName2} order by 1"
    }
    qt_coalesce5 "select coalesce(k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, null) from ${tableName1} order by 1"
    qt_coalesce6 "select coalesce(k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11) from ${tableName1} order by 1"
    qt_coalesce7 "select * from (select coalesce(\"string\", \"\")) a"
    qt_coalesce8 "select * from ${tableName2} where coalesce(k1, k2) in (1, null) order by 1, 2, 3, 4"
    qt_coalesce9 "select * from ${tableName1} where coalesce(k1, null) in (1, null) order by 1, 2, 3, 4, 5, 6"
    qt_coalesce10 "select  coalesce(1, null)"
    qt_coalesce11 """ select coalesce(null, json_parse('{"k": "v"}')); """

    test {
        sql """
            select coalesce(null,to_json('{"k": 123}'), map("a", 123));
        """
        exception "cannot cast MAP"
    }

    sql "drop table if exists `json_test`;"
    sql """
        create table `json_test` (
            id int,
            value json
        ) properties('replication_num' = '1');
    """

    sql """
        insert into `json_test` values
            (1, '{"k": 123}'),
            (2, null),
            (3, '{"k": 789}');
    """

    qt_coalesce_json """
        select id, coalesce(value, '{"k": 0}') from `json_test` order by 1;
    """

    sql "drop table if exists `map_test`;"
    sql """
        create table `map_test` (
            id int,
            value map<string, int>
        ) properties('replication_num' = '1');
    """

    sql """
        insert into `map_test` values
            (1, '{"k": 123}'),
            (2, null),
            (3, '{"k": 789}');
    """

    qt_coalesce_map """
        select id, coalesce(value, '{"k": 0}') from `map_test` order by 1;
    """

    sql "drop table if exists `array_test`;"
    sql """
        create table `array_test` (
            id int,
            value array<int>
        ) properties('replication_num' = '1');
    """

    sql """
        insert into `array_test` values
            (1, '[123, 456]'),
            (2, null),
            (3, '[789]');
    """

    qt_coalesce_array """
        select id, coalesce(value, '[0]') from `array_test` order by 1;
    """

    qt_coalesce_array2 """
        select id, coalesce(value, value) from `array_test` order by 1;
    """

    sql "drop table if exists `struct_test`;"
    sql """
        create table `struct_test` (
            id int,
            value STRUCT<name: VARCHAR(10), age: INT>
        ) properties('replication_num' = '1');
    """

    sql """
        insert into `struct_test` values
            (1, STRUCT("Alice", 30)),
            (2, null),
            (3, STRUCT("Bob", 25));
    """

    qt_coalesce_struct """
        select id, coalesce(value, STRUCT("Charlie", 18)) from `struct_test` order by 1;
    """
}
