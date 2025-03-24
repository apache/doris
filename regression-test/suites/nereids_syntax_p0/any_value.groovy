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

suite("any_value") {
    // enable nereids
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    test {
        sql "select any(s_suppkey), any(s_name), any_value(s_address) from supplier;"
    }
    qt_sql_max """select max(cast(concat(number, ":00:00") as time)) from numbers("number" = "100");"""
    qt_sql_min """select min(cast(concat(number, ":00:00") as time)) from numbers("number" = "100");"""
    sql """select any(cast(concat(number, ":00:00") as time)) from numbers("number" = "100");"""

    sql """DROP TABLE IF EXISTS any_test"""
    sql """
    CREATE TABLE `any_test` (
        `id` int(11) NULL,
        `c_array1` ARRAY<int(11)> NULL,
        `c_array2` ARRAY<int(11)> NOT NULL,
        `c_array3` ARRAY<string> NULL,
        `c_array4` ARRAY<string> NOT NULL,
        `s_info1` STRUCT<s_id:int(11), s_name:string, s_address:string> NULL,
        `s_info2` STRUCT<s_id:int(11), s_name:string, s_address:string> NOT NULL,
        `m1` Map<STRING, INT> NULL,
        `m2` Map<STRING, INT> NOT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`id`) BUCKETS 10
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "in_memory" = "false",
    "storage_format" = "V2"
    );
    """

    qt_sql_any1 """ select any(c_array1),any(c_array2),any(c_array3),any(c_array4),any(s_info1),any(s_info2),any(m1),any(m2) from any_test; """       
    qt_sql_any2 """ select any(c_array1),any(c_array2),any(c_array3),any(c_array4),any(s_info1),any(s_info2),any(m1),any(m2) from any_test group by id; """       
    sql """ insert into any_test values(1, array(1,2,3), array(4,5,6), array('a','b','c'), array('d','e','f'), named_struct('s_id', 1, 's_name', 'a', 's_address', 'b'), named_struct('s_id', 2, 's_name', 'c', 's_address', 'd'), map('a', 1, 'b', 2), map('c', 3, 'd', 4)); """
    qt_sql_any3 """ select any(c_array1),any(c_array2),any(c_array3),any(c_array4),any(s_info1),any(s_info2),any(m1),any(m2) from any_test; """       
    sql """ insert into any_test values(2, array(4,5,6), array(7,8,9), array('d','e','f'), array('g','h','i'), named_struct('s_id', 3, 's_name', 'e', 's_address', 'f'), named_struct('s_id', 4, 's_name', 'g', 's_address', 'h'), map('e', 5, 'f', 6), map('g', 7, 'h', 8)); """
    qt_sql_any4 """ select any(c_array1),any(c_array2),any(c_array3),any(c_array4),any(s_info1),any(s_info2),any(m1),any(m2) from any_test group by id order by id; """       

}