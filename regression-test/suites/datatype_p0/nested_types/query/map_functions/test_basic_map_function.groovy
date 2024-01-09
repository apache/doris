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

suite("test_basic_map_function", "p0") {
    sql """ ADMIN SET FRONTEND CONFIG ('disable_nested_complex_type' = 'false'); """
    sql """set enable_nereids_planner=false"""
    // ============ sum(map-value) ============
    qt_sql """ SELECT "sum-map-value" """
    sql """ DROP TABLE IF EXISTS t_map_amory;"""
    sql """ CREATE TABLE IF NOT EXISTS t_map_amory(id int(11), m Map<String, largeint>) ENGINE=OLAP DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num' = '1');"""
    sql """ INSERT INTO t_map_amory VALUES(1, map("a", 1, "b", 2, "c", 3));"""
    sql """ INSERT INTO t_map_amory VALUES(1, map(concat("key",'a') , 1, "b", 2, "c", 3));"""
    sql """ INSERT INTO t_map_amory VALUES(2, map("a", 1, "b", 2, "c", 3));"""
    order_qt_sql """ SELECT id, sum(m['a']), sum(m['b']), sum(m['c']), sum(m['c'] = 0) FROM t_map_amory group by id order by id; """

    // ============ literal array-map ============
    qt_sql """ SELECT "literal-array-map" """
    sql """ DROP TABLE IF EXISTS arr_nested_map_test_table;"""
    sql """ CREATE TABLE IF NOT EXISTS arr_nested_map_test_table (id int, arr array<map<string, decimal(28,12)>>) ENGINE=OLAP DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num' = '1');"""
    // old planner can't support literal array-map "No matching function with signature"
    // sql """ INSERT INTO arr_nested_map_test_table VALUES(1, array(map("a", 1.1, "b", 2.2), map("c", 3.3, "d", 4.4)));"""
    sql """ INSERT INTO arr_nested_map_test_table VALUES(2, [{'l': 0.0, 'h': 10000.0, 't': 0.1}, {'l': 10001.0, 'h': 100000000000000.0, 't': 0.2}]);"""
    order_qt_sql """ SELECT * FROM arr_nested_map_test_table order by id ; """
    order_qt_sql """ SELECT arr[1]['h'], arr[1]['t'], arr[2]['l'], arr[2]['t'] FROM arr_nested_map_test_table order by id ; """

    // ============ insert into select literal ============
    qt_sql """ SELECT "insert-into-select-literal" """
    sql """ DROP TABLE IF EXISTS t_map_amory_1;"""
    sql """ CREATE TABLE IF NOT EXISTS t_map_amory_1(id datetime, m Map<largeint, String>) ENGINE=OLAP DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num' = '1');"""
    // old planner will make type deduction error
    // sql """ INSERT INTO t_map_amory_1 SELECT '2020-01-01', map(-1, 'a', 0, 'b', cast('1234567898765432123456789' as largeint), 'c', cast('-1234567898765432123456789' as largeint), 'd');"""
    sql """ INSERT INTO t_map_amory_1 SELECT '2020-01-01', map(cast('1234567898765432123456789' as largeint), 'c', cast('-1234567898765432123456789' as largeint), 'd', -1, 'a', 0, 'b'); """
    qt_sql """ SELECT * FROM t_map_amory_1; """
    qt_sql """ SELECT m[-1], m[0], m[cast('1234567898765432123456789' as largeint)], m[cast('-1234567898765432123456789' as largeint)] FROM t_map_amory_1;"""
    qt_sql """ SELECT m[cast(0 as BIGINT)],  m[cast(-1 as Largeint)], m[cast(0 as largeint)] FROM t_map_amory_1;"""

    // =================== cast literal ===================
    qt_sql """ SELECT "cast-literal" """
    qt_sql """ SELECT CAST({'amory':26, 'amory_up':34} as Map<String, int>);"""
    qt_sql """ SELECT CAST('{\\'amory\\':26, \\'amory_up\\':34}' as Map<String, int>)"""
    qt_sql """ SELECT CAST({'amory':[26, 34], 'commiter':[2023, 2024]} as Map<String, Array<int>>)"""
    qt_sql """ SELECT CAST('{\\'amory\\':[26, 34], \\'commiter\\':[2023, 2024]}' as Map<String, Array<int>>)"""
    qt_sql """ SELECT CAST({'amory':{'in': 2023}, 'amory_up':{'commiter': 2024}} as Map<String, Map<String, int>>)"""
    qt_sql """ SELECT CAST('{\\'amory\\':{\\'in\\': 2023}, \\'amory_up\\':{\\'commiter\\': 2024}}' as Map<String, Map<String, int>>)"""


    // test in nereids planner
    sql """set enable_nereids_planner=true"""
    sql """ set enable_fallback_to_original_planner=false"""

    // ============ sum(map-value) ============
    qt_sql """ SELECT "sum-map-value" """
    sql """ DROP TABLE IF EXISTS t_map_amory;"""
    sql """ CREATE TABLE IF NOT EXISTS t_map_amory(id int(11), m Map<String, largeint>) ENGINE=OLAP DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num' = '1');"""
    sql """ INSERT INTO t_map_amory VALUES(1, map("a", 1, "b", 2, "c", 3));"""
    sql """ INSERT INTO t_map_amory VALUES(1, map(concat("key",'a'), 1, "b", 2, "c", 3));"""
    sql """ INSERT INTO t_map_amory VALUES(2, map("a", 1, "b", 2, "c", 3));"""
    order_qt_nereid_sql """ SELECT id, sum(m['a']), sum(m['b']), sum(m['c']), sum(m['c'] = 0) FROM t_map_amory group by id order by id; """

    // ============ literal array-map ============
    qt_sql """ SELECT "literal-array-map" """
    sql """ DROP TABLE IF EXISTS arr_nested_map_test_table;"""
    sql """ CREATE TABLE IF NOT EXISTS arr_nested_map_test_table (id int, arr array<map<string, decimal(28,12)>>) ENGINE=OLAP DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num' = '1');"""
    sql """ INSERT INTO arr_nested_map_test_table VALUES(1, array(map("a", 1.1, "b", 2.2), map("c", 3.3, "d", 4.4)));"""
    sql """ INSERT INTO arr_nested_map_test_table VALUES(2, [{'l': 0.0, 'h': 10000.0, 't': 0.1}, {'l': 10001.0, 'h': 100000000000000.0, 't': 0.2}]);"""
    order_qt_nereid_sql """ SELECT * FROM arr_nested_map_test_table order by id ; """
    order_qt_nereid_sql """ SELECT arr[1]['a'], arr[1]['b'], arr[2]['l'], arr[2]['t'] FROM arr_nested_map_test_table order by id ; """

    // ============ insert into select literal ============
    qt_sql """ SELECT "insert-into-select-literal" """
    sql """ DROP TABLE IF EXISTS t_map_amory_1;"""
    sql """ CREATE TABLE IF NOT EXISTS t_map_amory_1(id datetime, m Map<largeint, String>) ENGINE=OLAP DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num' = '1');"""
    sql """ INSERT INTO t_map_amory_1 SELECT '2020-01-01', map(-1, 'a', 0, 'b', cast('1234567898765432123456789' as largeint), 'c', cast('-1234567898765432123456789' as largeint), 'd');"""
    qt_nereid_sql """ SELECT * FROM t_map_amory_1; """
    qt_nereid_sql """ SELECT m[-1], m[0], m[cast('1234567898765432123456789' as largeint)], m[cast('-1234567898765432123456789' as largeint)] FROM t_map_amory_1;"""
    qt_nereid_sql """ SELECT m[cast(0 as BIGINT)],  m[cast(-1 as Largeint)], m[cast(0 as largeint)] FROM t_map_amory_1;"""

    // =================== cast literal ===================
    qt_sql """ SELECT "cast-literal" """
    qt_nereid_sql """ SELECT CAST({'amory':26, 'amory_up':34} as Map<String, int>);"""
    qt_nereid_sql """ SELECT CAST('{\\'amory\\':26, \\'amory_up\\':34}' as Map<String, int>)"""
    qt_nereid_sql """ SELECT CAST({'amory':[26, 34], 'commiter':[2023, 2024]} as Map<String, Array<int>>)"""
    qt_nereid_sql """ SELECT CAST('{\\'amory\\':[26, 34], \\'commiter\\':[2023, 2024]}' as Map<String, Array<int>>)"""
    qt_nereid_sql """ SELECT CAST({'amory':{'in': 2023}, 'amory_up':{'commiter': 2024}} as Map<String, Map<String, int>>)"""
    qt_nereid_sql """ SELECT CAST('{\\'amory\\':{\\'in\\': 2023}, \\'amory_up\\':{\\'commiter\\': 2024}}' as Map<String, Map<String, int>>)"""




}
