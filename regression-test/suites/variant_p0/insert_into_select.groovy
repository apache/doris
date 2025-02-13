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

suite("regression_test_variant_insert_into_select", "variant_type"){
    def table_name = "insert_into_select"
    sql "DROP TABLE IF EXISTS ${table_name}_var"
    sql "DROP TABLE IF EXISTS ${table_name}_str"
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name}_var (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 3
        properties("replication_num" = "1");
    """
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name}_str (
            k bigint,
            v string 
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 3
        properties("replication_num" = "1");
    """

    sql """insert into ${table_name}_var values (1, '{"a" : 1, "b" : [1], "c": 1.0}')"""
    sql """insert into ${table_name}_var values (2, '{"a" : 2, "b" : [1], "c": 2.0}')"""
    sql """insert into ${table_name}_var values (3, '{"a" : 3, "b" : [3], "c": 3.0}')"""
    sql """insert into ${table_name}_var values (4, '{"a" : 4, "b" : [4], "c": 4.0}')"""
    sql """insert into ${table_name}_var values (5, '{"a" : 5, "b" : [5], "c": 5.0}')"""
    sql """insert into ${table_name}_var values (6, '{"a" : 6, "b" : [6], "c": 6.0, "d" : [{"x" : 6}, {"y" : "6"}]}')"""
    sql """insert into ${table_name}_var values (7, '{"a" : 7, "b" : [7], "c": 7.0, "e" : [{"x" : 7}, {"y" : "7"}]}')"""
    sql """insert into ${table_name}_var values (8, '{"a" : 8, "b" : [8], "c": 8.0, "f" : [{"x" : 8}, {"y" : "8"}]}')"""

    sql """insert into ${table_name}_str select * from ${table_name}_var"""
    sql """insert into ${table_name}_var select * from ${table_name}_str"""
    sql """insert into ${table_name}_var select * from ${table_name}_var"""
    qt_sql """select v["a"], v["b"], v["c"], v['d'], v['e'], v['f'] from  ${table_name}_var order by k"""
    qt_sql "select v from  ${table_name}_str order by k"
    qt_sql """insert into ${table_name}_var select * from ${table_name}_str"""
    qt_sql """insert into ${table_name}_var select * from ${table_name}_var"""
    qt_sql """select v["a"], v["b"], v["c"], v['d'], v['e'], v['f'] from  insert_into_select_var order by k limit 215"""

    // test struct/map/array/json type into variant
    sql """ truncate table ${table_name}_var"""
    sql "DROP TABLE IF EXISTS ${table_name}_complex"
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name}_complex (
            k bigint,
            a array<string>,
            b map<int, string>,
            c struct<a:int, b:string, c:ipv4, d:decimal>,
            d json,
            a_s array<struct<a:int, b:string, d:datetime>>,
            b_s map<string, struct<a:int, b:string, d:datetime>>,
            c_s struct<a:array<int>, b:map<int, string>, c:struct<a:int, b:string, c:ipv4, d:decimal>>
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 3
        properties("replication_num" = "1");
    """
    sql """insert into ${table_name}_complex values (1, ['a', 'b', 'c'], map(1, 'a', 2, 'b'), named_struct('a', 1, 'b', 'b', 'c', '192.0.0.1', 'd', 1.0), '{"a": 1}', 
            array(named_struct('a', 1, 'b', 'b', 'd', '2021-01-01 00:00:00')), map('a', named_struct('a', 1, 'b', 'b', 'd', '2021-01-01 00:00:00')), 
            named_struct('a', array(1, 2), 'b', map(1, 'a', 2, 'b'), 'c', named_struct('a', 1, 'b', 'b', 'c', '192.0.0.1', 'd', 1.0)) )"""
    sql """ INSERT INTO ${table_name}_complex VALUES
            (2,
              ['a1','b1','c1'],
              map(1, 'a1', 2, 'b1'),
              named_struct('a', 1, 'b', 'b1', 'c', '192.168.1.1', 'd', 1.0),
              '{"a": 1}',
              array(named_struct('a', 1, 'b', 'b1', 'd', '2021-01-01 00:00:00')),
              map('a', named_struct('a', 1, 'b', 'b1', 'd', '2021-01-01 00:00:00')),
              named_struct('a', array(1, 2), 'b', map(1, 'a1', 2, 'b1'), 'c', named_struct('a', 1, 'b', 'b1', 'c', '192.168.1.1', 'd', 1.0))
            )"""
    sql """ INSERT INTO ${table_name}_complex VALUES (3,
          ['a1', 'b1', 'c1', 'd1', 'e1'],
          map(1, 'a1', 2, 'b1', 3, 'c1', 4, 'd1'),
          named_struct('a', 1, 'b', 'b1', 'c', '192.168.1.1', 'd', 1.0),
          '{"x": 1, "y": "test1", "z": [1,2,3]}',
          array(named_struct('a', 1, 'b', 'b1', 'd', '2021-01-01 00:00:00')),
          map('a', named_struct('a', 1, 'b', 'b1', 'd', '2021-01-01 00:00:00')),
          named_struct('a', [1, 2], 'b', map(1, 'a1', 2, 'b1'), 'c', named_struct('a', 1, 'b', 'b1', 'c', '192.168.1.1', 'd', 1.0))
        ),
        (4,
          [],
          {},
          named_struct('a', 2, 'b', 'b2', 'c', '192.168.1.2', 'd', 2.0),
          NULL,
          [],
          {},
          named_struct('a', NULL, 'b', NULL, 'c', named_struct('a', 2, 'b', 'b2', 'c', '192.168.1.2', 'd', 2.0))
        ),
        (5,
          ['x1', 'y1', 'z1'],
          map(10, 'x1', 20, 'y1', 30, 'z1'),
          named_struct('a', 3, 'b', 'b3', 'c', '192.168.1.3', 'd', 3.0),
          '{"data": ["alpha", "beta"], "flag": true}',
          array(named_struct('a', 3, 'b', 'b3', 'd', '2021-03-01 00:00:00')),
          map('b', named_struct('a', 3, 'b', 'b3', 'd', '2021-03-01 00:00:00')),
          named_struct('a', [3, 4, 5], 'b', map(5, 'c3', 6, 'd3'), 'c', named_struct('a', 3, 'b', 'b3', 'c', '192.168.1.3', 'd', 3.0))
        ),
        (6,
          ['m1', 'n1'],
          map(50, 'm1', 60, 'n1'),
          named_struct('a', 4, 'b', 'b4', 'c', '192.168.1.4', 'd', 4.0),
          '{"nested": {"key": "value"}}',
          NULL,
          map('c', named_struct('a', 4, 'b', 'b4', 'd', '2021-04-01 00:00:00')),
          named_struct('a', [4, 5, 6, 7], 'b', map(1, 'm4', 2, 'n4'), 'c', named_struct('a', 4, 'b', 'b4', 'c', '192.168.1.4', 'd', 4.0))
        ),
        (7,
          ['p1', 'q1', 'r1', 's1', 't1'],
          map(100, 'p1', 200, 'q1', 300, 'r1', 400, 's1', 500, 't1'),
          named_struct('a', 5, 'b', 'b5', 'c', '192.168.1.5', 'd', 5.0),
          '{"info": "random text", "count": 5}',
          array(named_struct('a', 5, 'b', 'b5', 'd', '2021-05-01 00:00:00')),
          {},
          named_struct('a', [5], 'b', map(3, 'r5'), 'c', named_struct('a', 5, 'b', 'b5', 'c', '192.168.1.5', 'd', 5.0))
        );
        """
        // select the origin table
        qt_sql """select * from ${table_name}_complex order by k"""

        // then insert into select from ${table_name}_complex to ${table_name}_var with key increment by 1
        sql """insert into ${table_name}_var select k, a from ${table_name}_complex"""
        sql """insert into ${table_name}_var select k+7, b from ${table_name}_complex"""
        sql """insert into ${table_name}_var select k+14, c from ${table_name}_complex"""
        sql """insert into ${table_name}_var select k+21, d from ${table_name}_complex"""
        sql """insert into ${table_name}_var select k+28, a_s from ${table_name}_complex"""
        sql """insert into ${table_name}_var select k+35, b_s from ${table_name}_complex"""
        sql """insert into ${table_name}_var select k+42, c_s from ${table_name}_complex"""
        qt_sql """select * from ${table_name}_var order by k"""
}
