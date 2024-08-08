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

suite("test_serde_dialect", "p0") {

    sql """create database if not exists test_serde_dialect;"""
    sql """use test_serde_dialect;"""
    sql """drop table if exists test_serde_dialect_tbl"""
    sql """
        create table if not exists test_serde_dialect_tbl (
            c1 tinyint,
            c2 smallint,
            c3 int,
            c4 bigint,
            c5 largeint,
            c6 float,
            c7 double,
            c8 decimal(27, 9),
            c9 date,
            c10 datetime,
            c11 datetime(6),
            c12 ipv4,
            c13 ipv6,
            c14 string,
            c15 char(6),
            c16 varchar(1024),
            c17 boolean,
            c18 json,
            c19 array<int>,
            c20 array<double>,
            c21 array<decimal(10, 5)>,
            c22 array<string>,
            c23 array<map<string, string>>,
            c24 array<array<string>>,
            c25 array<struct<s_id:int(11), s_name:string, s_address:string>>,
            c26 array<struct<s_id:struct<k1:string, k2:decimal(10,2)>, s_name:array<ipv4>, s_address:map<string, ipv6>>>,
            c27 map<string, string>,
            c28 map<string, array<array<string>>>,
            c29 map<int, map<string, array<array<string>>>>,
            c30 map<decimal(5, 3), array<struct<s_id:struct<k1:string, k2:decimal(10,2)>, s_name:array<string>, s_address:map<string, string>>>>,
            c31 struct<s_id:int(11), s_name:string, s_address:string>,
            c32 struct<s_id:int(11), s_name:array<string>, s_address:string>,
            c33 array<date>,
            c34 array<datetime(3)>
        )
        distributed by random buckets 1
        properties("replication_num" = "1");
    """

    // Some presto sql examples:
    // select array[1,2,3,null,5];
    // select array[1.1,2.1,3.1,null,5.00];
    // select array[1.1,2.1,3.00000,null,5.12345];
    // select array['abc', 'de, f"', null, ''];
    // select array[map(array['k1','k2','k3','k4'], array['v1',null,'','a , "a']), map(array['k1','k2','k3 , "abc':'','k4'], array['v1',null,'','a , "a'])];
    // select array[array['abc', 'de, f"', null, ''], array[], null];
    // select map(array['k1','k2','k3','k4'], array['v1',null,'','a , "a']);
    // select cast(row(100, 'abc , "', null) as ROW(s_id bigint, s_name varchar, s_address varchar));
    // select cast(row(null, array['abc', 'de, f"', null, ''], '') as ROW(s_id bigint, s_name array<varchar>, s_address varchar));
    sql """
        insert into test_serde_dialect_tbl
        (c1, c2,c3, c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,c21,c22,c23,c24,c27,c28,c29,c31,c32,c33,c34)
        values(
        1,2,3,4,5,1.1,2.0000,123456.123456789,"2024-06-30", "2024-06-30 10:10:11", "2024-06-30 10:10:11.123456",
        '59.50.185.152',
        'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff',
        'this is a string with , and "',
        'abc ef',
        ' 123ndedwdw',
        true,
        '[1, 2, 3, 4, 5]',
        [1,2,3,null,5],
        [1.1,2.1,3.1,null,5.00],
        [1.1,2.1,3.00000,null,5.12345],
        ['abc', 'de, f"', null, ''],
        [{'k1': 'v1', 'k2': null, 'k3':'', 'k4':'a , "a'}, {'k1': 'v1', 'k2': null, 'k3 , "abc':'', 'k4':'a , "a'}],
        [['abc', 'de, f"', null, ''],[],null],
        {'k1': 'v1', 'k2': null, 'k3':'', 'k4':'a , "a'},
        {'k1': [['abc', 'de, f"', null, ''],[],null], 'k2': null},
        {10: {'k1': [['abc', 'de, f"', null, ''],[],null]}, 11: null},
        named_struct('s_id', 100, 's_name', 'abc , "', 's_address', null),
        named_struct('s_id', null, 's_name', ['abc', 'de, f"', null, ''], 's_address', ''),
        ['2024-06-01',null,'2024-06-03'],
        ['2024-06-01 10:10:10',null,'2024-06-03 01:11:23.123']
        );
    """

    sql """set serde_dialect="doris";"""
    qt_sql01 """select * from test_serde_dialect_tbl"""
    sql """set serde_dialect="presto";"""
    qt_sql01 """select * from test_serde_dialect_tbl"""

    test {
        sql """set serde_dialect="invalid""""
        exception "sqlDialect value is invalid"
    }
}
