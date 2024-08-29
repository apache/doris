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

suite("regression_test_variant_predefine_schema", "p0"){
    sql """DROP TABLE IF EXISTS test_predefine"""
    sql """
        CREATE TABLE `test_predefine` (
            `id` bigint NOT NULL,
            `type` varchar(30) NULL,
            `v1` variant<a.b.c:int,ss:string,dcm:decimal,dt:datetime,ip:ipv4,a.b.d:double> NULL,
            INDEX idx_var_sub(`v1`) USING INVERTED PROPERTIES("parser" = "english", "sub_column_path" = "a.b.c") )
        ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 3
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1");
    """

    sql """insert into test_predefine values(1, '1', '{"a" : {"b" : {"c" : "123456", "d" : "11.111"}}, "ss" : 199991111, "dcm" : 123.456, "dt" : "2021-01-01 00:00:00", "ip" : "127.0.0.1"}')"""
    sql """insert into test_predefine values(2, '2', '{"a" : {"b" : {"c" : 678910, "d" : 22.222}}, "ss" : "29999111", "dcm" : "456.123", "dt" : "2022-01-01 11:11:11", "ip" : "127.0.0.1"}')"""
    sql """insert into test_predefine values(3, '3', '{"dcm" : 789.123, "dt" : "2025-01-01 11:11:11.1", "ip" : "127.0.0.1"}')"""
    sql """insert into test_predefine values(4, '4', '{"a" : {"b" : {"c" : "678910", "d" : "33.222"}}}')"""
    sql """insert into test_predefine values(5, '5', 'null')"""
    sql """insert into test_predefine values(6, '6', null)"""
    sql """insert into test_predefine values(7, '7', '{"xxx" : 12345}')"""
    sql """insert into test_predefine values(8, '8', '{"yyy" : 111.111}')"""
    sql """insert into test_predefine values(9, '2', '{"a" : {"b" : {"c" : 678910, "d" : 22.222}}, "ss" : "29999111", "dcm" : "456.123", "dt" : "2022-01-01 11:11:11", "ip" : "127.0.0.1"}')"""
    sql """insert into test_predefine values(10, '1', '{"a" : {"b" : {"c" : "123456", "d" : "11.111"}}, "ss" : 199991111, "dcm" : 123.456, "dt" : "2021-01-01 00:00:00", "ip" : "127.0.0.1"}')"""
    sql """insert into test_predefine values(12, '3', '{"dcm" : 789.123, "dt" : "2025-01-01 11:11:11.1", "ip" : "127.0.0.1"}')"""
    sql """insert into test_predefine values(11, '4', '{"a" : {"b" : {"c" : "678910", "d" : "33.222"}}}')"""
    qt_sql """select * from test_predefine order by id"""
    sql """set describe_extend_variant_column = true"""
    qt_sql "desc test_predefine"

    qt_sql """select cast(v1['ip'] as ipv4) from test_predefine where cast(v1['ip'] as ipv4) = '127.0.0.1';"""
    qt_sql """select cast(v1['dcm'] as decimal) from test_predefine where cast(v1['dcm'] as decimal) = '123.456';"""
    qt_sql """select v1['dcm'] from test_predefine order by id;"""
    qt_sql """select v1['dt'] from test_predefine where cast(v1['dt'] as datetime) = '2022-01-01 11:11:11';"""
    qt_sql """select v1['dt'] from test_predefine  where  cast(v1['dt'] as datetime) = '2022-01-01 11:11:11' order by id limit 10"""
    qt_sql """select * from test_predefine  where  cast(v1['dt'] as datetime) = '2022-01-01 11:11:11' order by id limit 10;"""
    qt_sql """select * from test_predefine  where  v1['dt'] is not null order by id limit 10;"""

    sql """DROP TABLE IF EXISTS test_predefine1"""
    sql """
        CREATE TABLE `test_predefine1` (
            `id` bigint NOT NULL,
            `v1` variant NULL,
            INDEX idx_var_sub(`v1`) USING INVERTED PROPERTIES("parser" = "english", "sub_column_path" = "a.b.c") )
        ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1");
    """ 
    sql """insert into test_predefine1 values(1, '{"predefine_col1" : 1024}')"""
    sql """insert into test_predefine1 values(2, '{"predefine_col2" : 1.11111}')"""
    sql """insert into test_predefine1 values(3, '{"predefine_col3" : "11111.00000"}')"""
    sql """insert into test_predefine1 values(4, '{"predefine_col4" : "2020-01-01-01"}')"""

    sql """insert into test_predefine1 values(5, '{"PREDEFINE_COL1" : 1024}')"""
    sql """insert into test_predefine1 values(6, '{"PREDEFINE_COL2" : 1.11111}')"""
    sql """insert into test_predefine1 values(7, '{"PREDEFINE_COL3" : "11111.00000"}')"""
    sql """insert into test_predefine1 values(8, '{"PREDEFINE_COL4" : "2020-01-01-01"}')"""
    sql """select * from test_predefine1 order by id limit 1"""
    qt_sql """desc test_predefine1"""
    qt_sql """select * from test_predefine1 order by id"""


    // complex types with scalar types
    sql "DROP TABLE IF EXISTS test_predefine2"
    sql """
        CREATE TABLE `test_predefine2` (
            `id` bigint NOT NULL,
            `v1` variant<
                array_int:array<int>,
                array_string:array<string>,
                array_decimal:array<decimalv3(27,9)>,
                array_datetime:array<datetime>,
                array_datetimev2:array<datetimev2>,
                array_date:array<date>,
                array_datev2:array<datev2>,
                array_ipv4:array<ipv4>,
                array_ipv6:array<ipv6>,
                array_float:array<float>,
                array_boolean:array<boolean>,
                int_:int, 
                string_:string, 
                decimal_:decimalv3(27,9), 
                datetime_:datetime,
                datetimev2_:datetimev2(6),
                date_:date,
                datev2_:datev2,
                ipv4_:ipv4,
                ipv6_:ipv6,
                float_:float,
                boolean_:boolean,
                varchar_:varchar
            > NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1");
    """
    def json1 = """
        {
                "array_int" : [1, 2, 3],
                "array_string" : ["a", "b", "c"],
                "array_decimal" : [1.1, 2.2, 3.3],
                "array_datetime" : ["2021-01-01 00:00:00", "2022-01-01 00:00:00", "2023-01-01 00:00:00"],
                "array_datetimev2" : ["2021-01-01 00:00:00", "2022-01-01 00:00:00", "2023-01-01 00:00:00"],
                "array_date" : ["2021-01-01", "2022-01-01", "2023-01-01"],
                "array_datev2" : ["2021-01-01", "2022-01-01", "2023-01-01"],
                "array_ipv4" : ["127.0.0.1", "172.0.1.1"],
                "array_ipv6" : ["ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe"],
                "array_float" : ["1.11111"],
                "array_boolean" : [true, false, true],
                "int_" : 11111122,
                "string_" : 12111222113.0,
                "decimal_" : 188118222.011121933,
                "datetime_" : "2022-01-01 11:11:11",
                "datetimev2_" : "2022-01-01 11:11:11.999999",
                "date_" : "2022-01-01",
                "datev2_" : "2022-01-01",
                "ipv4_" : "127.0.0.1",
                "ipv6_" : "ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe",
                "float_" : "128.111",
                "boolean_" : true,
                "varchar_" : "hello world"
            }
    """
    def json2 = """
        {
                "array_int" : ["1", "2", 3],
                "array_string" : ["a", "b", "c"],
                "array_decimal" : [1.1, 2.2, 3.3],
                "array_datetime" : ["2021-01-01 00:00:00", "2022-01-01 00:00:00", "2023-01-01 00:00:00"],
                "array_datetimev2" : ["2021-01-01 00:00:00", "2022-01-01 00:00:00", "2023-01-01 00:00:00"],
                "array_date" : ["2021-01-01", "2022-01-01", "2023-01-01"],
                "array_datev2" : ["2021-01-01", "2022-01-01", "2023-01-01"],
                "array_ipv4" : ["127.0.0.1", "172.0.1.1"],
                "array_ipv6" : ["ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe"],
                "array_float" : [2.22222],
                "array_boolean" : [1, 0, 1, 0, 1],
                "int_" : "3333333333",
                "string_" : 12111222113.0,
                "decimal_" : "219911111111.011121933",
                "datetime_" : "2022-01-01 11:11:11",
                "datetimev2_" : "2022-01-01 11:11:11.999999",
                "date_" : "2022-01-01",
                "datev2_" : "2022-01-01",
                "ipv4_" : "127.0.0.1",
                "ipv6_" : "ffff:ffff:ffff:ffff:ffff:ffff:ffff:fffe",
                "float_" : 1.111111111,
                "boolean_" : true,
                "varchar_" : "world hello"
            }
    """
    def json3 = """
        {
                "array_int" : ["1", "2", 3],
                "array_string" : ["a", "b", "c"],
                "array_datetimev2" : ["2021-01-01 00:00:00", "2022-01-01 00:00:00", "2023-01-01 00:00:00"],
                "int_" : "3333333333",
                "decimal_" : "219911111111.011121933",
                "date_" : "2022-01-01",
                "ipv4_" : "127.0.0.1",
                "float_" : 1.111111111,
                "boolean_" : true,
                "varchar_" : "world hello"
            }
    """
    def json4 = """
        {
                "array_int" : ["1", "2", 3],
                "array_string" : ["a", "b", "c"],
                "array_datetimev2" : ["2021-01-01 00:00:00", "2022-01-01 00:00:00", "2023-01-01 00:00:00"],
                "ipv4_" : "127.0.0.1",
                "float_" : 1.111111111,
                "varchar_" : "world hello",
                "ext_1" : 1.111111,
                "ext_2" : "this is an extra field",
                "ext_3" : [1, 2, 3]
            }
    """
    sql "insert into test_predefine2 values(1, '${json1}')"
    sql "insert into test_predefine2 values(2, '${json2}')"
    sql "insert into test_predefine2 values(3, '${json3}')"
    sql "insert into test_predefine2 values(4, '${json4}')"
       
    qt_sql """select * from test_predefine2 order by id"""

    for (int i = 10; i < 100; i++) {
        sql "insert into test_predefine2 values(${i}, '${json4}')"
    } 

    // // schema change
    // // 1. add column
    sql "alter table test_predefine1 add column v2 variant<dcm:decimal,dt:datetime> default null"
    sql """insert into test_predefine1 values(101, '{"a" :1}', '{"dcm": 1111111}')""" 
    sql "alter table test_predefine1 add column v3 variant default null"
    sql """insert into test_predefine1 values(102, '{"a" :1}', '{"dcm": 1111111}', '{"dcm": 1111111}');"""
    // 2. alter column type
    sql "alter table test_predefine1 modify column v3 variant<dcm:decimal,dt:datetime,ip:ipv6>"
    sql """insert into test_predefine1 values(103, '{"a" :1}', '{"dcm": 1111111}', '{"dt": "2021-01-01 11:11:11"}');"""
    qt_sql """select * from test_predefine1 where id >= 100 order by id"""
    // 3. drop column
    qt_sql "desc test_predefine1"
    sql "alter table test_predefine1 drop column v3"

    sql """insert into test_predefine1 select id, v1, v1 from test_predefine2"""
}