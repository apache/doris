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

suite("regression_test_variant_agg"){
    sql """DROP TABLE IF EXISTS var_agg"""
    sql """
        CREATE TABLE IF NOT EXISTS var_agg (
                k bigint,
                v variant replace,
                s bigint sum
            )
            AGGREGATE KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS 4
            properties("replication_num" = "1", "disable_auto_compaction" = "false");
    """
    sql """insert into var_agg values (1,  '[1]', 1),(1,  '{"a" : 1}', 1);"""
    sql """insert into var_agg values (2,  '[2]', 2),(1,  '{"a" : [[[1]]]}', 2);"""
    sql """insert into var_agg values (3,  '3', 3),(1,  '{"a" : 1}', 3), (1,  '{"a" : [1]}', 3);"""
    sql """insert into var_agg values (4,  '"4"', 4),(1,  '{"a" : "1223"}', 4);"""
    sql """insert into var_agg values (5,  '5', 5),(1,  '{"a" : [1]}', 5);"""
    sql """insert into var_agg values (6,  '"[6]"', 6),(1,  '{"a" : ["1", 2, 1.1]}', 6);"""
    sql """insert into var_agg values (7,  '7', 7),(1,  '{"a" : 1, "b" : {"c" : 1}}', 7);"""
    sql """insert into var_agg values (8,  '8.11111', 8),(1,  '{"a" : 1, "b" : {"c" : [{"a" : 1}]}}', 8);"""
    sql """insert into var_agg values (9,  '"9999"', 9),(1,  '{"a" : 1, "b" : {"c" : [{"a" : 1}]}}', 9);"""
    sql """insert into var_agg values (10,  '1000000', 10),(1,  '{"a" : 1, "b" : {"c" : [{"a" : 1}]}}', 10);"""
    sql """insert into var_agg values (11,  '[123.0]', 11),(1999,  '{"a" : 1, "b" : {"c" : 1}}', 11),(19921,  '{"a" : 1, "d" : 10}', 11);"""
    sql """insert into var_agg values (12,  '[123.2]', 12),(1022,  '{"a" : 1, "b" : {"f" : 17034, "g"  :1.111 }}', 12),(1029,  '{"a" : 1, "b" : {"c" : 1}}', 12);"""
    qt_sql1 "select k, cast(v['a'] as array<int>) from  var_agg where  size(cast(v['a'] as array<int>)) > 0 order by k, cast(v['a'] as string) asc"
    qt_sql2 "select k, cast(v as int), cast(v['b'] as string) from  var_agg where  length(cast(v['b'] as string)) > 4 order  by k, cast(v as string), cast(v['b'] as string) "
    qt_sql3 "select k, v from  var_agg order by k, cast(v as string) limit 5"
    qt_sql4 "select v['b'], v['b']['c'], cast(v as int) from  var_agg where cast(v['b'] as string) is not null and   cast(v['b'] as string) != '{}' order by k,cast(v as string) desc limit 10000;"
    qt_sql5 "select v['b'] from var_agg where cast(v['b'] as int) > 0;"
    qt_sql6 "select cast(v['b'] as string) from var_agg where cast(v['b'] as string) is not null and   cast(v['b'] as string) != '{}' order by k,  cast(v['b'] as string) "
    qt_sql7 "select * from var_agg where cast(v['b'] as string) is not null and   cast(v['b'] as string) != '{}' order by k,  cast(v['b'] as string) "
    qt_sql8 "select * from var_agg order by 1, cast(2 as string), 3"
    sql "alter table var_agg drop column s"
    sql """insert into var_agg select 5, '{"a" : 1234, "xxxx" : "fffff", "point" : 42000}'  as json_str
            union  all select 5, '{"a": 1123}' as json_str union all select *, '{"a": 11245, "x" : 42005}' as json_str from numbers("number" = "1024") limit 1024;"""
    sql """insert into var_agg select 5, '{"a" : 1234, "xxxx" : "fffff", "point" : 42000}'  as json_str
            union  all select 5, '{"a": 1123}' as json_str union all select *, '{"a": 11245, "y" : 11111111}' as json_str from numbers("number" = "2048") where number > 1024 limit 1024;"""
    sql """insert into var_agg select 5, '{"a" : 1234, "xxxx" : "fffff", "point" : 42000}'  as json_str
            union  all select 5, '{"a": 1123}' as json_str union all select *, '{"a": 11245, "c" : 1.11}' as json_str from numbers("number" = "1024") limit 1024;"""
    sql """insert into var_agg select 5, '{"a" : 1234, "xxxx" : "fffff", "point" : 42000}'  as json_str
            union  all select 5, '{"a": 1123}' as json_str union all select *, '{"a": 11245, "e" : [123456]}' as json_str from numbers("number" = "1024") limit 1024;"""
    sql """insert into var_agg select 5, '{"a" : 1234, "xxxx" : "fffff", "point" : 42000}'  as json_str
            union  all select 5, '{"a": 1123}' as json_str union all select *, '{"a": 11245, "f" : ["123456"]}' as json_str from numbers("number" = "1024") limit 1024;"""
    qt_sql9 "select * from var_agg order by cast(2 as string), 3, 1 limit 10"
    qt_sql9 "select * from var_agg where k > 1024 order by cast(2 as string), 3, 1 limit 10"
}