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
}