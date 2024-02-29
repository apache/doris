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

    sql """insert into ${table_name}_str select * from ${table_name}_var"""
    sql """insert into ${table_name}_var select * from ${table_name}_str"""
    sql """insert into ${table_name}_var select * from ${table_name}_var"""
    qt_sql """select v["a"], v["b"], v["c"] from  ${table_name}_var order by k"""
    qt_sql "select v from  ${table_name}_str order by k"
}