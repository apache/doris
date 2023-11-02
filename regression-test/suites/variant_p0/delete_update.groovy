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

suite("regression_test_variant_delete_and_update", "variant_type"){
    // MOR
    def table_name = "var_delete_update"
    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            k bigint,
            v variant
        )
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 3
        properties("replication_num" = "1");
    """

    sql """insert into ${table_name} values (1, '{"a" : 1, "b" : [1], "c": 1.0}')"""
    sql """insert into ${table_name} values (2, '{"a" : 2, "b" : [1], "c": 2.0}')"""
    sql """insert into ${table_name} values (3, '{"a" : 3, "b" : [3], "c": 3.0}')"""
    sql """insert into ${table_name} values (4, '{"a" : 4, "b" : [4], "c": 4.0}')"""
    sql """insert into ${table_name} values (5, '{"a" : 5, "b" : [5], "c": 5.0}')"""

    sql "delete from ${table_name} where k = 1"
    sql """update ${table_name} set v = '{"updated_value" : 123}' where k = 2"""
    qt_sql "select * from ${table_name} order by k"

    // MOW
    table_name = "var_delete_update_mow"
    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            k bigint,
            v variant,
            vs string 
        )
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 3
        properties("replication_num" = "1", "enable_unique_key_merge_on_write" = "true");
    """
    sql "insert into var_delete_update_mow select k, cast(v as string), cast(v as string) from var_delete_update"
    sql "delete from ${table_name} where k = 1"
    sql "delete from ${table_name} where k in (select k from variant_mow where k in (3, 4, 5))"
    // FIXME
    sql """update ${table_name} set vs = '{"updated_value" : 123}' where k = 2"""
    sql """update ${table_name} set v = '{"updated_value" : 123}' where k = 2"""
    qt_sql "select * from ${table_name} order by k"
}