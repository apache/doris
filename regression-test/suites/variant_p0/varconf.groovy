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

suite("regression_test_variant_config", "variant_type"){
    def table_name = "variant_config"
    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 20
        properties(
            "variant.enable_decimal_type" = "true",
            "variant.ratio_of_defaults_as_sparse_column" = "0.5",
            "replication_num" = "1"
        );
    """
    sql "set describe_extend_variant_column = true"
    sql """insert into ${table_name} values (1, '{"a" : 1}')"""

    sql """ALTER TABLE ${table_name} set ("variant.enable_decimal_type" = "false")"""
    sql """ALTER TABLE ${table_name} set ("variant.ratio_of_defaults_as_sparse_column" = "0.95")"""
    sql """insert into  ${table_name} select 22222, '{"a": 11245, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.111}}'  as json_str
            union  all select 111111, '{"a": 1123}' as json_str union all select *, '{"a" : 1234, "xxxx" : "kaana"}' as json_str from numbers("number" = "4096") limit 4096 ;"""
    qt_sql "desc ${table_name}"

    qt_sql """select cast(v["a"] as int), cast(v["b"] as json) from ${table_name} order by cast(v["a"] as int) limit 10"""
    qt_sql """select cast(v["b"] as json) from ${table_name} where cast(v["b"] as json) is not null"""

    sql "truncate table ${table_name}"
    sql """ALTER TABLE ${table_name} set ("variant.ratio_of_defaults_as_sparse_column" = "0")"""
    sql """insert into  ${table_name} select 0, '{"a": 11245, "b" : [123, {"xx" : 1}], "c" : {"c" : 456, "d" : null, "e" : 7.111}}'  as json_str
            union  all select 0, '{"a": 1123}' as json_str union all select 0, '{"a" : 1234, "xxxx" : "kaana"}' as json_str from numbers("number" = "4096") limit 4096 ;"""
    qt_sql "desc ${table_name}"
}