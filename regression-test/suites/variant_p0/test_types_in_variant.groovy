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

suite("regression_test_variant_types", "var_view") {

    sql " set default_variant_enable_doc_mode = false "
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    sql """ set default_variant_max_sparse_column_statistics_size = 10 """
    sql """ set default_variant_sparse_hash_shard_count = 10 """
    def table_name = "test_variant_types"
    sql "drop table if exists ${table_name}"
    
    sql """
        create table ${table_name} (
            id int,
            var variant<properties("variant_max_subcolumns_count" = "0")>
        ) engine = olap
        duplicate key (id)
        distributed by hash(id) buckets 1
        properties ("replication_num" = "1")
    """

    sql """
        insert into ${table_name} (id, var) values (1, '{"a": 1, "b": 1.1, "c": "string", "d": true, "e": null, "f": 18446744073709551615}');
    """

    sql """
        select * from ${table_name};
    """

    sql """set describe_extend_variant_column = true"""

    qt_sql_scalar "desc ${table_name}"
    
    sql """ insert into ${table_name} (id, var) values (2, '{"g": [1, 2, 3], "h": [1.1, 2.2], "i": ["string", "string2"], "j": [true, false], "l": [18446744073709551615, 18446744073709551605]}'); """
    
    qt_sql "select * from ${table_name} order by id"

    qt_sql_array "desc ${table_name}"

    sql """ insert into ${table_name} (id, var) values (3, '{"m": [1, 18446744073709551605]}'); """

    qt_sql "select * from ${table_name} order by id"

    qt_sql_array_largeint "desc ${table_name}"

    sql """ insert into ${table_name} (id, var) values (4, '{"n": [2, "string", null, true, 1.1, 18446744073709551615]}'); """
    
    qt_sql "select * from ${table_name} order by id"

    qt_sql_array_json "desc ${table_name}"
    
    sql """ insert into ${table_name} (id, var) values (5, '{"o": [18446744073709551615, ["string", null]]}'); """
    
    qt_sql "select * from ${table_name} order by id"

    qt_sql_json "desc ${table_name}"

     sql "drop table if exists ${table_name}"

     sql """ set enable_variant_flatten_nested = true """
    
    sql """
        create table ${table_name} (
            id int,
            var variant<properties("variant_max_subcolumns_count" = "0")>
        ) engine = olap
        duplicate key (id)
        distributed by hash(id) buckets 1
        properties ("replication_num" = "1", "variant_enable_flatten_nested" = "true")
    """

    sql """ set enable_variant_flatten_nested = false """

    sql """ insert into ${table_name} (id, var) values (1, '{"a": [{"b" : 18446744073709551615}]}'); """

    qt_sql "select * from ${table_name} order by id"

    qt_sql_array_largeint "desc ${table_name}"
    
    sql """ insert into ${table_name} (id, var) values (2, '{"a": [{"b" : true}]}'); """

    qt_sql "select * from ${table_name} order by id"

    qt_sql_array_largeint "desc ${table_name}"

    sql """ insert into ${table_name} (id, var) values (3, '{"a": [{"b" : 1.1}]}'); """


    qt_sql "select * from ${table_name} order by id"

    qt_sql_array_json "desc ${table_name}"
}