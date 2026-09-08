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

suite("test_dict_complex_type") {
    sql "drop database if exists test_dict_complex_type"
    sql "create database test_dict_complex_type"
    sql "use test_dict_complex_type"

    // =========================================================
    // Part 1: Array as VALUE column
    // =========================================================
    sql """
        create table if not exists array_value_table(
            k0 int not null,
            v0 array<int> null,
            v1 array<varchar> not null
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS auto
        properties("replication_num" = "1");
    """
    sql """insert into array_value_table values(1, [1,2,3], ['a','b','c']);"""
    sql """insert into array_value_table values(2, null, ['x','y']);"""
    sql """insert into array_value_table values(3, [100], ['hello']);"""

    sql """
        create dictionary array_value_dict using array_value_table
        (
            k0 KEY,
            v0 VALUE,
            v1 VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """
    waitAllDictionariesReady()

    // query existing key — nullable array value
    qt_array_val1 """ select dict_get("test_dict_complex_type.array_value_dict", "v0", 1) """
    // key 2 has null v0
    qt_array_val2 """ select dict_get("test_dict_complex_type.array_value_dict", "v0", 2) """
    // key not found → null
    qt_array_val3 """ select dict_get("test_dict_complex_type.array_value_dict", "v0", 99) """
    // non-nullable array value
    qt_array_val4 """ select dict_get("test_dict_complex_type.array_value_dict", "v1", 1) """
    qt_array_val5 """ select dict_get("test_dict_complex_type.array_value_dict", "v1", 3) """
    // key not found → null
    qt_array_val6 """ select dict_get("test_dict_complex_type.array_value_dict", "v1", 99) """
    // null key → null
    qt_array_val7 """ select dict_get("test_dict_complex_type.array_value_dict", "v1", null) """

    // =========================================================
    // Part 2: Array as KEY column
    // =========================================================
    sql """
        create table if not exists array_key_table(
            id int not null,
            k0 array<varchar> not null,
            v0 varchar not null
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS auto
        properties("replication_num" = "1");
    """
    sql """insert into array_key_table values(1, ['a','b'], 'val_ab');"""
    sql """insert into array_key_table values(2, ['x','y','z'], 'val_xyz');"""
    sql """insert into array_key_table values(3, [], 'val_empty');"""

    sql """
        create dictionary array_key_dict using array_key_table
        (
            k0 KEY,
            v0 VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """
    waitAllDictionariesReady()

    // exact match
    qt_array_key1 """ select dict_get("test_dict_complex_type.array_key_dict", "v0", ['a','b']) """
    qt_array_key2 """ select dict_get("test_dict_complex_type.array_key_dict", "v0", ['x','y','z']) """
    qt_array_key3 """ select dict_get("test_dict_complex_type.array_key_dict", "v0", cast([] as array<varchar>)) """
    // key not found → null
    qt_array_key4 """ select dict_get("test_dict_complex_type.array_key_dict", "v0", ['notexist']) """
    // null key → null
    qt_array_key5 """ select dict_get("test_dict_complex_type.array_key_dict", "v0", null) """

    // =========================================================
    // Part 3: Array KEY + Array VALUE mixed
    // =========================================================
    sql """
        create table if not exists array_key_value_table(
            id int not null,
            k0 array<int> not null,
            v0 array<varchar> null
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS auto
        properties("replication_num" = "1");
    """
    sql """insert into array_key_value_table values(1, [1,2], ['one','two']);"""
    sql """insert into array_key_value_table values(2, [3], null);"""

    sql """
        create dictionary array_kv_dict using array_key_value_table
        (
            k0 KEY,
            v0 VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """
    waitAllDictionariesReady()

    qt_array_kv1 """ select dict_get("test_dict_complex_type.array_kv_dict", "v0", [1,2]) """
    // value is null
    qt_array_kv2 """ select dict_get("test_dict_complex_type.array_kv_dict", "v0", [3]) """
    // key not found → null
    qt_array_kv3 """ select dict_get("test_dict_complex_type.array_kv_dict", "v0", [99]) """

    // =========================================================
    // Part 4: Multi-key (int + array) dict
    // =========================================================
    sql """
        create table if not exists multi_key_array_table(
            id int not null,
            k0 int not null,
            k1 array<varchar> not null,
            v0 varchar not null
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS auto
        properties("replication_num" = "1");
    """
    sql """insert into multi_key_array_table values(1, 10, ['a'], 'ten_a');"""
    sql """insert into multi_key_array_table values(2, 20, ['b','c'], 'twenty_bc');"""

    sql """
        create dictionary multi_key_array_dict using multi_key_array_table
        (
            k0 KEY,
            k1 KEY,
            v0 VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """
    waitAllDictionariesReady()

    qt_multi_key1 """ select dict_get_many("test_dict_complex_type.multi_key_array_dict", ["v0"], struct(10, ['a'])) """
    qt_multi_key2 """ select dict_get_many("test_dict_complex_type.multi_key_array_dict", ["v0"], struct(20, ['b','c'])) """
    // key not found
    qt_multi_key3 """ select dict_get_many("test_dict_complex_type.multi_key_array_dict", ["v0"], struct(10, ['notexist'])) """

    // =========================================================
    // Part 5: Map as KEY column
    // =========================================================
    sql """
        create table if not exists map_key_table(
            id int not null,
            k0 map<int, varchar> not null,
            v0 varchar not null
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS auto
        properties("replication_num" = "1");
    """
    sql """insert into map_key_table values(1, {1:'x'}, 'val_1x');"""
    sql """insert into map_key_table values(2, {2:'y', 3:'z'}, 'val_23yz');"""
    sql """insert into map_key_table values(3, {}, 'val_empty');"""

    sql """
        create dictionary map_key_dict using map_key_table
        (
            k0 KEY,
            v0 VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """
    waitAllDictionariesReady()

    // exact match
    qt_map_key1 """ select dict_get("test_dict_complex_type.map_key_dict", "v0", map(1, 'x')) """
    qt_map_key2 """ select dict_get("test_dict_complex_type.map_key_dict", "v0", map(2, 'y', 3, 'z')) """
    qt_map_key3 """ select dict_get("test_dict_complex_type.map_key_dict", "v0", cast(map() as map<int, varchar>)) """
    // key not found → null
    qt_map_key4 """ select dict_get("test_dict_complex_type.map_key_dict", "v0", map(99, 'miss')) """
    // null key → null
    qt_map_key5 """ select dict_get("test_dict_complex_type.map_key_dict", "v0", null) """

    // =========================================================
    // Part 6: Struct as KEY column
    // =========================================================
    sql """
        create table if not exists struct_key_table(
            id int not null,
            k0 struct<f0:int, f1:varchar> not null,
            v0 varchar not null
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS auto
        properties("replication_num" = "1");
    """
    sql """insert into struct_key_table values(1, struct(1, 'a'), 'va');"""
    sql """insert into struct_key_table values(2, struct(2, 'b'), 'vb');"""
    sql """insert into struct_key_table values(3, struct(3, 'c'), 'vc');"""

    sql """
        create dictionary struct_key_dict using struct_key_table
        (
            k0 KEY,
            v0 VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """
    waitAllDictionariesReady()

    // exact match
    qt_struct_key1 """ select dict_get("test_dict_complex_type.struct_key_dict", "v0", named_struct('f0', 1, 'f1', 'a')) """
    qt_struct_key2 """ select dict_get("test_dict_complex_type.struct_key_dict", "v0", named_struct('f0', 2, 'f1', 'b')) """
    qt_struct_key3 """ select dict_get("test_dict_complex_type.struct_key_dict", "v0", named_struct('f0', 3, 'f1', 'c')) """
    // key not found → null
    qt_struct_key4 """ select dict_get("test_dict_complex_type.struct_key_dict", "v0", named_struct('f0', 99, 'f1', 'x')) """
    // null key → null
    qt_struct_key5 """ select dict_get("test_dict_complex_type.struct_key_dict", "v0", null) """
}
