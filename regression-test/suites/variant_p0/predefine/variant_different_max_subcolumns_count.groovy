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

suite("variant_different_max_subcolumns_count", "p0") {
    
    
    def table_name = "variant_different_max_subcolumns_count"
    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """ 
        CREATE TABLE IF NOT EXISTS ${table_name} (
            k bigint,
            v variant<'a' : int, 'b' : string, properties("variant_max_subcolumns_count" = "0", "variant_enable_typed_paths_to_sparse" = "false")>,
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true");
    """

    sql """INSERT INTO ${table_name} values(1, '{"a": "1", "b": "hello", "c": 1.1}'), (2, '{"c": 2.2}')"""
    sql """INSERT INTO ${table_name} values(3, '{"a": "3", "b": "world", "c": 3.3}')"""
    sql """INSERT INTO ${table_name} values(4, '{"b": "world", "c": 4.4}')"""
    sql """INSERT INTO ${table_name} values(5, '{"a": "5", "c": 5.5}')"""

    qt_sql "select v['a'], v['b'], v['c'], * from ${table_name} order by k"
    trigger_and_wait_compaction(table_name, "full")
    qt_sql "select v['a'], v['b'], v['c'], * from ${table_name} order by k"

    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            k bigint,
            v variant<'a' : int, 'b' : string, properties("variant_max_subcolumns_count" = "0", "variant_enable_typed_paths_to_sparse" = "false")>,
            v2 variant<'a' : int, 'b' : string, properties("variant_max_subcolumns_count" = "1", "variant_enable_typed_paths_to_sparse" = "false")>,
            v3 variant,
            v4 variant<'a' : int, 'b' : string, properties("variant_max_subcolumns_count" = "1", "variant_enable_typed_paths_to_sparse" = "true")>
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true");
    """

    sql """INSERT INTO ${table_name} values(1, '{"a": "1", "b": "hello", "c": 1.1}', '{"a": "1", "b": "hello", "c": 1.1}', '{"a": "1", "b": "hello", "c": 1.1}', '{"a": "1", "b": "hello", "c": 1.1}')"""
    sql """INSERT INTO ${table_name} values(2, '{"c": 2.2}', '{"c": 2.2}', '{"c": 2.2}', '{"c": 2.2}')"""
    sql """INSERT INTO ${table_name} values(3, '{"a": "3", "b": "world", "c": 3.3}', '{"a": "3", "b": "world", "c": 3.3}', '{"a": "3", "b": "world", "c": 3.3}', '{"a": "3", "b": "world", "c": 3.3}')"""
    sql """INSERT INTO ${table_name} values(4, '{"b": "world", "c": 4.4}', '{"b": "world", "c": 4.4}', '{"b": "world", "c": 4.4}', '{"b": "world", "c": 4.4}')"""
    sql """INSERT INTO ${table_name} values(5, '{"a": "5", "c": 5.5}', '{"a": "5", "c": 5.5}', '{"a": "5", "c": 5.5}', '{"a": "5", "c": 5.5}')"""
    sql """INSERT INTO ${table_name} values(6, '{"a" : "5", "b" : "world"}', '{"a" : "5", "b" : "world"}', '{"a" : "5", "b" : "world"}', '{"a" : "5", "b" : "world"}')"""
    sql """INSERT INTO ${table_name} values(7, '{"a" : "1"}', '{"a" : "1"}', '{"a" : "1"}', '{"a" : "1"}')"""
    sql """INSERT INTO ${table_name} values(8, '{"b" : "1"}', '{"b" : "1"}', '{"b" : "1"}', '{"b" : "1"}')"""

    qt_sql "select v['a'], v['b'], v['c'], v2['a'], v2['b'], v2['c'], v3['a'], v3['b'], v3['c'], v4['a'], v4['b'], v4['c'], * from ${table_name} order by k"

    trigger_and_wait_compaction(table_name, "full")
    qt_sql "select v['a'], v['b'], v['c'], v2['a'], v2['b'], v2['c'], v3['a'], v3['b'], v3['c'], v4['a'], v4['b'], v4['c'], * from ${table_name} order by k"
    
    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            k bigint,
            v variant<'a' : int, 'b' : string, properties("variant_max_subcolumns_count" = "0")>,
            v2 variant<'a' : int, 'b' : string, properties("variant_max_subcolumns_count" = "1")>,
            v3 variant<'a' : int, 'b' : string, properties("variant_max_subcolumns_count" = "3")>,
            v4 variant<'a' : int, 'b' : string, properties("variant_max_subcolumns_count" = "5")>,
            v5 variant<'a' : int, 'b' : string, properties("variant_max_subcolumns_count" = "7")>
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true");
    """
    sql """INSERT INTO ${table_name} values(1, '{"a": "1", "b": "hello", "c": 1.1}', '{"a": "1", "b": "hello", "c": 1.1}', '{"a": "1", "b": "hello", "c": 1.1}', '{"a": "1", "b": "hello", "c": 1.1}', '{"a": "1", "b": "hello", "c": 1.1}')"""
    sql """INSERT INTO ${table_name} values(2, '{"c": 2.2, "d": 2.2}', '{"c": 2.2, "d": 2.2}', '{"c": 2.2, "d": 2.2}', '{"c": 2.2, "d": 2.2}', '{"c": 2.2, "d": 2.2}')"""
    sql """INSERT INTO ${table_name} values(3, '{"e": "3", "f": "world", "g": 3.3, "h": 3.3}', '{"e": "3", "f": "world", "g": 3.3, "h": 3.3}', '{"e": "3", "f": "world", "g": 3.3, "h": 3.3}', '{"e": "3", "f": "world", "g": 3.3, "h": 3.3}', '{"e": "3", "f": "world", "g": 3.3, "h": 3.3}')"""
    sql """INSERT INTO ${table_name} values(1, '{"a": "1", "b": "hello", "c": 1.1}', '{"a": "1", "b": "hello", "c": 1.1}', '{"a": "1", "b": "hello", "c": 1.1}', '{"a": "1", "b": "hello", "c": 1.1}', '{"a": "1", "b": "hello", "c": 1.1}')"""
    sql """INSERT INTO ${table_name} values(2, '{"c": 2.2, "d": 2.2}', '{"c": 2.2, "d": 2.2}', '{"c": 2.2, "d": 2.2}', '{"c": 2.2, "d": 2.2}', '{"c": 2.2, "d": 2.2}')"""
    sql """INSERT INTO ${table_name} values(3, '{"e": "3", "f": "world", "g": 3.3, "h": 3.3}', '{"e": "3", "f": "world", "g": 3.3, "h": 3.3}', '{"e": "3", "f": "world", "g": 3.3, "h": 3.3}', '{"e": "3", "f": "world", "g": 3.3, "h": 3.3}', '{"e": "3", "f": "world", "g": 3.3, "h": 3.3}')"""

    qt_sql "select v['a'], v['b'], v['c'], v2['a'], v2['b'], v2['c'], v3['a'], v3['b'], v3['c'], v4['a'], v4['b'], v4['c'], v5['a'], v5['b'], v5['c'], * from ${table_name} order by k"

    trigger_and_wait_compaction(table_name, "full")
    qt_sql "select v['a'], v['b'], v['c'], v2['a'], v2['b'], v2['c'], v3['a'], v3['b'], v3['c'], v4['a'], v4['b'], v4['c'], v5['a'], v5['b'], v5['c'], * from ${table_name} order by k"

}