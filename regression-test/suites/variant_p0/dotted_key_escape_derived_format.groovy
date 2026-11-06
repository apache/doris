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

suite("dotted_key_escape_derived_format", "p0,nonConcurrent") {
    sql "set default_variant_enable_nested_group=false"
    sql "DROP TABLE IF EXISTS dotted_key_escape_derived_source"
    sql """CREATE TABLE dotted_key_escape_derived_source(k INT, v VARIANT)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1", "variant_escape_dots_in_keys"="true")"""
    sql """INSERT INTO dotted_key_escape_derived_source VALUES
        (1,parse_to_variant('{"a.b":101,"a":{"b":102},"a%2Eb":103,"50%":104,"":105}'))"""
    sql "DROP TABLE IF EXISTS dotted_key_escape_derived_legacy"
    sql """CREATE TABLE dotted_key_escape_derived_legacy(k INT,v VARIANT)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES("replication_num"="1")"""
    sql """INSERT INTO dotted_key_escape_derived_legacy VALUES (0,parse_to_variant('{"a":{"b":11}}'))"""

    sql "DROP TABLE IF EXISTS dotted_key_escape_derived_cte"
    sql """CREATE TABLE dotted_key_escape_derived_cte DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1") AS WITH c AS (SELECT * FROM dotted_key_escape_derived_source)
        SELECT k,coalesce(v,v) v FROM c"""
    qt_cte """SELECT k,v,v['a.b'],v['a']['b'],v['a%2Eb'] FROM dotted_key_escape_derived_cte ORDER BY k"""
    sql "DROP VIEW IF EXISTS dotted_key_escape_derived_view"
    sql "CREATE VIEW dotted_key_escape_derived_view AS SELECT * FROM dotted_key_escape_derived_source"
    sql "DROP TABLE IF EXISTS dotted_key_escape_derived_view_copy"
    sql """CREATE TABLE dotted_key_escape_derived_view_copy DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1") AS SELECT * FROM dotted_key_escape_derived_view"""
    qt_view """SELECT k,v,v['a.b'],v['a']['b'] FROM dotted_key_escape_derived_view_copy ORDER BY k"""
    sql "DROP TABLE IF EXISTS dotted_key_escape_derived_subquery"
    sql """CREATE TABLE dotted_key_escape_derived_subquery DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1") AS SELECT CAST(1 AS INT) k,
        (SELECT v FROM dotted_key_escape_derived_source WHERE k=1) v"""
    qt_subquery """SELECT k,v,v['a.b'],v['a']['b'] FROM dotted_key_escape_derived_subquery ORDER BY k"""
    sql "DROP TABLE IF EXISTS dotted_key_escape_derived_recursive"
    sql """CREATE TABLE dotted_key_escape_derived_recursive DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1") AS WITH RECURSIVE r(k,v) AS (
        SELECT k,v FROM dotted_key_escape_derived_legacy UNION ALL
        SELECT s.k,s.v FROM r JOIN dotted_key_escape_derived_source s ON r.k=0)
        SELECT k,v FROM r"""
    qt_recursive """SELECT k,v,v['a.b'],v['a']['b'] FROM dotted_key_escape_derived_recursive ORDER BY k"""

    sql "DROP TABLE IF EXISTS dotted_key_escape_derived_false"
    sql """CREATE TABLE dotted_key_escape_derived_false DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1","variant_escape_dots_in_keys"="false")
        AS SELECT k,parse_to_variant('{"legacy.key":401}') v FROM dotted_key_escape_derived_source"""
    quickRunTest("false_property", "SHOW CREATE TABLE dotted_key_escape_derived_false", false,
            { row -> [row[1].contains('variant_escape_dots_in_keys').toString()] })
    qt_explicit_false "SELECT k,v FROM dotted_key_escape_derived_false ORDER BY k"
    sql "DROP TABLE IF EXISTS dotted_key_escape_derived_true"
    sql """CREATE TABLE dotted_key_escape_derived_true DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1","variant_escape_dots_in_keys"="true") AS
        SELECT CAST(1 AS INT) k,parse_to_variant('{"a.b":301,"a":{"b":302}}') v"""
    qt_explicit_true """SELECT k,v,v['a.b'],v['a']['b'] FROM dotted_key_escape_derived_true ORDER BY k"""
    sql "DROP TABLE IF EXISTS dotted_key_escape_derived_int"
    sql """CREATE TABLE dotted_key_escape_derived_int DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num"="1") AS SELECT k FROM dotted_key_escape_derived_source WHERE false"""
    sql "ALTER TABLE dotted_key_escape_derived_int ADD COLUMN v VARIANT"
    sql """INSERT INTO dotted_key_escape_derived_int VALUES
        (1,parse_to_variant('{"a.b":401,"a":{"b":402}}'))"""
    qt_int_then_add """SELECT k,v,v['a.b'],v['a']['b'] FROM dotted_key_escape_derived_int ORDER BY k"""
    quickRunTest("int_property", "SHOW CREATE TABLE dotted_key_escape_derived_int", false,
            { row -> [row[1].contains('variant_escape_dots_in_keys').toString()] })
}
