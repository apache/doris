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

suite("test_variant_join_type_properties", "p0,nonConcurrent") {
    // Variant join keys compare canonically. Keys whose VARIANT types differ only in
    // properties (max subcolumns count, doc mode) must join like identical types.
    sql "DROP TABLE IF EXISTS variant_join_props_a"
    sql "DROP TABLE IF EXISTS variant_join_props_b"
    sql "DROP TABLE IF EXISTS variant_join_props_c"
    sql "DROP TABLE IF EXISTS variant_join_props_d"
    sql """CREATE TABLE variant_join_props_a (
            id INT, v VARIANT<PROPERTIES("variant_max_subcolumns_count" = "10")>, s STRING)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num" = "1")"""
    sql """CREATE TABLE variant_join_props_b (
            id INT, v VARIANT<PROPERTIES("variant_max_subcolumns_count" = "20")>)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num" = "1")"""
    sql """CREATE TABLE variant_join_props_c (
            id INT, v VARIANT<PROPERTIES("variant_enable_doc_mode" = "true")>)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num" = "1")"""
    sql """CREATE TABLE variant_join_props_d (
            id INT,
            v VARIANT<PROPERTIES("variant_max_subcolumns_count" = "10")>,
            w VARIANT<PROPERTIES("variant_max_subcolumns_count" = "20")>)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")"""
    sql """INSERT INTO variant_join_props_a VALUES
        (1, parse_to_variant('1'), '1.0'), (2, parse_to_variant('"x"'), '"x"'),
        (3, NULL, NULL), (4, parse_to_variant('{"k":1,"j":[1,2]}'), '{"j":[1.0,2],"k":1.0}'),
        (5, parse_to_variant('true'), '1')"""
    sql """INSERT INTO variant_join_props_b VALUES
        (1, parse_to_variant('1.0')), (2, parse_to_variant('"x"')), (3, NULL),
        (4, parse_to_variant('{"j":[1,2.0],"k":1}')), (6, parse_to_variant('false'))"""
    sql """INSERT INTO variant_join_props_c VALUES
        (1, parse_to_variant('1')), (4, parse_to_variant('{"k":1.0,"j":[1,2]}'))"""
    sql """INSERT INTO variant_join_props_d VALUES
        (1, parse_to_variant('1'), parse_to_variant('1.0')),
        (2, parse_to_variant('"x"'), parse_to_variant('"x"')),
        (4, parse_to_variant('{"k":1}'), parse_to_variant('{"k":2}'))"""

    // Different max subcolumns counts.
    order_qt_broadcast """SELECT a.id, b.id FROM variant_join_props_a a
        JOIN [broadcast] variant_join_props_b b ON a.v = b.v"""
    order_qt_shuffle """SELECT a.id, b.id FROM variant_join_props_a a
        JOIN [shuffle] variant_join_props_b b ON a.v = b.v"""
    order_qt_null_safe """SELECT a.id, b.id FROM variant_join_props_a a
        JOIN variant_join_props_b b ON a.v <=> b.v"""
    order_qt_subpath """SELECT a.id, b.id FROM variant_join_props_a a
        JOIN variant_join_props_b b ON a.v['j'] = b.v['j']"""
    order_qt_left_semi """SELECT a.id FROM variant_join_props_a a
        LEFT SEMI JOIN variant_join_props_b b ON a.v = b.v"""
    order_qt_left_anti """SELECT a.id FROM variant_join_props_a a
        LEFT ANTI JOIN variant_join_props_b b ON a.v = b.v"""
    order_qt_mark """SELECT a.id, a.v IN (SELECT b.v FROM variant_join_props_b b)
        FROM variant_join_props_a a"""
    order_qt_not_in """SELECT a.id FROM variant_join_props_a a
        WHERE a.v NOT IN (SELECT b.v FROM variant_join_props_b b WHERE b.v IS NOT NULL)"""
    // Different doc mode.
    order_qt_doc_mode """SELECT a.id, c.id FROM variant_join_props_a a
        JOIN variant_join_props_c c ON a.v = c.v"""
    // A stored column against a computed Variant whose type has default properties.
    order_qt_computed_key """SELECT l.id, r.id FROM variant_join_props_a l
        JOIN variant_join_props_a r ON l.v = parse_to_variant(r.s)"""
    // The optimizer infers a.v = b.v from d.v = a.v, d.w = b.v and d.v = d.w and uses it
    // as a hash key between tables whose Variant properties differ.
    sql "SET disable_join_reorder = true"
    order_qt_inferred_key """SELECT a.id, b.id FROM variant_join_props_a a
        JOIN variant_join_props_b b ON a.id = b.id
        JOIN variant_join_props_d d ON d.v = a.v AND d.w = b.v
        WHERE d.v = d.w"""
}
