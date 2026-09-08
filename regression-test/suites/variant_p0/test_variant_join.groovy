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

suite("test_variant_join", "p0,nonConcurrent") {
    setFeConfigTemporary([enable_variant_v2: true]) {
        sql "DROP TABLE IF EXISTS variant_join_left"
        sql "DROP TABLE IF EXISTS variant_join_right"
        sql """CREATE TABLE variant_join_left (id INT, v VARIANT)
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
            PROPERTIES("replication_num"="1")"""
        sql """CREATE TABLE variant_join_right (id INT, v VARIANT)
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
            PROPERTIES("replication_num"="1")"""
        sql """INSERT INTO variant_join_left VALUES
            (1, parse_to_variant('1')), (2, parse_to_variant('1.0')),
            (3, parse_to_variant('"1"')), (4, parse_to_variant('false')),
            (5, NULL), (6, parse_to_variant('{"a":1,"b":[2,null]}')),
            (7, parse_to_variant('[1,2]')), (8, parse_to_variant('true')),
            (9, parse_to_variant('{"id":10}')), (10, parse_to_variant('{}'))"""
        sql """INSERT INTO variant_join_right VALUES
            (1, parse_to_variant('1.0')), (2, parse_to_variant('"1"')),
            (3, parse_to_variant('false')), (4, NULL),
            (5, parse_to_variant('{"b":[2.0,null],"a":1.0}')),
            (6, parse_to_variant('[2,1]')), (7, parse_to_variant('true')),
            (8, parse_to_variant('{"id":10.0}')), (9, parse_to_variant('[]'))"""

        order_qt_broadcast """SELECT l.id, r.id FROM variant_join_left l
            JOIN [broadcast] variant_join_right r ON l.v = r.v"""
        order_qt_shuffle """SELECT l.id, r.id FROM variant_join_left l
            JOIN [shuffle] variant_join_right r ON l.v = r.v"""
        order_qt_null_safe """SELECT l.id, r.id FROM variant_join_left l
            JOIN variant_join_right r ON l.v <=> r.v"""
        order_qt_composite_null_safe """SELECT l.id, r.id FROM variant_join_left l
            JOIN variant_join_right r ON l.v <=> r.v AND l.id % 2 = r.id % 2"""
        order_qt_left """SELECT l.id, r.id FROM variant_join_left l
            LEFT JOIN variant_join_right r ON l.v = r.v"""
        order_qt_full """SELECT l.id, r.id FROM variant_join_left l
            FULL JOIN variant_join_right r ON l.v = r.v"""
        order_qt_semi """SELECT l.id FROM variant_join_left l
            LEFT SEMI JOIN variant_join_right r ON l.v = r.v"""
        order_qt_anti """SELECT l.id FROM variant_join_left l
            LEFT ANTI JOIN variant_join_right r ON l.v = r.v"""
        order_qt_in """SELECT id FROM variant_join_left
            WHERE v IN (SELECT v FROM variant_join_right)"""
        order_qt_not_in """SELECT id FROM variant_join_left
            WHERE v NOT IN (SELECT v FROM variant_join_right)"""
        order_qt_mark """SELECT l.id, l.v IN (SELECT r.v FROM variant_join_right r)
            FROM variant_join_left l"""
        order_qt_exists """SELECT l.id, EXISTS(SELECT 1 FROM variant_join_right r WHERE l.v = r.v)
            FROM variant_join_left l"""
        order_qt_subpath """SELECT l.id, r.id FROM variant_join_left l
            JOIN variant_join_right r ON l.v['id'] = r.v['id']"""
        order_qt_residual """SELECT l.id, r.id FROM variant_join_left l
            JOIN variant_join_right r ON l.v = r.v OR l.id = r.id"""
        order_qt_scalar_equality """SELECT l.id, l.v = parse_to_variant('1.0'),
            l.v != parse_to_variant('1.0'), l.v <=> CAST(NULL AS VARIANT)
            FROM variant_join_left l"""
        qt_json_null_equality """SELECT parse_to_variant('null') = parse_to_variant('null'),
            parse_to_variant('null') = parse_to_variant('{}')"""
        sql "SET enable_spill = true"
        sql "SET enable_force_spill = true"
        sql "SET spill_min_revocable_mem = 1"
        order_qt_spill """SELECT l.id, r.id FROM variant_join_left l
            JOIN [shuffle] variant_join_right r ON l.v <=> r.v"""
        test {
            sql "SELECT * FROM variant_join_left l JOIN variant_join_right r ON l.v > r.v"
            exception "CAST to a concrete type first"
        }
    }
}
