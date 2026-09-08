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

suite("test_variant_relational_corners", "p0,nonConcurrent") {
    setFeConfigTemporary([enable_variant_v2: true]) {
        qt_constant_types """SELECT
            parse_to_variant('true') = parse_to_variant('1'),
            parse_to_variant('false') = parse_to_variant('0'),
            parse_to_variant('null') = parse_to_variant('{}'),
            parse_to_variant('{"k":{}}')['k'] IS NULL,
            parse_to_variant('9007199254740993') = parse_to_variant('9007199254740992.0')"""
        qt_decimal_types """SELECT
            CAST(CAST(1.50 AS DECIMAL(10,2)) AS VARIANT)
                = CAST(CAST(1.500 AS DECIMAL(10,3)) AS VARIANT),
            CAST(CAST(1.50 AS DECIMAL(10,2)) AS VARIANT) = parse_to_variant('1.5'),
            CAST(CAST(1.00 AS DECIMAL(10,2)) AS VARIANT) = parse_to_variant('1'),
            CAST(CAST(-1 AS TINYINT) AS VARIANT) = CAST(CAST(-1 AS BIGINT) AS VARIANT)"""
        sql "DROP TABLE IF EXISTS variant_relational_corners"
        sql """CREATE TABLE variant_relational_corners (id INT, v VARIANT)
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
            PROPERTIES("replication_num"="1")"""
        // Keep values inside objects so extraction exercises shredded subcolumns,
        // mixed-type promotion and the missing-path validity map.
        sql """INSERT INTO variant_relational_corners VALUES
            (1, parse_to_variant('{"k":1}')),
            (2, parse_to_variant('{"k":1.0}')),
            (3, parse_to_variant('{"k":"1"}')),
            (4, parse_to_variant('{"k":true}')),
            (5, parse_to_variant('{"k":0}')),
            (6, parse_to_variant('{"k":-0.0}')),
            (7, parse_to_variant('{"k":false}')),
            (8, parse_to_variant('{"k":null}')),
            (9, parse_to_variant('{}')),
            (10, NULL),
            (11, parse_to_variant('{"k":[1,2]}')),
            (12, parse_to_variant('{"k":[1.0,2.0]}')),
            (13, parse_to_variant('{"k":[2,1]}')),
            (14, parse_to_variant('{"k":{"a":1,"b":2}}')),
            (15, parse_to_variant('{"k":{"b":2.0,"a":1.0}}')),
            (16, parse_to_variant('{"k":[]}')),
            (17, parse_to_variant('{"k":{}}')),
            (18, parse_to_variant('{"k":9007199254740992}')),
            (19, parse_to_variant('{"k":9007199254740993}')),
            (20, parse_to_variant('{"k":9007199254740992.0}')),
            (21, parse_to_variant('{"k":1.5}')),
            (22, parse_to_variant('{"k":""}'))"""

        qt_extracted """SELECT id, v['k'], v['k'] IS NULL
            FROM variant_relational_corners ORDER BY id"""
        qt_groups """SELECT min(id), count(*) FROM variant_relational_corners
            GROUP BY v['k'] ORDER BY min(id)"""
        qt_distinct "SELECT count(DISTINCT v['k']) FROM variant_relational_corners"
        qt_native_order """SELECT id FROM variant_relational_corners
            ORDER BY v['k'] NULLS FIRST, id"""
        qt_native_order_desc """SELECT id FROM variant_relational_corners
            ORDER BY v['k'] DESC NULLS LAST, id LIMIT 10"""
        // Compare stored values to constants explicitly: generated output is an
        // observation of round-trip behavior, not a definition of type coercion.
        qt_roundtrip_types """SELECT id, v['k'] = parse_to_variant('true'),
            v['k'] = parse_to_variant('1'), v['k'] = parse_to_variant('{}')
            FROM variant_relational_corners WHERE id IN (1,2,3,4,8,9,17)
            ORDER BY id"""
        ["broadcast", "shuffle"].each { distribution ->
            qt_sql """SELECT l.id, r.id FROM variant_relational_corners l
                JOIN [${distribution}] variant_relational_corners r ON l.v['k'] = r.v['k']
                ORDER BY l.id, r.id"""
            qt_sql """SELECT l.id, r.id FROM variant_relational_corners l
                JOIN [${distribution}] variant_relational_corners r ON l.v['k'] <=> r.v['k']
                ORDER BY l.id, r.id"""
        }
        // CAST is intentionally a different oracle here: strings can merge with
        // numbers and DOUBLE cannot distinguish adjacent integers above 2^53.
        qt_cast_precision """SELECT l.id, r.id FROM variant_relational_corners l
            JOIN variant_relational_corners r
              ON CAST(l.v['k'] AS DOUBLE) = CAST(r.v['k'] AS DOUBLE)
            WHERE l.id BETWEEN 18 AND 20 AND r.id BETWEEN 18 AND 20
            ORDER BY l.id, r.id"""
        qt_cast_groups """SELECT min(id), count(*) FROM variant_relational_corners
            WHERE id IN (1,2,3) GROUP BY CAST(v['k'] AS BIGINT) ORDER BY min(id)"""
        qt_cast_order """SELECT id, CAST(v['k'] AS DOUBLE)
            FROM variant_relational_corners WHERE id IN (1,2,5,6,18,19,20,21)
            ORDER BY CAST(v['k'] AS DOUBLE), id"""
        sql "DROP TABLE IF EXISTS variant_relational_numeric"
        sql """CREATE TABLE variant_relational_numeric (id INT, v VARIANT)
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num"="1")"""
        sql """INSERT INTO variant_relational_numeric VALUES
            (1, parse_to_variant('{"k":9007199254740992}')),
            (2, parse_to_variant('{"k":9007199254740993}')),
            (3, parse_to_variant('{"k":9007199254740992.0}')),
            (4, parse_to_variant('{"k":1}')),
            (5, parse_to_variant('{"k":true}')),
            (6, parse_to_variant('{"k":false}')),
            (7, parse_to_variant('{"k":0}'))"""
        qt_numeric_promotion """SELECT id, v['k'] FROM variant_relational_numeric ORDER BY id"""
        qt_numeric_groups """SELECT min(id), count(*) FROM variant_relational_numeric
            GROUP BY v['k'] ORDER BY min(id)"""
        qt_numeric_join """SELECT l.id, r.id FROM variant_relational_numeric l
            JOIN variant_relational_numeric r ON l.v['k'] = r.v['k'] ORDER BY l.id,r.id"""
        sql "SET enable_spill = true"
        sql "SET enable_force_spill = true"
        sql "SET spill_min_revocable_mem = 1"
        qt_spill_groups """SELECT min(id), count(*) FROM variant_relational_corners
            GROUP BY v['k'] ORDER BY min(id)"""
        qt_spill_join """SELECT l.id, r.id FROM variant_relational_corners l
            JOIN [shuffle] variant_relational_corners r ON l.v['k'] <=> r.v['k']
            ORDER BY l.id, r.id"""
    }
}
