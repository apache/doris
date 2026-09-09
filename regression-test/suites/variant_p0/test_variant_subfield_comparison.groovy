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

suite("test_variant_subfield_comparison", "p0,nonConcurrent") {
    setFeConfigTemporary([enable_variant_v2: true]) {
        // Keep the JSON text as well as its stored Variant representation: reading a
        // subcolumn can promote types, while parsing the text preserves each input.
        sql "DROP TABLE IF EXISTS variant_subfield_comparison"
        sql """CREATE TABLE variant_subfield_comparison (id INT, doc STRING)
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num"="1")"""
        sql """INSERT INTO variant_subfield_comparison (id, doc) VALUES
            (1, '{"a":1,"b":1}'),
            (2, '{"a":1,"b":2}'),
            (3, '{"a":-1,"b":-1}'),
            (4, '{"a":127,"b":128}'),
            (5, '{"a":1,"b":1.0}'),
            (6, '{"a":1,"b":1.5}'),
            (7, '{"a":1.5,"b":1.5}'),
            (8, '{"a":0,"b":-0.0}'),
            (9, '{"a":9007199254740993,"b":9007199254740992.0}'),
            (10, '{"a":9007199254740992,"b":9007199254740992.0}'),
            (11, '{"a":"1","b":1}'),
            (12, '{"a":"1.5","b":1.5}'),
            (13, '{"a":"abc","b":"abc"}'),
            (14, '{"a":"abc","b":"abd"}'),
            (15, '{"a":"","b":""}'),
            (16, '{"a":true,"b":true}'),
            (17, '{"a":true,"b":false}'),
            (18, '{"a":true,"b":1}'),
            (19, '{"a":false,"b":0}'),
            (20, '{"a":null,"b":null}'),
            (21, '{"a":null,"b":1}'),
            (22, '{"a":1,"b":null}'),
            (23, '{"a":1}'),
            (24, '{"b":1}'),
            (25, '{}'),
            (26, '{"a":null}'),
            (27, NULL),
            (28, '{"a":[1,2],"b":[1,2]}'),
            (29, '{"a":[1,2],"b":[1.0,2.0]}'),
            (30, '{"a":[1,2],"b":[2,1]}'),
            (31, '{"a":[],"b":[]}'),
            (32, '{"a":[1],"b":1}'),
            (33, '{"a":[1,null,"x",[2]],"b":[1.0,null,"x",[2.0]]}'),
            (34, '{"a":[null],"b":[]}'),
            (35, '{"a":{"x":1,"y":2},"b":{"y":2.0,"x":1.0}}'),
            (36, '{"a":{},"b":{}}'),
            (37, '{"a":{},"b":[]}'),
            (38, '{"a":{"x":null},"b":{}}'),
            (39, '{"a":[1],"b":["1"]}'),
            (40, '{"a":[{"x":1}],"b":[{"x":2}]}')"""
        // Insert into a second table to retain both original text and parsed values.
        sql "DROP TABLE IF EXISTS variant_subfield_values"
        sql """CREATE TABLE variant_subfield_values (id INT, doc STRING, v VARIANT)
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num"="1")"""
        sql """INSERT INTO variant_subfield_values
            SELECT id, doc, parse_to_variant(doc) FROM variant_subfield_comparison"""

        // These are separate round-trip expectations, not an assertion that storage
        // preserves JSON null and empty objects. Rows 20-27 and 36-38 expose the
        // current difference between stored-path SQL NULL and parsed JSON values.
        ["v", "parse_to_variant(doc)"].each { expression ->
            qt_sql """SELECT id, x['a'], x['b'],
                x['a'] = x['b'], x['a'] != x['b'], x['a'] <=> x['b'],
                x['b'] = x['a'], x['b'] != x['a'], x['b'] <=> x['a'],
                x['a'] IS NULL, x['b'] IS NULL
                FROM (SELECT id, ${expression} x FROM variant_subfield_values) t ORDER BY id"""
            ["=", "!=", "<=>"].each { op ->
                qt_sql """SELECT id FROM variant_subfield_values
                    WHERE ${expression}['a'] ${op} ${expression}['b'] ORDER BY id"""
            }
            qt_sql """SELECT id FROM variant_subfield_values
                WHERE NOT (${expression}['a'] = ${expression}['b']) ORDER BY id"""
            qt_sql """SELECT id FROM variant_subfield_values
                WHERE ${expression}['a'] = ${expression}['b']
                    OR ${expression}['a'] IS NULL ORDER BY id"""
        }
        // TopN ordering is supported; relational ordering predicates still require CAST.
        ["v", "parse_to_variant(doc)"].each { expression ->
            [">", ">=", "<", "<="].each { op ->
                test {
                    sql "SELECT ${expression}['a'] ${op} ${expression}['b'] FROM variant_subfield_values"
                    exception "CAST to a concrete type first"
                }
                test {
                    sql """SELECT id FROM variant_subfield_values
                        WHERE ${expression}['a'] ${op} ${expression}['b']"""
                    exception "CAST to a concrete type first"
                }
            }
        }
        // Isolate the known storage round-trip differences from ordinary equality cases.
        qt_null_object_roundtrip """SELECT id,
            v['a'] IS NULL, parse_to_variant(doc)['a'] IS NULL,
            v['b'] IS NULL, parse_to_variant(doc)['b'] IS NULL,
            v['a'] <=> v['b'],
            parse_to_variant(doc)['a'] <=> parse_to_variant(doc)['b']
            FROM variant_subfield_values WHERE id IN (20,21,26,36,37,38) ORDER BY id"""

        // JSON text cannot retain Decimal, date or IP physical types. Cast scalar
        // columns to Variant directly to exercise these same-row comparison types.
        sql "DROP TABLE IF EXISTS variant_comparison_scalar_sources"
        sql """CREATE TABLE variant_comparison_scalar_sources
            (id INT, a STRING, b STRING)
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num"="1")"""
        [
            ["DECIMAL(18,2)", "DECIMAL(38,3)", "1.50", "1.500", "1.501"],
            ["DECIMAL(18,2)", "DOUBLE", "1.50", "1.5", "1.6"],
            ["DECIMAL(18,2)", "BIGINT", "1.00", "1", "2"],
            ["DATEV2", "DATEV2", "2024-02-29", "2024-02-29", "2024-03-01"],
            ["DATETIMEV2(3)", "DATETIMEV2(6)", "2024-02-29 12:34:56.123",
                "2024-02-29 12:34:56.123000", "2024-02-29 12:34:56.123001"],
            ["IPV4", "IPV4", "10.0.0.2", "10.0.0.2", "10.0.0.10"],
            ["IPV6", "IPV6", "2001:db8::1", "2001:0db8:0:0:0:0:0:1", "2001:db8::2"],
            ["DOUBLE", "DOUBLE", "NaN", "NaN", "Infinity"],
            ["DOUBLE", "DOUBLE", "Infinity", "Infinity", "-Infinity"]
        ].each { spec ->
            sql "TRUNCATE TABLE variant_comparison_scalar_sources"
            sql """INSERT INTO variant_comparison_scalar_sources VALUES
                (1, '${spec[2]}', '${spec[3]}'), (2, '${spec[2]}', '${spec[4]}'),
                (3, NULL, '${spec[3]}'), (4, '${spec[2]}', NULL), (5, NULL, NULL)"""
            def source = """SELECT id, CAST(CAST(a AS ${spec[0]}) AS VARIANT) a,
                CAST(CAST(b AS ${spec[1]}) AS VARIANT) b
                FROM variant_comparison_scalar_sources"""
            qt_sql """SELECT id, a IS NULL, b IS NULL,
                a = b, a != b, a <=> b, b = a, b != a, b <=> a
                FROM (${source}) t ORDER BY id"""
            ["=", "!=", "<=>"].each { op ->
                qt_sql "SELECT id FROM (${source}) t WHERE a ${op} b ORDER BY id"
            }
        }
        qt_explicit_numeric_cast """SELECT id,
            CAST(v['a'] AS DOUBLE) = CAST(v['b'] AS DOUBLE),
            CAST(v['a'] AS DOUBLE) > CAST(v['b'] AS DOUBLE),
            CAST(v['a'] AS DOUBLE) < CAST(v['b'] AS DOUBLE)
            FROM variant_subfield_values WHERE id BETWEEN 1 AND 12 ORDER BY id"""

        // Homogeneous paths exercise typed subcolumns with different numeric widths.
        sql "DROP TABLE IF EXISTS variant_subfield_numeric"
        sql """CREATE TABLE variant_subfield_numeric (id INT, v VARIANT)
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num"="1")"""
        sql """INSERT INTO variant_subfield_numeric VALUES
            (1, parse_to_variant('{"a":1,"b":1.0}')),
            (2, parse_to_variant('{"a":1,"b":1.5}')),
            (3, parse_to_variant('{"a":0,"b":-0.0}')),
            (4, parse_to_variant('{"a":9007199254740993,"b":9007199254740992.0}')),
            (5, parse_to_variant('{"a":9007199254740992,"b":9007199254740992.0}')),
            (6, parse_to_variant('{"a":null,"b":1.0}')),
            (7, parse_to_variant('{"a":1}')),
            (8, parse_to_variant('{}')),
            (9, NULL)"""
        qt_typed_projection """SELECT id, v['a'], v['b'],
            v['a'] = v['b'], v['a'] != v['b'], v['a'] <=> v['b']
            FROM variant_subfield_numeric ORDER BY id"""
        ["=", "!=", "<=>"].each { op ->
            qt_sql """SELECT id FROM variant_subfield_numeric
                WHERE v['a'] ${op} v['b'] ORDER BY id"""
        }
    }
}
