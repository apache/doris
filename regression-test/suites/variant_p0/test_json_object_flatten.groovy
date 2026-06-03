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

suite("regression_test_json_object_flatten", "p0") {
    // 1) JSONB column: function takes JSONB directly.
    sql """DROP TABLE IF EXISTS json_object_flatten_jsonb_t"""
    sql """
        CREATE TABLE json_object_flatten_jsonb_t (
            k bigint,
            j jsonb
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true");
    """
    sql """INSERT INTO json_object_flatten_jsonb_t VALUES (1, '{"a": {"b": 2}}')"""
    sql """INSERT INTO json_object_flatten_jsonb_t VALUES (2, '{"a": {"b": {"c": 3}}}')"""
    sql """INSERT INTO json_object_flatten_jsonb_t VALUES (3, '{"a": 1, "b": "hi"}')"""
    sql """INSERT INTO json_object_flatten_jsonb_t VALUES (4, '{}')"""
    sql """INSERT INTO json_object_flatten_jsonb_t VALUES (5, '{"a": {}}')"""
    sql """INSERT INTO json_object_flatten_jsonb_t VALUES (6, '{"a": null}')"""
    sql """INSERT INTO json_object_flatten_jsonb_t VALUES (7, '{"a": {"b": null}}')"""
    sql """INSERT INTO json_object_flatten_jsonb_t VALUES (8, '{"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":{"i":{"j":{"k":{"l":1}}}}}}}}}}}}')"""
    sql """INSERT INTO json_object_flatten_jsonb_t VALUES (9, NULL)"""
    sql """INSERT INTO json_object_flatten_jsonb_t VALUES (10, '42')"""
    sql """INSERT INTO json_object_flatten_jsonb_t VALUES (11, '"hello"')"""
    // Array-leaf cases: keep-arrays semantics — the array stays opaque at
    // its dot-path; element-level keys must NOT leak into the flat output.
    sql """INSERT INTO json_object_flatten_jsonb_t VALUES (12, '{"a": [1, 2, 3]}')"""
    sql """INSERT INTO json_object_flatten_jsonb_t VALUES (13, '{"a": [{"b": 1}, {"b": 2}]}')"""
    sql """INSERT INTO json_object_flatten_jsonb_t VALUES (14, '{"a": {"b": [1, 2, 3]}}')"""
    sql """INSERT INTO json_object_flatten_jsonb_t VALUES (15, '{"a": {"b": [{"c": 1}, {"c": 2}]}}')"""
    sql """INSERT INTO json_object_flatten_jsonb_t VALUES (16, '[1, 2, {"x": 3}]')"""
    sql """INSERT INTO json_object_flatten_jsonb_t VALUES (17, '{"x": {"s": 1, "a": [1, 2], "o": {"k": "v"}}}')"""

    qt_sql_jsonb """SELECT k, json_object_flatten(j) FROM json_object_flatten_jsonb_t ORDER BY k"""

    // 2) VARIANT column: Nereids inserts an implicit Variant -> JSONB cast,
    //    so users can pass the variant column straight to json_object_flatten.
    sql """DROP TABLE IF EXISTS json_object_flatten_variant_t"""
    sql """
        CREATE TABLE json_object_flatten_variant_t (
            k bigint,
            v variant
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true");
    """
    sql """INSERT INTO json_object_flatten_variant_t VALUES (1, '{"a": {"b": 2}}')"""
    sql """INSERT INTO json_object_flatten_variant_t VALUES (2, '{"a": {"b": {"c": 3}}}')"""
    sql """INSERT INTO json_object_flatten_variant_t VALUES (3, '{"a": 1, "b": "hi"}')"""
    sql """INSERT INTO json_object_flatten_variant_t VALUES (4, '{}')"""
    sql """INSERT INTO json_object_flatten_variant_t VALUES (5, '{"a": {}}')"""
    sql """INSERT INTO json_object_flatten_variant_t VALUES (6, '{"a": null}')"""
    sql """INSERT INTO json_object_flatten_variant_t VALUES (7, '{"a": {"b": null}}')"""
    sql """INSERT INTO json_object_flatten_variant_t VALUES (8, '{"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":{"i":{"j":{"k":{"l":1}}}}}}}}}}}}')"""
    sql """INSERT INTO json_object_flatten_variant_t VALUES (9, NULL)"""
    sql """INSERT INTO json_object_flatten_variant_t VALUES (10, '42')"""
    sql """INSERT INTO json_object_flatten_variant_t VALUES (11, '"hello"')"""
    // Array-leaf cases through the variant -> jsonb cast path.
    sql """INSERT INTO json_object_flatten_variant_t VALUES (12, '{"a": [1, 2, 3]}')"""
    sql """INSERT INTO json_object_flatten_variant_t VALUES (13, '{"a": [{"b": 1}, {"b": 2}]}')"""
    sql """INSERT INTO json_object_flatten_variant_t VALUES (14, '{"a": {"b": [1, 2, 3]}}')"""
    sql """INSERT INTO json_object_flatten_variant_t VALUES (15, '{"a": {"b": [{"c": 1}, {"c": 2}]}}')"""
    sql """INSERT INTO json_object_flatten_variant_t VALUES (16, '[1, 2, {"x": 3}]')"""
    sql """INSERT INTO json_object_flatten_variant_t VALUES (17, '{"x": {"s": 1, "a": [1, 2], "o": {"k": "v"}}}')"""

    qt_sql_variant """SELECT k, json_object_flatten(v) FROM json_object_flatten_variant_t ORDER BY k"""
}
