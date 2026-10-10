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

// Regression coverage for https://github.com/apache/doris/issues/68074 :
// a JSON boolean stored in a homogeneous-boolean VARIANT path used to reconstruct as the
// number 1/0 instead of the JSON literal true/false, so `{"b": true}` did not round-trip.
suite("test_variant_boolean_json_output", "p0") {
    def variantV2Function = getFeConfig("enable_variant_v2").toBoolean() ? "parse_to_variant" : ""
    def table_name = "test_variant_boolean_json_output"

    sql "DROP TABLE IF EXISTS ${table_name}"
    sql """
        CREATE TABLE ${table_name} (
            id INT,
            j VARIANT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """ insert into ${table_name} values
            (1, ${variantV2Function}('{"b": true}')),
            (2, ${variantV2Function}('{"b": false}')),
            (3, ${variantV2Function}('{"o": {"b": true}}')),
            (4, ${variantV2Function}('{"a": [true, false]}')),
            (5, ${variantV2Function}('{"b": true, "s": "x"}'))
    """

    // Whole-document JSON reconstruction: a homogeneous boolean path must print as a JSON
    // literal, not as the number 1/0. These are simple single-key documents, so the exact
    // text is unambiguous.
    def scalarTrue = sql """ select cast(j as string) from ${table_name} where id = 1 """
    assertEquals('{"b":true}', scalarTrue[0][0])
    def scalarFalse = sql """ select cast(j as string) from ${table_name} where id = 2 """
    assertEquals('{"b":false}', scalarFalse[0][0])

    // `j['b']` (element_at on a scalar boolean root) takes a different code path from the
    // structured-document serializer above (it can bypass straight to the ordinary SQL
    // BOOLEAN-to-STRING caster) and must be fixed independently of it.
    def elementAtTrue = sql """ select cast(j['b'] as string) from ${table_name} where id = 1 """
    assertEquals('true', elementAtTrue[0][0])
    def elementAtFalse = sql """ select cast(j['b'] as string) from ${table_name} where id = 2 """
    assertEquals('false', elementAtFalse[0][0])

    // A JSON function reading back the reconstructed document must see a JSON boolean, not a
    // JSON number, once the document itself round-trips correctly.
    def jsonExtractTrue = sql """
        select json_extract(cast(j as string), '\$.b') from ${table_name} where id = 1
    """
    assertEquals('true', jsonExtractTrue[0][0])

    // Nested object: still a single key, so the exact text stays unambiguous.
    def nested = sql """ select cast(j as string) from ${table_name} where id = 3 """
    assertEquals('{"o":{"b":true}}', nested[0][0])

    // array<boolean>: read back through json_extract element-by-element rather than
    // comparing the raw text, since VARIANT's array JSON writer inserts a space after each
    // element delimiter (independent of this fix) and is not what this regression targets.
    def arrayFirst = sql """
        select json_extract(cast(j as string), '\$.a[0]') from ${table_name} where id = 4
    """
    assertEquals('true', arrayFirst[0][0])
    def arraySecond = sql """
        select json_extract(cast(j as string), '\$.a[1]') from ${table_name} where id = 4
    """
    assertEquals('false', arraySecond[0][0])

    // Control case: a boolean sharing its path with a string is stored as JSONB (not a plain
    // BOOLEAN subcolumn) and already printed correctly before this fix. It must keep doing so.
    def heterogeneousB = sql """
        select json_extract(cast(j as string), '\$.b') from ${table_name} where id = 5
    """
    assertEquals('true', heterogeneousB[0][0])
    def heterogeneousS = sql """
        select json_extract(cast(j as string), '\$.s') from ${table_name} where id = 5
    """
    assertEquals('"x"', heterogeneousS[0][0])

    // Compatibility guard: this fix must be scoped to VARIANT JSON reconstruction only.
    // An ordinary SQL BOOLEAN column keeps rendering as 1/0 when cast to STRING.
    sql "DROP TABLE IF EXISTS test_variant_boolean_json_output_plain_bool"
    sql """
        CREATE TABLE test_variant_boolean_json_output_plain_bool (
            id INT,
            b BOOLEAN
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """ insert into test_variant_boolean_json_output_plain_bool values (1, true), (2, false) """
    def plainBool = sql """
        select cast(b as string) from test_variant_boolean_json_output_plain_bool order by id
    """
    assertEquals('1', plainBool[0][0])
    assertEquals('0', plainBool[1][0])
}
