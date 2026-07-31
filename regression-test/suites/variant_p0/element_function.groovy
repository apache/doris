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

suite("regression_test_variant_element_at", "p0")  {
      sql """
        CREATE TABLE IF NOT EXISTS element_fn_test(
            k bigint,
            v variant,
            v1 variant not null,
        )
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 4
        properties("replication_num" = "1");
    """

    sql """insert into element_fn_test values (1, '{"arr1" : [1, 2, 3]}', '{"arr2" : [4, 5, 6]}')"""
    qt_sql """select array_first((x,y) -> (x - y) < 0, cast(v['arr1'] as array<int>), cast(v1['arr2'] as array<int>)) from element_fn_test"""

    // CIR-20498: extracting a string property from a scalar-string variant
    // (e.g. `cast(text as variant)['key']`) must not leak the surrounding JSON
    // double quotes. The root of such a variant is a raw JSON string, so the
    // extraction goes through the simdjson document path; a string value must be
    // returned unescaped, consistently with the structured-subcolumn path.
    def scalar = sql """select cast('{"wsn":"SRFSPXFDVY","uploadTimeValue":"2026-05-20 18:40:02"}' as variant)['wsn']"""
    assertEquals("SRFSPXFDVY", scalar[0][0])

    def sub = sql """select substring(cast('{"uploadTimeValue":"2026-05-20 18:40:02"}' as variant)['uploadTimeValue'], 1, 10)"""
    assertEquals("2026-05-20", sub[0][0])

    // values containing escaped characters must be unescaped, not kept as raw JSON tokens
    def escaped = sql """select cast('{"k":"a\\\\"b"}' as variant)['k']"""
    assertEquals("a\"b", escaped[0][0])

    // non-string scalars keep their existing JSON representation
    def num = sql """select cast('{"n":49.98}' as variant)['n']"""
    assertEquals("49.98", num[0][0])

    // array / object values must keep their JSON text representation (no unquoting):
    // only the top-level string scalar is unquoted; quotes nested inside JSON are
    // part of the value and must be preserved.
    def arr = sql """select cast('{"a":[1,2,3]}' as variant)['a']"""
    assertEquals("[1,2,3]", arr[0][0])

    def obj = sql """select cast('{"o":{"name":"john"}}' as variant)['o']"""
    assertEquals('{"name":"john"}', obj[0][0])
}