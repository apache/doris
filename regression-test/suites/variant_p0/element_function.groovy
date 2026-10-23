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
    def variantV2Function = "parse_to_variant"
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

    sql """insert into element_fn_test values (1, ${variantV2Function}('{"arr1" : [1, 2, 3]}'), ${variantV2Function}('{"arr2" : [4, 5, 6]}'))"""
    qt_sql """select array_first((x,y) -> (x - y) < 0, cast(v['arr1'] as array<int>), cast(v1['arr2'] as array<int>)) from element_fn_test"""

    // CIR-20498: extracting a string property from a scalar-string variant
    // (e.g. `${variantV2Function}(text)['key']`) must not leak the surrounding JSON
    // double quotes. The root of such a variant is a raw JSON string, so the
    // extraction goes through the simdjson document path; a string value must be
    // returned unescaped, consistently with the structured-subcolumn path.
    def scalar = sql """select ${variantV2Function}('{"wsn":"SRFSPXFDVY","uploadTimeValue":"2026-05-20 18:40:02"}')['wsn']"""
    assertEquals("SRFSPXFDVY", scalar[0][0])

    def sub = sql """select substring(${variantV2Function}('{"uploadTimeValue":"2026-05-20 18:40:02"}')['uploadTimeValue'], 1, 10)"""
    assertEquals("2026-05-20", sub[0][0])

    // values containing escaped characters must be unescaped, not kept as raw JSON tokens
    def escaped = sql """select ${variantV2Function}('{"k":"a\\\\"b"}')['k']"""
    assertEquals("a\"b", escaped[0][0])

    // non-string scalars keep their existing JSON representation
    def num = sql """select ${variantV2Function}('{"n":49.98}')['n']"""
    assertEquals("49.98", num[0][0])

    // array / object values must keep their JSON text representation (no unquoting):
    // only the top-level string scalar is unquoted; quotes nested inside JSON are
    // part of the value and must be preserved.
    def arr = sql """select cast(${variantV2Function}('{"a":[1,2,3]}')['a'] as json)"""
    assertEquals("[1,2,3]", arr[0][0])

    def obj = sql """select sort_json_object_keys(json_extract(
            cast(${variantV2Function}('{"o":{"name":"john"}}') as json), '\$.o'))"""
    assertEquals('{"name":"john"}', obj[0][0])

    // DORIS-28435: an integer index on a VARIANT array that was itself extracted with element_at is 1-based like
    // ARRAY element_at and counts from the end when negative; 0, out-of-range indexes, non-array values and
    // missing paths give NULL, while a string index still reads object keys. On a stored column the planner
    // must not turn the integer index into the storage sub-path items.1.
    sql "DROP TABLE IF EXISTS element_at_nested_index_test"
    sql """
        CREATE TABLE element_at_nested_index_test (
            id INT,
            json_variant VARIANT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO element_at_nested_index_test VALUES
        (1, ${variantV2Function}('{"items": [2, 3, 4]}')),
        (2, ${variantV2Function}('{"items": [[5, 6], "s", 7]}')),
        (3, ${variantV2Function}('{"items": {"1": "key one"}}')),
        (4, ${variantV2Function}('{"other": 1}')),
        (5, ${variantV2Function}('{"items": [{"k": 1}, {"k": 2}]}')),
        (6, ${variantV2Function}('{"items": [2, null, 4]}'))"""
    order_qt_nested_integer_index """
        SELECT id,
               element_at(element_at(json_variant, 'items'), 1),
               element_at(element_at(json_variant, 'items'), -1),
               element_at(element_at(json_variant, 'items'), 0),
               element_at(element_at(json_variant, 'items'), 4),
               json_variant['items'][2],
               element_at(element_at(json_variant, 'items'), '1')
        FROM element_at_nested_index_test
    """
    qt_nested_integer_index_const """
        SELECT element_at(element_at(${variantV2Function}('{"items": [2, 3, 4]}'), 'items'), 1),
               element_at(element_at(${variantV2Function}('{"items": [2, 3, 4]}'), 'items'), -1)
    """

    // Filters read items and apply the index the same way: as a pushed-down predicate, after a string key that
    // follows the index, next to the object-key sub-column items.1 in one filter, and under a TopN.
    order_qt_nested_integer_index_filter """
        SELECT id FROM element_at_nested_index_test
        WHERE CAST(element_at(element_at(json_variant, 'items'), 1) AS INT) = 2
    """
    order_qt_nested_integer_index_filter_negative """
        SELECT id, json_variant['items'][2] FROM element_at_nested_index_test
        WHERE CAST(json_variant['items'][-1] AS INT) = 4
    """
    order_qt_nested_integer_index_filter_null """
        SELECT id FROM element_at_nested_index_test
        WHERE element_at(element_at(json_variant, 'items'), 1) IS NULL
    """
    order_qt_nested_integer_index_filter_object """
        SELECT id FROM element_at_nested_index_test WHERE CAST(json_variant['items'][1]['k'] AS INT) = 1
    """
    order_qt_nested_integer_index_filter_with_key """
        SELECT id FROM element_at_nested_index_test
        WHERE CAST(json_variant['items']['1'] AS STRING) = 'key one' OR CAST(json_variant['items'][1] AS INT) = 2
    """
    qt_nested_integer_index_filter_topn """
        SELECT id, CAST(json_variant['items'] AS STRING) FROM element_at_nested_index_test
        WHERE CAST(json_variant['items'][1] AS INT) = 2 ORDER BY id LIMIT 1
    """
    // MATCH needs a storage column, and a path with an integer index is not one.
    test {
        sql "SELECT id FROM element_at_nested_index_test WHERE json_variant['items'][2] MATCH_ANY 's'"
        exception "Only support match left operand is SlotRef"
    }
}
