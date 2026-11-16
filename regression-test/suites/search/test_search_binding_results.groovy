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

suite("test_search_binding_results", "p0") {
    sql "SET default_variant_enable_doc_mode = false"
    sql "SET default_variant_enable_typed_paths_to_sparse = false"
    sql "SET default_variant_max_subcolumns_count = 100"
    sql "SET enable_common_expr_pushdown = true"
    sql "SET enable_sql_cache = false"
    sql "SET enable_query_cache = false"
    sql "DROP TABLE IF EXISTS search_binding_results"
    sql """
        CREATE TABLE search_binding_results (
            id INT,
            title VARCHAR(100),
            body VARCHAR(100),
            v VARIANT,
            INDEX idx_title(title) USING INVERTED PROPERTIES("parser" = "unicode"),
            INDEX idx_v(v) USING INVERTED PROPERTIES("parser" = "unicode")
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1", "disable_auto_compaction" = "true")
    """
    sql """INSERT INTO search_binding_results VALUES
        (1, 'hello', 'hello', parse_to_variant('{"city":"hello","country":"world"}')),
        (2, 'world', 'world', parse_to_variant('{"city":"world","country":"hello"}'))
    """
    sql "SET enable_inverted_index_query_cache = true"
    // Keep the DSL/alias identical while changing the physical Variant path.
    for (int i = 0; i < 3; i++) {
        qt_city """SELECT id FROM
            (SELECT id, v['city'] AS label FROM search_binding_results) t
            WHERE search('label:hello') ORDER BY id"""
    }
    for (int i = 0; i < 3; i++) {
        qt_country """SELECT id FROM
            (SELECT id, v['country'] AS label FROM search_binding_results) t
            WHERE search('label:hello') ORDER BY id"""
    }
    qt_country_first """SELECT id FROM
        (SELECT id, v['country'] AS reverse_label FROM search_binding_results) t
        WHERE search('reverse_label:hello') ORDER BY id"""
    qt_city_second """SELECT id FROM
        (SELECT id, v['city'] AS reverse_label FROM search_binding_results) t
        WHERE search('reverse_label:hello') ORDER BY id"""

    sql "SET enable_inverted_index_query_cache = false"
    qt_constant_equality """SELECT id FROM search_binding_results
        WHERE title = 'hello' AND search('title:hello') ORDER BY id"""
    qt_constant_dsl """SELECT id FROM search_binding_results
        WHERE search(concat('title:', 'hello')) ORDER BY id"""
    qt_constant_options """SELECT id FROM search_binding_results
        WHERE search('hello', concat('{"default_field":', '"title"}')) ORDER BY id"""
    qt_intersect """SELECT title FROM search_binding_results WHERE search('title:hello')
        INTERSECT SELECT body FROM search_binding_results ORDER BY 1"""
    qt_except """SELECT title FROM search_binding_results WHERE search('title:hello')
        EXCEPT SELECT body FROM search_binding_results WHERE id = 2 ORDER BY 1"""
    test {
        sql "SELECT id FROM search_binding_results WHERE search(concat(' ', ' '))"
        exception "empty search DSL"
    }

    // A separate rowset has no city path at all. An alias must preserve missing-path metadata.
    sql """INSERT INTO search_binding_results VALUES
        (3, 'hello', 'hello', parse_to_variant('{"country":"hello"}'))"""
    qt_absent_alias """SELECT id FROM
        (SELECT id, v['city'] AS city FROM search_binding_results) t
        WHERE search('city:hello') ORDER BY id"""
    qt_absent_direct """SELECT id FROM search_binding_results
        WHERE search('v.city:hello') ORDER BY id"""
    qt_absent_parent_alias """SELECT id FROM
        (SELECT id, v AS doc FROM search_binding_results) t
        WHERE search('doc.city:hello') ORDER BY id"""
}
