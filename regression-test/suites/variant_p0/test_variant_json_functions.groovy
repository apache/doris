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

// JSON functions accept a Variant argument by casting it to JSON implicitly.
suite("test_variant_json_functions", "p0") {
    sql "DROP TABLE IF EXISTS test_variant_json_functions"
    sql """
        CREATE TABLE test_variant_json_functions (k INT, v VARIANT)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_variant_json_functions
        SELECT k, parse_to_variant(doc) FROM (
            SELECT 1 AS k, '{"a":1,"s":"x","b":{"c":[1,2]}}' AS doc
            UNION ALL SELECT 2, '{"a":2,"s":"yz","b":{"c":[3]},"n":{"m":true}}'
            UNION ALL SELECT 3, '[1,"two",{"three":3}]'
            UNION ALL SELECT 4, '42'
            UNION ALL SELECT 5, 'abc'
            UNION ALL SELECT 6, NULL) t
    """

    // json_insert and json_set reject a scalar document, so they only run on object rows.
    order_qt_insert_set_replace_remove """
        SELECT k, json_insert(v, '\$.z', 9), json_set(v, '\$.a', 0), json_replace(v, '\$.s', 'r'),
            json_remove(v, '\$.a')
        FROM test_variant_json_functions WHERE k IN (1, 2, 6) ORDER BY k
    """
    order_qt_extract """
        SELECT k, json_extract(v, '\$.b.c'), json_extract_string(v, '\$.s'), json_extract_int(v, '\$.a'),
            json_extract_bigint(v, '\$.a'), json_extract_double(v, '\$.a'), json_extract_bool(v, '\$.n.m'),
            json_extract_isnull(v, '\$.a'), jsonb_extract(v, '\$[1]'), get_json_string(v, '\$.s'),
            get_json_int(v, '\$.a')
        FROM test_variant_json_functions ORDER BY k
    """
    order_qt_inspect """
        SELECT k, json_keys(v), json_length(v), json_type(v, '\$'), json_exists_path(v, '\$.s'),
            json_contains(v, '1', '\$.a'), json_search(v, 'one', 'x'), json_hash(v),
            json_object_flatten(v), sort_json_object_keys(v), normalize_json_numbers_to_double(v)
        FROM test_variant_json_functions ORDER BY k
    """
    // Variant values nested into new JSON documents keep their JSON structure.
    order_qt_construct """
        SELECT k, to_json(v), json_array(v, 1), json_array_ignore_null(v, NULL), json_object('v', v),
            json_insert('{}', '\$.v', v), json_set('{}', '\$.b', v['b']), json_replace('{"v":0}', '\$.v', v['a'])
        FROM test_variant_json_functions ORDER BY k
    """

    // Variant elements of an ARRAY<VARIANT> convert the same way.
    order_qt_array_of_variant """
        SELECT k, to_json(CAST(v AS ARRAY<VARIANT>)), CAST(CAST(v AS ARRAY<VARIANT>) AS JSON),
            json_array(CAST(v AS ARRAY<VARIANT>))
        FROM test_variant_json_functions WHERE k = 3
    """

    // The implicit cast gives the same result as the explicit one.
    order_qt_same_as_explicit_cast """
        SELECT k,
            CAST(json_replace(v, '\$.s', 'r') AS STRING) <=> CAST(json_replace(CAST(v AS JSON), '\$.s', 'r') AS STRING),
            CAST(json_extract(v, '\$.b') AS STRING) <=> CAST(json_extract(CAST(v AS JSON), '\$.b') AS STRING),
            CAST(json_keys(v) AS STRING) <=> CAST(json_keys(CAST(v AS JSON)) AS STRING),
            json_hash(v) <=> json_hash(CAST(v AS JSON)),
            CAST(to_json(v) AS STRING) <=> CAST(CAST(v AS JSON) AS STRING),
            CAST(json_array(v) AS STRING) <=> CAST(json_array(CAST(v AS JSON)) AS STRING)
        FROM test_variant_json_functions ORDER BY k
    """

    // Functions with a string signature keep treating Variant as text, so an unparsable document
    // that was stored as a Variant string is still reported as invalid JSON.
    order_qt_string_signature """
        SELECT k, json_valid(v), json_quote(v), json_unquote(v)
        FROM test_variant_json_functions WHERE k IN (1, 4, 5) ORDER BY k
    """
    order_qt_json_parse """
        SELECT k, json_parse(v) FROM test_variant_json_functions WHERE k IN (1, 4) ORDER BY k
    """

    // Typed Variant scalars without a JSON counterpart become JSON strings.
    order_qt_typed_scalars """
        SELECT json_type(CAST(CAST('2026-01-02' AS DATE) AS VARIANT), '\$'),
            json_extract(CAST(CAST('2026-01-02 03:04:05' AS DATETIME) AS VARIANT), '\$'),
            json_type(CAST(CAST(1.50 AS DECIMAL(5, 2)) AS VARIANT), '\$'),
            json_array(CAST(CAST('2026-01-02' AS DATE) AS VARIANT), CAST(1.50 AS DECIMAL(5, 2)))
    """

    // A timezone-aware Variant timestamp is formatted with the session time zone on every path.
    // Doc mode does not accept a TIMESTAMPTZ path, so the table turns it off explicitly.
    sql "DROP TABLE IF EXISTS test_variant_json_functions_tz"
    sql """
        CREATE TABLE test_variant_json_functions_tz (
            k INT, v VARIANT<'ts':timestamptz(6), PROPERTIES("variant_enable_doc_mode" = "false")>)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "SET time_zone = '+08:00'"
    sql """INSERT INTO test_variant_json_functions_tz
        SELECT 1, parse_to_variant('{"ts":"2026-01-01 10:00:00+08:00"}')"""
    order_qt_session_time_zone """
        SELECT k, CAST(v AS JSON), to_json(v), json_array(v), json_object('ts', v['ts'])
        FROM test_variant_json_functions_tz ORDER BY k
    """
}
