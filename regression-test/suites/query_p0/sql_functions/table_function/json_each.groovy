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

suite('json_each') {
    sql ''' DROP TABLE IF EXISTS jdata '''
    sql '''
        CREATE TABLE IF NOT EXISTS jdata (
            id INT,
            jval JSONB
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    '''

    sql """ INSERT INTO jdata VALUES
        (1, '{"a":"foo","b":"bar"}'),
        (2, '{"x":1,"y":true,"z":null}'),
        (3, '{}'),
        (4, NULL),
        (5, '{"neg":-1,"bool_f":false}'),
        (6, '{"cn":"\u4e2d\u6587"}'),
        (7, '"a_string"'),
        (8, '[1,2,3]'),
        (9, '{"arr":[1,2],"sub":{"x":1}}')
    """

    // ---------- json_each ----------

    // basic string values: value is JSONB, shown with JSON quotes
    qt_json_each_basic '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each(jval) t AS k, v
        WHERE id = 1
        ORDER BY id, k
    '''

    // int / bool true / JSON null → SQL NULL
    qt_json_each_mixed '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each(jval) t AS k, v
        WHERE id = 2
        ORDER BY id, k
    '''

    // empty object → 0 rows
    qt_json_each_empty '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each(jval) t AS k, v
        WHERE id = 3
        ORDER BY id, k
    '''

    // SQL NULL input, non-outer → 0 rows
    qt_json_each_null_input '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each(jval) t AS k, v
        WHERE id = 4
        ORDER BY id, k
    '''

    // ids 1-4 combined
    qt_json_each_all '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each(jval) t AS k, v
        WHERE id IN (1, 2, 3, 4)
        ORDER BY id, k
    '''

    // inline literal
    qt_json_each_literal """
        SELECT k, v
        FROM (SELECT 1) dummy
        LATERAL VIEW json_each('{"name":"doris","version":3}') t AS k, v
        ORDER BY k
    """

    // negative int, boolean false
    qt_json_each_neg_false '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each(jval) t AS k, v
        WHERE id = 5
        ORDER BY id, k
    '''

    // unicode string value
    qt_json_each_unicode '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each(jval) t AS k, v
        WHERE id = 6
        ORDER BY id, k
    '''

    // non-object input: JSON string → 0 rows
    qt_json_each_non_object_str '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each(jval) t AS k, v
        WHERE id = 7
        ORDER BY id, k
    '''

    // non-object input: JSON array → 0 rows
    qt_json_each_non_object_arr '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each(jval) t AS k, v
        WHERE id = 8
        ORDER BY id, k
    '''

    // complex value types (nested obj + array): values are JSONB
    qt_json_each_complex '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each(jval) t AS k, v
        WHERE id = 9
        ORDER BY id, k
    '''

    // ---------- json_each_text ----------

    // string values unquoted in text mode
    qt_json_each_text_basic '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each_text(jval) t AS k, v
        WHERE id = 1
        ORDER BY id, k
    '''

    // int / bool / JSON null → SQL NULL
    qt_json_each_text_mixed '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each_text(jval) t AS k, v
        WHERE id = 2
        ORDER BY id, k
    '''

    // empty object → 0 rows
    qt_json_each_text_empty '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each_text(jval) t AS k, v
        WHERE id = 3
        ORDER BY id, k
    '''

    // SQL NULL input, non-outer → 0 rows
    qt_json_each_text_null_input '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each_text(jval) t AS k, v
        WHERE id = 4
        ORDER BY id, k
    '''

    // ids 1-4 combined
    qt_json_each_text_all '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each_text(jval) t AS k, v
        WHERE id IN (1, 2, 3, 4)
        ORDER BY id, k
    '''

    // inline literal: strings unquoted
    qt_json_each_text_literal """
        SELECT k, v
        FROM (SELECT 1) dummy
        LATERAL VIEW json_each_text('{"name":"doris","version":3}') t AS k, v
        ORDER BY k
    """

    // negative int, boolean false
    qt_json_each_text_neg_false '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each_text(jval) t AS k, v
        WHERE id = 5
        ORDER BY id, k
    '''

    // unicode string value: unquoted in text mode
    qt_json_each_text_unicode '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each_text(jval) t AS k, v
        WHERE id = 6
        ORDER BY id, k
    '''

    // non-object input: JSON string → 0 rows
    qt_json_each_text_non_object_str '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each_text(jval) t AS k, v
        WHERE id = 7
        ORDER BY id, k
    '''

    // non-object input: JSON array → 0 rows
    qt_json_each_text_non_object_arr '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each_text(jval) t AS k, v
        WHERE id = 8
        ORDER BY id, k
    '''

    // complex value types in text mode: values are text representation
    qt_json_each_text_complex '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each_text(jval) t AS k, v
        WHERE id = 9
        ORDER BY id, k
    '''

    // ---------- json_each_outer ----------

    // outer: NULL input → 1 row with NULL k, NULL v (original row preserved)
    qt_json_each_outer_null_input '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each_outer(jval) t AS k, v
        WHERE id = 4
        ORDER BY id, k
    '''

    // outer: empty object → 1 row with NULL k, NULL v
    qt_json_each_outer_empty '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each_outer(jval) t AS k, v
        WHERE id = 3
        ORDER BY id, k
    '''

    // outer: normal object → same result as non-outer json_each
    qt_json_each_outer_basic '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each_outer(jval) t AS k, v
        WHERE id = 1
        ORDER BY id, k
    '''

    // outer: non-object inputs (string / array) → 1 row each with NULL k, NULL v
    qt_json_each_outer_non_object '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each_outer(jval) t AS k, v
        WHERE id IN (7, 8)
        ORDER BY id, k
    '''

    // outer: mixed ids 1-4: id=3 (empty) and id=4 (NULL) each emit one NULL-padded row
    qt_json_each_outer_all '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each_outer(jval) t AS k, v
        WHERE id IN (1, 2, 3, 4)
        ORDER BY id, k
    '''

    // ---------- json_each_text_outer ----------

    // outer: NULL input → 1 row with NULL k, NULL v
    qt_json_each_text_outer_null_input '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each_text_outer(jval) t AS k, v
        WHERE id = 4
        ORDER BY id, k
    '''

    // outer: empty object → 1 row with NULL k, NULL v
    qt_json_each_text_outer_empty '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each_text_outer(jval) t AS k, v
        WHERE id = 3
        ORDER BY id, k
    '''

    // outer: normal object → same as json_each_text (strings unquoted)
    qt_json_each_text_outer_basic '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each_text_outer(jval) t AS k, v
        WHERE id = 1
        ORDER BY id, k
    '''

    // outer: non-object inputs → 1 row each with NULL k, NULL v
    qt_json_each_text_outer_non_object '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each_text_outer(jval) t AS k, v
        WHERE id IN (7, 8)
        ORDER BY id, k
    '''

    // outer: mixed ids 1-4
    qt_json_each_text_outer_all '''
        SELECT id, k, v
        FROM jdata
        LATERAL VIEW json_each_text_outer(jval) t AS k, v
        WHERE id IN (1, 2, 3, 4)
        ORDER BY id, k
    '''

    // ---------- Multiple LATERAL VIEW combinations ----------

    // double json_each: expand nested object
    qt_multi_lateral_nested '''
        SELECT id, k1, k2, v2
        FROM jdata
        LATERAL VIEW json_each(jval) t1 AS k1, v1
        LATERAL VIEW json_each(v1) t2 AS k2, v2
        WHERE id = 9 AND k1 = 'sub'
        ORDER BY k1, k2
    '''

    // json_each with const literal in second lateral view
    qt_multi_lateral_const '''
        SELECT id, k1, k2, v2
        FROM jdata
        LATERAL VIEW json_each(jval) t1 AS k1, v1
        LATERAL VIEW json_each('{"x":1,"y":2}') t2 AS k2, v2
        WHERE id = 1
        ORDER BY k1, k2
    '''

    // three lateral views: column + two const literals with different sizes
    qt_multi_lateral_three_mixed '''
        SELECT id, k1, k2, k3
        FROM jdata
        LATERAL VIEW json_each(jval) t1 AS k1, v1
        LATERAL VIEW json_each('{"a":1,"b":2}') t2 AS k2, v2
        LATERAL VIEW json_each('{"x":1,"y":2,"z":3}') t3 AS k3, v3
        WHERE id = 1
        ORDER BY k1, k2, k3
    '''

    // multiple json_each on same row: cartesian product
    qt_multi_lateral_cartesian '''
        SELECT k1, v1, k2, v2
        FROM (SELECT '{"a":1,"b":2}' AS j1, '{"x":10,"y":20}' AS j2) t
        LATERAL VIEW json_each(j1) t1 AS k1, v1
        LATERAL VIEW json_each(j2) t2 AS k2, v2
        ORDER BY k1, k2
    '''

    // ---------- Corner cases ----------

    // deeply nested object keys
    qt_corner_deep_nesting '''
        SELECT k, v
        FROM (SELECT '{"level1":{"level2":{"level3":"deep"}}}' AS j) t
        LATERAL VIEW json_each(j) t AS k, v
        ORDER BY k
    '''

    // special characters in keys
    qt_corner_special_keys '''
        SELECT k, v
        FROM (SELECT '{"key with spaces":"v1","key.with.dots":"v2","key-with-dash":"v3"}' AS j) t
        LATERAL VIEW json_each_text(j) t AS k, v
        ORDER BY k
    '''

    // large number of keys
    qt_corner_many_keys '''
        SELECT COUNT(*) AS key_count
        FROM (SELECT '{"k1":1,"k2":2,"k3":3,"k4":4,"k5":5,"k6":6,"k7":7,"k8":8,"k9":9,"k10":10}' AS j) t
        LATERAL VIEW json_each(j) t AS k, v
    '''

    // empty string key
    qt_corner_empty_key '''
        SELECT k, v
        FROM (SELECT '{"":"empty_key_value","normal":"value"}' AS j) t
        LATERAL VIEW json_each_text(j) t AS k, v
        ORDER BY k
    '''

    // const input across multiple blocks: test process_close state reset
    qt_corner_const_multi_block '''
        SELECT k, v
        FROM (
            SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3
        ) t
        LATERAL VIEW json_each_text('{"const":"value"}') t AS k, v
        ORDER BY id, k
    '''
}
