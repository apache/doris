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

    // complex value types (nested obj + array): functional coverage only
    sql '''
        SELECT id, k
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

    // complex value types in text mode: functional coverage only
    sql '''
        SELECT id, k
        FROM jdata
        LATERAL VIEW json_each_text(jval) t AS k, v
        WHERE id = 9
        ORDER BY id, k
    '''
}
