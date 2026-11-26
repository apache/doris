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

suite("test_json_text_embedded_nul") {
    // unhex('5C') is a backslash, so every JSON literal below really contains
    // \u0000, the legal escape for U+0000 inside a JSON string. Rendering such
    // a nested value back to text must not stop at the NUL.

    qt_json_each_text_nested_array '''
        SELECT k, length(v), v
        FROM (SELECT 1) d
        LATERAL VIEW json_each_text(concat('{"x":["a', unhex('5C'), 'u0000b"]}')) t AS k, v
    '''

    qt_json_each_text_nested_object '''
        SELECT k, length(v), v
        FROM (SELECT 1) d
        LATERAL VIEW json_each_text(concat('{"x":{"y":"a', unhex('5C'), 'u0000b"}}')) t AS k, v
    '''

    // A NUL at the very end of a string used to be trimmed by the stored-length
    // helper, so keep a dedicated case for it.
    qt_json_each_text_trailing_nul '''
        SELECT k, length(v), v
        FROM (SELECT 1) d
        LATERAL VIEW json_each_text(concat('{"x":["a', unhex('5C'), 'u0000","', unhex('5C'), 'u0000"]}')) t AS k, v
    '''

    // A direct string element is copied by length, so it keeps the raw NUL byte;
    // compare it through hex() to keep the expected output printable.
    qt_explode_json_array_string_direct '''
        SELECT length(c), hex(c)
        FROM (SELECT 1) d
        LATERAL VIEW explode_json_array_string(concat('["a', unhex('5C'), 'u0000b"]')) t AS c
    '''

    qt_explode_json_array_string_nested '''
        SELECT length(c), c
        FROM (SELECT 1) d
        LATERAL VIEW explode_json_array_string(concat('[["a', unhex('5C'), 'u0000b"]]')) t AS c
    '''

    qt_explode_json_array_string_trailing_nul '''
        SELECT length(c), c
        FROM (SELECT 1) d
        LATERAL VIEW explode_json_array_string(concat('[["a', unhex('5C'), 'u0000"]]')) t AS c
    '''
}
