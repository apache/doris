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

suite("doc_json_functions_test") {
    qt_json_extract_general '''
        SELECT JSON_EXTRACT('{"k1":"v31","k2":300}', '$.k1');
    '''
    testFoldConst('''
        SELECT JSON_EXTRACT('{"k1":"v31","k2":300}', '$.k1');
    ''')

    qt_json_extract_null_object '''
        SELECT JSON_EXTRACT(NULL, '$.k1');
    '''
    testFoldConst('''
        SELECT JSON_EXTRACT(NULL, '$.k1');
    ''')

    qt_json_extract_null_path '''
        SELECT JSON_EXTRACT('{"k1":"v31","k2":300}', NULL);
    '''
    testFoldConst('''
        SELECT JSON_EXTRACT('{"k1":"v31","k2":300}', NULL);
    ''')

    qt_json_extract_multi_level '''
        SELECT JSON_EXTRACT('{"k1":"v31","k2":{"sub_key": 1234.56}}', '$.k2.sub_key');
    '''
    testFoldConst('''
        SELECT JSON_EXTRACT('{"k1":"v31","k2":{"sub_key": 1234.56}}', '$.k2.sub_key');
    ''')

    qt_json_extract_array_path '''
        SELECT JSON_EXTRACT(json_array("abc", 123, "2025-07-16 18:35:25"), '$[2]');
    '''
    testFoldConst('''
        SELECT JSON_EXTRACT(json_array("abc", 123, "2025-07-16 18:35:25"), '$[2]');
    ''')

    qt_json_extract_non_existent '''
        SELECT JSON_EXTRACT('{"k1":"v31","k2":300}', '$.k3');
    '''
    testFoldConst('''
        SELECT JSON_EXTRACT('{"k1":"v31","k2":300}', '$.k3');
    ''')

    qt_json_extract_multi_paths '''
        SELECT JSON_EXTRACT('{"id": 123, "name": "doris"}', '$.name', '$.id', '$.not_exists');
    '''
    testFoldConst('''
        SELECT JSON_EXTRACT('{"id": 123, "name": "doris"}', '$.name', '$.id', '$.not_exists');
    ''')

    qt_json_extract_single_match '''
        SELECT json_extract('{"id": 123, "name": "doris"}', '$.aaa', '$.name');
    '''
    testFoldConst('''
        SELECT json_extract('{"id": 123, "name": "doris"}', '$.aaa', '$.name');
    ''')

    qt_json_extract_no_match '''
        SELECT JSON_EXTRACT('{"id": 123, "name": "doris"}', '$.k1', '$.k2', '$.not_exists');
    '''
    testFoldConst('''
        SELECT JSON_EXTRACT('{"id": 123, "name": "doris"}', '$.k1', '$.k2', '$.not_exists');
    ''')

    qt_json_extract_wildcard_array '''
        SELECT json_extract('{"k": [1,2,3,4,5]}', '$.k[*]');
    '''
    testFoldConst('''
        SELECT json_extract('{"k": [1,2,3,4,5]}', '$.k[*]');
    ''')

    qt_json_extract_wildcard_object '''
        SELECT json_extract('{"k": [1,2,3,4,5], "k2": "abc", "k3": {"k4": "v4"}}', '$.*', '$.k3.k4');
    '''
    testFoldConst('''
        SELECT json_extract('{"k": [1,2,3,4,5], "k2": "abc", "k3": {"k4": "v4"}}', '$.*', '$.k3.k4');
    ''')

    qt_json_extract_recursive '''
        SELECT json_extract('{"k": 123, "b": {"k": ["ab", "cd"]}}', '$**.k');
    '''
    testFoldConst('''
        SELECT json_extract('{"k": 123, "b": {"k": ["ab", "cd"]}}', '$**.k');
    ''')

    qt_json_extract_3x_id '''
        SELECT json_extract('{"id": 123, "name": "doris"}', '$.id');
    '''
    testFoldConst('''
        SELECT json_extract('{"id": 123, "name": "doris"}', '$.id');
    ''')

    qt_json_extract_3x_array '''
        SELECT json_extract('[1, 2, 3]', '$.[1]');
    '''
    testFoldConst('''
        SELECT json_extract('[1, 2, 3]', '$.[1]');
    ''')

    qt_json_extract_3x_multi '''
        SELECT json_extract('{"k1": "v1", "k2": { "k21": 6.6, "k22": [1, 2] } }', '$.k1', '$.k2.k21', '$.k2.k22', '$.k2.k22[1]');
    '''
    testFoldConst('''
        SELECT json_extract('{"k1": "v1", "k2": { "k21": 6.6, "k22": [1, 2] } }', '$.k1', '$.k2.k21', '$.k2.k22', '$.k2.k22[1]');
    ''')

    qt_json_extract_no_quotes '''
        SELECT json_extract_no_quotes('{"id": 123, "name": "doris"}', '$.name');
    '''
    testFoldConst('''
        SELECT json_extract_no_quotes('{"id": 123, "name": "doris"}', '$.name');
    ''')

    qt_json_extract_isnull '''
        SELECT JSON_EXTRACT_ISNULL('{"id": 123, "name": "doris"}', '$.id');
    '''
    testFoldConst('''
        SELECT JSON_EXTRACT_ISNULL('{"id": 123, "name": "doris"}', '$.id');
    ''')

    qt_json_extract_bool '''
        SELECT JSON_EXTRACT_BOOL('{"id": 123, "name": "NULL"}', '$.id');
    '''
    testFoldConst('''
        SELECT JSON_EXTRACT_BOOL('{"id": 123, "name": "NULL"}', '$.id');
    ''')

    qt_json_extract_int '''
        SELECT JSON_EXTRACT_INT('{"id": 123, "name": "NULL"}', '$.id');
    '''
    testFoldConst('''
        SELECT JSON_EXTRACT_INT('{"id": 123, "name": "NULL"}', '$.id');
    ''')

    qt_json_extract_int_mismatch '''
        SELECT JSON_EXTRACT_INT('{"id": 123, "name": "doris"}', '$.name');
    '''
    testFoldConst('''
        SELECT JSON_EXTRACT_INT('{"id": 123, "name": "doris"}', '$.name');
    ''')

    qt_json_extract_string '''
        SELECT JSON_EXTRACT_STRING('{"id": 123, "name": "doris"}', '$.name');
    '''
    testFoldConst('''
        SELECT JSON_EXTRACT_STRING('{"id": 123, "name": "doris"}', '$.name');
    ''')
}
