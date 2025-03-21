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

suite("test_json_parse") {
    sql """DROP TABLE IF EXISTS `test_json_parse_table`;"""
    sql """CREATE TABLE test_json_parse_table (
            id INT,
            json_str VARCHAR(500)
        ) PROPERTIES ("replication_num"="1");"""

    sql """INSERT INTO test_json_parse_table VALUES
        (1, '{"name": "Alice", "age": 30}'),
        (2, '{"name": "Bob", "age": 25}'),
        (3, 'Invalid JSON String'),
        (4, NULL),
        (5, '{"list": [1, 2, 3], "valid": true}'),
        (6, ''),
        (7, 'null'),
        (8, '123'),
        (9, '[1, 2, 3]');"""

    qt_basic_parse "SELECT json_parse_error_to_null('{\"key\": \"value\"}');"
    qt_invalid_json "SELECT json_parse_error_to_null('Invalid JSON');"
    qt_empty_string "SELECT json_parse_error_to_null('');"
    qt_null_string "SELECT json_parse_error_to_null('null');"
    qt_number_string "SELECT json_parse_error_to_null('123');"
    qt_null_input "SELECT json_parse_error_to_null(NULL);"

    qt_parse_from_table "SELECT id, json_parse_error_to_null(json_str) FROM test_json_parse_table ORDER BY id;"

    check_fold_consistency "json_parse_error_to_null('{\"key\": \"value\"}')"
    check_fold_consistency "json_parse_error_to_null('Invalid JSON')"
    check_fold_consistency "json_parse_error_to_null(NULL)"
    check_fold_consistency "json_parse_error_to_null('')"
    check_fold_consistency "json_parse_error_to_null('null')"
    check_fold_consistency "json_parse_error_to_null('123')"
    check_fold_consistency "json_parse_error_to_null('[1, 2, 3]')"
}

