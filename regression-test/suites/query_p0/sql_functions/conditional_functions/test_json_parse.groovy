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
            json_str VARCHAR(500),
            json_value JSON
        ) PROPERTIES ("replication_num"="1");"""

    sql """INSERT INTO test_json_parse_table VALUES
        (1, '{"name": "Alice", "age": 30}', '{"name": "Alice2", "age": 32}'),
        (2, '{"name": "Bob", "age": 25}', '{"name": "Bob", "age": 25}'),
        (3, 'Invalid JSON String', '{"name": "Jack", "age": 28}'),
        (4, NULL, '{"name": "Jim", "age": 33}'),
        (5, '{"list": [1, 2, 3], "valid": true}', NULL),
        (6, 'Invalid JSON String', NULL),
        (7, 'null', '[1, 2, 3]'),
        (8, '', '{"key": "value"}'),
        (8, '123', '{"key2": "value2"}'),
        (9, '[1, 2, 3]', '{"key": "value"}');"""

    qt_basic_parse "SELECT json_parse_error_to_null('{\"key\": \"value\"}');"
    qt_invalid_json "SELECT json_parse_error_to_null('Invalid JSON');"
    qt_empty_string "SELECT json_parse_error_to_null('');"
    qt_null_string "SELECT json_parse_error_to_null('null');"
    qt_number_string "SELECT json_parse_error_to_null('123');"
    qt_null_input "SELECT json_parse_error_to_null(NULL);"

    qt_parse_from_table_null "SELECT id, json_parse_error_to_null(json_str) FROM test_json_parse_table ORDER BY id;"
    qt_parse_from_table_value1 "SELECT id, json_parse_error_to_value(json_str) FROM test_json_parse_table ORDER BY id;"
    qt_parse_from_table_value2 "SELECT id, json_parse_error_to_value(json_str, json_value) FROM test_json_parse_table ORDER BY id;"

    check_fold_consistency "json_parse_error_to_null('{\"key\": \"value\"}')"
    check_fold_consistency "json_parse_error_to_null('Invalid JSON')"
    check_fold_consistency "json_parse_error_to_null(NULL)"
    check_fold_consistency "json_parse_error_to_null('')"
    check_fold_consistency "json_parse_error_to_null('null')"
    check_fold_consistency "json_parse_error_to_null('123')"
    check_fold_consistency "json_parse_error_to_null('[1, 2, 3]')"


    sql """DROP TABLE IF EXISTS `test_invalid_path_tbl`;"""
    sql """
        CREATE TABLE `test_invalid_path_tbl` (
            `id` int NULL,
            `v` json NULL,
            `s` text NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY RANDOM BUCKETS AUTO
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1");
    """

    sql """ insert into test_invalid_path_tbl values (1, '{"key": "value"}', '\$.\$.'); """

    test {
        sql "SELECT id, json_exists_path(v, s) FROM test_invalid_path_tbl;"
        exception "Json path error: Invalid Json Path for value: \$.\$."
    }
}

