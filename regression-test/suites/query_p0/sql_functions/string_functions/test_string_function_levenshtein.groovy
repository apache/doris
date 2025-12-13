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

suite("test_string_function_levenshtein") {
    qt_select "SELECT levenshtein('kitten', 'sitting')"
    qt_select "SELECT levenshtein('hello', 'hello')"
    qt_select "SELECT levenshtein('a', '')"
    qt_select "SELECT levenshtein('', 'a')"
    qt_select "SELECT levenshtein('', '')"
    qt_select "SELECT levenshtein('abc', 'abdc')"
    qt_select "SELECT levenshtein('abcd', 'abc')"
    qt_select "SELECT levenshtein('abc', 'def')"
    qt_select "SELECT levenshtein('abc', 'ABC')"
    qt_select "SELECT levenshtein('测试', '测试')"
    qt_select "SELECT levenshtein('测试', '测验')"
    qt_select "SELECT levenshtein(NULL, 'abc')"
    qt_select "SELECT levenshtein('abc', NULL)"

    def tableName = "test_levenshtein_tbl"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            `id` int,
            `col1` string,
            `col2` string
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    sql "INSERT INTO ${tableName} VALUES (1, 'apple', 'app'), (2, 'book', 'back'), (3, NULL, 'abc')"
    qt_select "SELECT id, levenshtein(col1, col2) FROM ${tableName} ORDER BY id"
    sql "DROP TABLE ${tableName}"
}
