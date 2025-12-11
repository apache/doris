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
    // 1. Basic functional test
    // Expected: 3 (k->s, e->i, n->g)
    qt_select_basic "SELECT levenshtein('kitten', 'sitting')"
    
    // Expected: 0 (identical strings)
    qt_select_equal "SELECT levenshtein('hello', 'hello')"
    
    // Expected: 1 (empty string boundary)
    qt_select_empty "SELECT levenshtein('a', '')"

    // 2. NULL handling test
    // Expected: NULL
    qt_select_null1 "SELECT levenshtein(NULL, 'abc')"
    qt_select_null2 "SELECT levenshtein('abc', NULL)"

    // 3. Table query test
    def tableName = "test_levenshtein_tbl"
    sql "DROP TABLE IF EXISTS ${tableName}"
    
    sql """
        CREATE TABLE ${tableName} (
            `id` int,
            `s1` varchar(50),
            `s2` varchar(50)
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ INSERT INTO ${tableName} VALUES 
            (1, 'apple', 'app'), 
            (2, 'book', 'back'), 
            (3, NULL, 'abc') 
    """

    // Expected results: 
    // 1, 2
    // 2, 2
    // 3, NULL
    qt_select_from_table "SELECT id, levenshtein(s1, s2) FROM ${tableName} ORDER BY id"

    sql "DROP TABLE IF EXISTS ${tableName}"
}