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

suite("test_search_exact_basic") {
    def tableName = "exact_basic_test"

    sql "DROP TABLE IF EXISTS ${tableName}"

    // Simple table with basic index
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            name VARCHAR(200),
            INDEX idx_name (name) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    // Insert simple test data
    sql """INSERT INTO ${tableName} VALUES
        (1, 'apple'),
        (2, 'apple banana'),
        (3, 'banana'),
        (4, 'Apple'),
        (5, 'APPLE'),
        (6, 'apple banana cherry')
    """

    Thread.sleep(3000)

    // Test 1: EXACT should match the whole value
    qt_exact_whole "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, name FROM ${tableName} WHERE search('name:EXACT(apple banana)') ORDER BY id"

    // Test 2: EXACT should not match partial
    qt_exact_partial "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, name FROM ${tableName} WHERE search('name:EXACT(apple)') ORDER BY id"

    // Test 3: EXACT is case sensitive (without lowercase config)
    qt_exact_case "SELECT /*+SET_VAR(enable_common_expr_pushdown=true) */ id, name FROM ${tableName} WHERE search('name:EXACT(Apple)') ORDER BY id"
}
