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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_invalid_json_format", "p0") {
    // Assuming `sql` is a method available in this context
    sql """ set experimental_enable_nereids_planner = false """
    sql """ set experimental_enable_nereids_planner = true """
    sql """ set enable_fallback_to_original_planner = true """

    def testTable = "tbl_test_json"

    sql "DROP TABLE IF EXISTS ${testTable}"

    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            id INT,
            j JSON
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES("replication_num" = "1", "disable_auto_compaction" = "true");
        """

    println "Table ${testTable} is created"

    def testCases = [
        [query: """INSERT INTO ${testTable} (id, j) VALUES (1, '{"key1": "value1", "key2": "value2"}');""", description: 'Correct JSON syntax'],
        [query: """INSERT INTO ${testTable} (id, j) VALUES (2, '{"key": "valüe"}');""", description: 'Invalid character encoding'],
        [query: """INSERT INTO ${testTable} (id, j) VALUES (3, '{"key": 123456789012345678901234567890}');""", description: 'Excessively large number'],
        [query: """INSERT INTO ${testTable} (id, j) VALUES (4, '{"key": "${'a' * 10000}"}');""", description: 'Excessively large string'],
        [query: """INSERT INTO ${testTable} (id, j) VALUES (5, '{"key": ${'{' * 1000}"value"${'}' * 1000}}');""", description: 'Excessive nesting levels'],
        [query: """INSERT INTO ${testTable} (id, j) VALUES (6, '{"key": [1, "string", true]}');""", description: 'Mixed arrays of different types'],
        [query: """INSERT INTO ${testTable} (id, j) VALUES (7, '{"key1": "value1", "key2": "value2"');""", description: 'Missing closing brace'],
        [query: """INSERT INTO ${testTable} (id, j) VALUES (8, '{"key": "value", "key": "duplicate"}');""", description: 'Duplicate keys'],
        [query: """INSERT INTO ${testTable} (id, j) VALUES (9, '{"key": ["value1", "value2", {"nested_key": "nested_value"}}');""", description: 'Mixed nested structures'],
        [query: """INSERT INTO ${testTable} (id, j) VALUES (10, '{"key": true false}');""", description: 'Missing comma'],
        [query: """INSERT INTO ${testTable} (id, j) VALUES (11, '{"key": null, "key2": "value"}');""", description: 'Null value'],
        [query: """INSERT INTO ${testTable} (id, j) VALUES (12, '{"key": "value", "key2": [1, 2, "three", null]}');""", description: 'Mixed array types including null']
    ]

    testCases.each { testCase ->
        try {
            sql.execute(testCase.query)
            println "Test case '${testCase.description}' executed."
        } catch (Exception e) {
            println "Test case '${testCase.description}' failed with error: ${e.message}"
        }
    }

    def queryCases = [
        [query: "SELECT j FROM ${testTable} WHERE id = 1", description: 'Query case 1'],
        [query: "SELECT j FROM ${testTable} WHERE id = 2", description: 'Query case 2'],
        [query: "SELECT j FROM ${testTable} WHERE id = 3", description: 'Query case 3'],
        [query: "SELECT j FROM ${testTable} WHERE id = 4", description: 'Query case 4'],
        [query: "SELECT j FROM ${testTable} WHERE id = 5", description: 'Query case 5'],
        [query: "SELECT j FROM ${testTable} WHERE id = 6", description: 'Query case 6'],
        [query: "SELECT j FROM ${testTable} WHERE id = 7", description: 'Query case 7'],
        [query: "SELECT j FROM ${testTable} WHERE id = 8", description: 'Query case 8'],
        [query: "SELECT j FROM ${testTable} WHERE id = 9", description: 'Query case 9'],
        [query: "SELECT j FROM ${testTable} WHERE id = 10", description: 'Query case 10'],
        [query: "SELECT j FROM ${testTable} WHERE id = 11", description: 'Query case 11'],
        [query: "SELECT j FROM ${testTable} WHERE id = 12", description: 'Query case 12']
    ]

    queryCases.each { queryCase ->
        try {
            def result = sql.execute(queryCase.query)
            println "Query case '${queryCase.description}' executed. Result: ${result}"
        } catch (Exception e) {
            println "Query case '${queryCase.description}' failed with error: ${e.message}"
        }
    }

}