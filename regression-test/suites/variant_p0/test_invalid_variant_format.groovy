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

suite("test_variant_data_format", "variant_type") {
    // Assuming `sql` is a method available in this context
    sql """ set experimental_enable_nereids_planner = false """
    sql """ set experimental_enable_nereids_planner = true """
    sql """ set enable_fallback_to_original_planner = true """

    def testTable = "tbl_test_variant"

    sql "DROP TABLE IF EXISTS ${testTable}"

    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            id INT,
            v VARIANT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES("replication_num" = "1", "disable_auto_compaction" = "true");
        """

    println "Table ${testTable} is created"

    def testCases = [
        [query: """INSERT INTO ${testTable} (id, v) VALUES (1, '"string_value"');""", description: 'String value'],
        [query: """INSERT INTO ${testTable} (id, v) VALUES (2, '12345');""", description: 'Integer value'],
        [query: """INSERT INTO ${testTable} (id, v) VALUES (3, '123.45');""", description: 'Float value'],
        [query: """INSERT INTO ${testTable} (id, v) VALUES (4, '{"key": "value"}');""", description: 'JSON object'],
        [query: """INSERT INTO ${testTable} (id, v) VALUES (5, 'true');""", description: 'Boolean true'],
        [query: """INSERT INTO ${testTable} (id, v) VALUES (6, 'false');""", description: 'Boolean false'],
        [query: """INSERT INTO ${testTable} (id, v) VALUES (7, 'null');""", description: 'Null value'],
        [query: """INSERT INTO ${testTable} (id, v) VALUES (8, '["array", "of", "values"]');""", description: 'Array of strings'],
        [query: """INSERT INTO ${testTable} (id, v) VALUES (9, '[1, 2, 3]');""", description: 'Array of integers'],
        [query: """INSERT INTO ${testTable} (id, v) VALUES (10, '[1.1, 2.2, 3.3]');""", description: 'Array of floats'],
        [query: """INSERT INTO ${testTable} (id, v) VALUES (11, '[true, false, true]');""", description: 'Array of booleans'],
        [query: """INSERT INTO ${testTable} (id, v) VALUES (12, '[{"key1": "value1"}, {"key2": "value2"}]');""", description: 'Array of JSON objects']
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
        [query: "SELECT v FROM ${testTable} WHERE id = 1", description: 'Query case 1'],
        [query: "SELECT v FROM ${testTable} WHERE id = 2", description: 'Query case 2'],
        [query: "SELECT v FROM ${testTable} WHERE id = 3", description: 'Query case 3'],
        [query: "SELECT v FROM ${testTable} WHERE id = 4", description: 'Query case 4'],
        [query: "SELECT v FROM ${testTable} WHERE id = 5", description: 'Query case 5'],
        [query: "SELECT v FROM ${testTable} WHERE id = 6", description: 'Query case 6'],
        [query: "SELECT v FROM ${testTable} WHERE id = 7", description: 'Query case 7'],
        [query: "SELECT v FROM ${testTable} WHERE id = 8", description: 'Query case 8'],
        [query: "SELECT v FROM ${testTable} WHERE id = 9", description: 'Query case 9'],
        [query: "SELECT v FROM ${testTable} WHERE id = 10", description: 'Query case 10'],
        [query: "SELECT v FROM ${testTable} WHERE id = 11", description: 'Query case 11'],
        [query: "SELECT v FROM ${testTable} WHERE id = 12", description: 'Query case 12']
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