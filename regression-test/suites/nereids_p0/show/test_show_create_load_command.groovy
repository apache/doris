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

suite("test_show_create_load_command", "nereids_p0") {
    def dbName = "regression_test_nereids_p0_show"
    def tableName = "test_show_create_load_table"
    def label = "test_load_label"

    // No need for dataFilePath or file writing; use inputText for dynamic content

    try {
        sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""

        sql """DROP TABLE IF EXISTS ${dbName}.${tableName}"""

        sql """
            CREATE TABLE IF NOT EXISTS ${dbName}.${tableName} (
                id INT,
                name STRING
            )
            DISTRIBUTED BY HASH(id) BUCKETS 3
            PROPERTIES ("replication_num" = "1");
        """

        // Define the data content as a string (matches your original dynamic creation)
        def dataContent = """1,Alice
2,Bob
3,Charlie
"""

        // Perform Stream Load using the framework's streamLoad action
        streamLoad {
            db dbName
            table tableName

            set 'label', "${label}"

            set 'columns', 'id,name'
            set 'column_separator', ','

            inputText dataContent

            time 10000

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }

        // Stream Load is synchronous, so no sleep needed; the job is complete and visible immediately
        // Test the SHOW CREATE LOAD command with qualified db.label
        checkNereidsExecute("""SHOW CREATE LOAD FOR ${dbName}.${label}""")
        qt_cmd("""SHOW CREATE LOAD FOR ${dbName}.${label}""")
    } finally {
        sql """DROP TABLE IF EXISTS ${dbName}.${tableName}"""
        sql """DROP DATABASE IF EXISTS ${dbName}"""
    }
}