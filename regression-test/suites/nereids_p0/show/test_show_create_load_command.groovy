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
    def dbName = "test_show_create_load_db"
    def tableName = "test_show_create_load_table"
    def label = "test_load_label"

    // File path where the test data will be stored
    def dataFilePath = """${context.dataPath}/test_show_create_load_data"""

    try {
        sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""

        sql """DROP TABLE IF EXISTS ${dbName}.${tableName}"""

        // Create a data file dynamically
        def dataContent = """1,Alice
2,Bob
3,Charlie
"""
        new File(dataFilePath).withWriter('UTF-8') { writer ->
            writer.write(dataContent)
        }

        sql """
            CREATE TABLE IF NOT EXISTS ${dbName}.${tableName} (
                id INT,
                name STRING
            )
            DISTRIBUTED BY HASH(id) BUCKETS 3
            PROPERTIES ("replication_num" = "1");
        """

        // Use Stream Load via curl to create the load job with label (assume FE HTTP port 8030, adjust if needed)
        def curlCommand = """curl --location-trusted -u root: -T ${dataFilePath} -H "label:${dbName}.${label}" -H "columns:id,name" -H "column_separator:," http://127.0.0.1:8030/api/${dbName}/${tableName}/_stream_load"""
        def process = curlCommand.execute()
        def code = process.waitFor()
        def output = process.text

        // Check if Stream Load succeeded (parse JSON output)
        def json = new groovy.json.JsonSlurper().parseText(output)
        if (json.Status != "Success") {
            throw new Exception("Stream Load failed: ${output}")
        }

        // Wait briefly for the job to complete
        sleep(5000)

        // Test the SHOW CREATE LOAD command
        checkNereidsExecute("""SHOW CREATE LOAD FOR ${label}""")
        qt_cmd("""SHOW CREATE LOAD FOR ${label}""")
    } finally {
        sql """DROP TABLE IF EXISTS ${dbName}.${tableName}"""
        sql """DROP DATABASE IF EXISTS ${dbName}"""

        // Delete the data file
        def dataFile = new File(dataFilePath)
        if (dataFile.exists()) {
            dataFile.delete()
        }
    }
}
