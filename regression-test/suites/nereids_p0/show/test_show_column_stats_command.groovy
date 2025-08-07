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

suite("test_show_column_stats_command", "nereids_p0") {
    def dbName = "test_show_column_stats_db"
    def tableName = "test_show_column_stats_table"

    try {
        // Drop and recreate the database
        sql "DROP DATABASE IF EXISTS ${dbName}"
        sql "CREATE DATABASE ${dbName}"
        sql "USE ${dbName}"

        // Create the table
        sql """
            CREATE TABLE ${tableName} (
                id INT,
                name STRING,
                score DOUBLE
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 3
            PROPERTIES("replication_num" = "1")
        """

        // Insert data
        sql """INSERT INTO ${tableName} VALUES (1, 'Alice', 72.3), (2, 'Bob', 88.0), (3, 'Charlie', 95.5)"""

        // Analyze the table for column stats
        sql "ANALYZE TABLE ${dbName}.${tableName}"

        // Execute the SHOW COLUMN STATS command
        checkNereidsExecute("""
            SHOW COLUMN STATS ${dbName}.${tableName} (id, name, score)
        """)
    } finally {
        // Cleanup
        sql "DROP DATABASE IF EXISTS ${dbName}"
    }
}
