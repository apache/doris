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

suite("test_show_columns_command", "nereids_p0") {
    def dbName = "test_show_columns_db"
    def tableName = "test_show_columns_table"

    try {
        sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
        sql """
            CREATE TABLE IF NOT EXISTS ${dbName}.${tableName} (
                id INT,
                name STRING,
                score FLOAT
            )
            DISTRIBUTED BY HASH(id) BUCKETS 3
            PROPERTIES ("replication_num" = "1");
        """

        // Test SHOW COLUMNS
        checkNereidsExecute("""SHOW COLUMNS FROM ${dbName}.${tableName}""")
        qt_cmd("""SHOW COLUMNS FROM ${dbName}.${tableName}""")

        // Test SHOW FULL COLUMNS
        checkNereidsExecute("""SHOW FULL COLUMNS FROM ${dbName}.${tableName}""")
        qt_cmd("""SHOW FULL COLUMNS FROM ${dbName}.${tableName}""")

        // Test SHOW COLUMNS with LIKE
        checkNereidsExecute("""SHOW COLUMNS FROM ${dbName}.${tableName} LIKE 's%'""")
        qt_cmd("""SHOW COLUMNS FROM ${dbName}.${tableName} LIKE 's%'""")

        // Test SHOW COLUMNS with WHERE clause
        checkNereidsExecute("""SHOW COLUMNS FROM ${dbName}.${tableName} WHERE Field = 'id'""")
        qt_cmd("""SHOW COLUMNS FROM ${dbName}.${tableName} WHERE Field = 'id'""")

        // Test SHOW FULL COLUMNS with WHERE clause
        checkNereidsExecute("""SHOW FULL COLUMNS FROM ${dbName}.${tableName} WHERE Field LIKE '%name%'""")

    } finally {
        sql """DROP TABLE IF EXISTS ${dbName}.${tableName}"""
        sql """DROP DATABASE IF EXISTS ${dbName}"""
    }
}
