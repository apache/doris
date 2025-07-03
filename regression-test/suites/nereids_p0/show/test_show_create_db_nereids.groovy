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

suite("test_show_create_db_nereids", "query,create_database") {
    String dbName = "db_test_show_create";

    try {
        // Create a new database to test the SHOW CREATE DATABASE command
        sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
        
        // Run the SHOW CREATE DATABASE command and validate the output using checkNereidsExecute and qt_cmd
        checkNereidsExecute("""SHOW CREATE DATABASE ${dbName}""")
        qt_cmd("""SHOW CREATE DATABASE ${dbName}""")

        // Drop the database and verify that the command runs successfully
        sql "DROP DATABASE IF EXISTS ${dbName}"
        
        // Re-create the database with additional properties
        sql "CREATE DATABASE IF NOT EXISTS ${dbName} PROPERTIES ('property_key'='property_value')"
        
        // Verify the SHOW CREATE DATABASE command captures the properties using checkNereidsExecute and qt_cmd
        checkNereidsExecute("""SHOW CREATE DATABASE ${dbName}""")
        qt_cmd("""SHOW CREATE DATABASE ${dbName}""")
    } finally {
        // Clean up by dropping the database if it still exists
        try_sql("DROP DATABASE IF EXISTS ${dbName}")
    }
}
