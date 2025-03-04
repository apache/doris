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

suite("test_show_databases_command", "nereids_p0") {
    try {
        // Create test databases
        sql("CREATE DATABASE IF NOT EXISTS test_show_databases_db1")
        sql("CREATE DATABASE IF NOT EXISTS test_show_databases_db2")
        sql("CREATE DATABASE IF NOT EXISTS test_show_databases_db3")

        // Run and verify SHOW DATABASES
        checkNereidsExecute("SHOW DATABASES")

        // Run and verify SHOW DATABASES LIKE
        checkNereidsExecute("SHOW DATABASES LIKE 'test_show_databases_db%'")
        qt_cmd("SHOW DATABASES LIKE 'test_show_databases_db%'")

    } finally {
        // Clean up
        sql("DROP DATABASE IF EXISTS test_show_databases_db1")
        sql("DROP DATABASE IF EXISTS test_show_databases_db2")
        sql("DROP DATABASE IF EXISTS test_show_databases_db3")
    }
}
