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

suite("test_show_diagnose_tablet_nereids", "query,diagnose") {
    String tableName = "test_table_for_diagnosis";
    String tabletId = "";
    try {
        // Create a new table to test the SHOW TABLET DIAGNOSIS command
        sql "CREATE TABLE IF NOT EXISTS ${tableName} (id INT, name STRING) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3 PROPERTIES('replication_num'='1');"

        // Extract tablet ID from the created table
        def showTabletsResult = sql "SHOW TABLETS FROM ${tableName}"
        assert showTabletsResult.size() > 0
        tabletId = showTabletsResult[0][0]  // Assuming the first tablet ID is used

        // Execute the SHOW TABLET DIAGNOSIS command and verify the output
        checkNereidsExecute("SHOW TABLET DIAGNOSIS ${tabletId}")
        // Execute the SHOW TABLET DIAGNOSIS command and verify the output
        checkNereidsExecute("ADMIN DIAGNOSE TABLET ${tabletId}")
    } catch (Exception e) {
        log.error("Failed to execute SHOW TABLET DIAGNOSIS command", e)
        throw e
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}

