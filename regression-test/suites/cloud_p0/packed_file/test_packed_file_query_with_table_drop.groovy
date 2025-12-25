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

suite("test_packed_file_query_with_table_drop", "p0, nonConcurrent") {
    if (!isCloudMode()) {
        return;
    }
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    def backendId_to_backendBrpcPort = [:]
    getBackendIpHttpAndBrpcPort(backendId_to_backendIP, backendId_to_backendHttpPort, backendId_to_backendBrpcPort)

    setBeConfigTemporary([
        "enable_packed_file": "true",
        "small_file_threshold_bytes": "102400"  // 100KB threshold
    ]) {
        // Create main table that will be queried
        def mainTableName = "test_packed_file_main_table"
        sql """ DROP TABLE IF EXISTS ${mainTableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${mainTableName} (
                `k1` int(11) NULL,
                `k2` int(11) NULL,
                `v1` int(11) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`, `k2`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true"
            );
        """

        // Create multiple other tables that will have data deleted
        def otherTableNames = []
        def otherTableCount = 5
        for (int i = 0; i < otherTableCount; i++) {
            def tableName = "test_packed_file_other_table_${i}"
            otherTableNames.add(tableName)
            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """
                CREATE TABLE IF NOT EXISTS ${tableName} (
                    `k1` int(11) NULL,
                    `k2` int(11) NULL,
                    `v1` int(11) NULL
                ) ENGINE=OLAP
                DUPLICATE KEY(`k1`, `k2`)
                DISTRIBUTED BY HASH(`k1`) BUCKETS 1
                PROPERTIES (
                    "replication_allocation" = "tag.location.default: 1",
                    "disable_auto_compaction" = "true"
                );
            """
        }

        // Load data into main table (small files that will trigger packed)
        def mainTableLoadCount = 8
        def mainTableLoadThreads = []
        def loadMainTable = { table_name, thread_id ->
            try {
                for (int i = 0; i < 3; i++) {
                    def data = new StringBuilder()
                    // Generate ~50KB of data per load (smaller than 100KB threshold)
                    for (int j = 0; j < 500; j++) {
                        data.append("${thread_id * 1000 + i * 100 + j},${j},${thread_id * 10000 + i * 1000 + j}\n")
                    }
                    streamLoad {
                        table "${table_name}"
                        set 'column_separator', ','
                        set 'columns', 'k1, k2, v1'
                        inputText data.toString()
                        time 10000
                        check { result, exception, startTime, endTime ->
                            if (exception != null) {
                                throw exception
                            }
                            logger.info("Main table load result (thread ${thread_id}, batch ${i}): ${result}")
                            def json = parseJson(result)
                            assertEquals("success", json.Status.toLowerCase())
                            assertTrue(json.NumberLoadedRows > 0)
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Main table load failed for thread ${thread_id}: ${e.getMessage()}")
                throw e
            }
        }

        // Load data into other tables (small files that will trigger packed)
        def otherTableLoadThreads = []
        def loadOtherTable = { table_name, thread_id ->
            try {
                for (int i = 0; i < 2; i++) {
                    def data = new StringBuilder()
                    // Generate ~50KB of data per load
                    for (int j = 0; j < 500; j++) {
                        data.append("${thread_id * 1000 + i * 100 + j},${j},${thread_id * 10000 + i * 1000 + j}\n")
                    }
                    streamLoad {
                        table "${table_name}"
                        set 'column_separator', ','
                        set 'columns', 'k1, k2, v1'
                        inputText data.toString()
                        time 10000
                        check { result, exception, startTime, endTime ->
                            if (exception != null) {
                                throw exception
                            }
                            logger.info("Other table ${table_name} load result (thread ${thread_id}, batch ${i}): ${result}")
                            def json = parseJson(result)
                            assertEquals("success", json.Status.toLowerCase())
                            assertTrue(json.NumberLoadedRows > 0)
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Other table ${table_name} load failed for thread ${thread_id}: ${e.getMessage()}")
                throw e
            }
        }

        // Start loading data into main table
        for (int i = 0; i < mainTableLoadCount; i++) {
            def thread_id = i
            mainTableLoadThreads.add(Thread.startDaemon {
                loadMainTable(mainTableName, thread_id)
            })
        }

        // Start loading data into other tables
        for (int i = 0; i < otherTableCount; i++) {
            def tableName = otherTableNames[i]
            def thread_id = i
            otherTableLoadThreads.add(Thread.startDaemon {
                loadOtherTable(tableName, thread_id)
            })
        }

        // Wait for all loads to complete
        for (Thread t : mainTableLoadThreads) {
            t.join(120000)
        }
        for (Thread t : otherTableLoadThreads) {
            t.join(120000)
        }

        // Verify main table data before deleting other tables' data
        def expected_main_rows = mainTableLoadCount * 3 * 500  // 8 threads * 3 batches * 500 rows = 12000
        def result_before = sql "select count(*) from ${mainTableName}"
        assertEquals(expected_main_rows, result_before[0][0] as int,
                    "Expected exactly ${expected_main_rows} rows in main table, got ${result_before[0][0]}")

        // Delete data from other tables (not drop tables)
        logger.info("Deleting data from other tables")
        for (def tableName in otherTableNames) {
            sql """ drop table if exists ${tableName} force """
        }

        // Verify main table can still be queried correctly after deleting other tables' data
        def result_after = sql "select count(*) from ${mainTableName}"
        logger.info("Main table row count after deleting other tables' data: ${result_after[0][0]}, expected: ${expected_main_rows}")
        assertEquals(expected_main_rows, result_after[0][0] as int,
                    "After deleting other tables' data, expected exactly ${expected_main_rows} rows in main table, got ${result_after[0][0]}")

    }
}
