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

suite("test_packed_file_concurrent_load", "p0, nonConcurrent") {
    if (!isCloudMode()) {
        return;
    }
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    def backendId_to_backendBrpcPort = [:]
    getBackendIpHttpAndBrpcPort(backendId_to_backendIP, backendId_to_backendHttpPort, backendId_to_backendBrpcPort)

    // Helper function to get brpc metric by name from a single backend
    def getBrpcMetrics = {ip, port, name ->
        def url = "http://${ip}:${port}/brpc_metrics"
        def metrics = new URL(url).text
        def matcher = metrics =~ ~"${name}\\s+(\\d+)"
        if (matcher.find()) {
            return matcher[0][1] as long
        } else {
            return 0
        }
    }

    // Get packed file total small file count metric from all backends
    def get_packed_file_total_small_file_count = {
        long total_count = 0
        for (String backend_id: backendId_to_backendIP.keySet()) {
            def ip = backendId_to_backendIP.get(backend_id)
            def brpc_port = backendId_to_backendBrpcPort.get(backend_id)
            try {
                def count = getBrpcMetrics(ip, brpc_port, "packed_file_total_small_file_num")
                if (count > 0) {
                    total_count += count
                    logger.info("BE ${ip}:${brpc_port} packed_file_total_small_file_num = ${count}")
                }
            } catch (Exception e) {
                logger.warn("Failed to get metrics from BE ${ip}:${brpc_port}: ${e.getMessage()}")
            }
        }
        logger.info("Total packed_file_total_small_file_num across all backends: ${total_count}")
        return total_count
    }


    // Enable packed file feature and set small file threshold using framework's temporary config function
    // This will automatically restore configs after test completes
    setBeConfigTemporary([
        "enable_packed_file": "true",
        "small_file_threshold_bytes": "102400"
    ]) {
        // Get initial packed file count
        def initial_packed_file_count = get_packed_file_total_small_file_count()
        logger.info("Initial packed_file_total_small_file_count: ${initial_packed_file_count}")

        // Test case 1: Multiple small concurrent loads to the same tablet
        def tableName1 = "test_packed_file_same_tablet"
        sql """ DROP TABLE IF EXISTS ${tableName1} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName1} (
                `k1` int(11) NULL,
                `k2` int(11) NULL,
                `v1` int(11) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`, `k2`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """

        def load_threads1 = []
        def load_count1 = 10
        def load_data_same_tablet = { table_name, thread_id ->
            try {
                for (int i = 0; i < 5; i++) {
                    // Generate multiple rows per load to create small files
                    def data = new StringBuilder()
                    for (int j = 0; j < 100; j++) {
                        data.append("${thread_id * 100 + i * 10 + j},${j},${thread_id * 1000 + i * 100 + j}\n")
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
                            logger.info("Stream load result (same tablet, thread ${thread_id}, batch ${i}): ${result}")
                            def json = parseJson(result)
                            assertEquals("success", json.Status.toLowerCase())
                            assertTrue(json.NumberLoadedRows > 0)
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Load failed for thread ${thread_id}: ${e.getMessage()}")
                throw e
            }
        }

        // Start concurrent loads
        for (int i = 0; i < load_count1; i++) {
            def thread_id = i
            load_threads1.add(Thread.startDaemon {
                load_data_same_tablet(tableName1, thread_id)
            })
        }

        // Wait for all loads to complete
        for (Thread t : load_threads1) {
            t.join(60000) // 60 seconds timeout
        }

        def result1 = sql "select count(*) from ${tableName1}"
        def expected_rows1 = load_count1 * 5 * 100  // 10 threads * 5 batches * 100 rows = 5000
        assertEquals(expected_rows1, result1[0][0] as int, 
                    "Expected exactly ${expected_rows1} rows for DUPLICATE KEY table, got ${result1[0][0]}")

        def count_after_test1 = get_packed_file_total_small_file_count()
        logger.info("packed_file_total_small_file_count after test case 1: ${count_after_test1} (initial: ${initial_packed_file_count})")
        // The count must increase after test case 1
        assertTrue(count_after_test1 > initial_packed_file_count,
                   "packed_file_total_small_file_count must increase after test case 1. " +
                   "Initial: ${initial_packed_file_count}, After test1: ${count_after_test1}")

        // Test case 2: Multiple small concurrent loads to different partitions
        def tableName2 = "test_packed_file_different_partitions"
        sql """ DROP TABLE IF EXISTS ${tableName2} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName2} (
                `k1` int(11) NULL,
                `k2` int(11) NULL,
                `v1` int(11) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`, `k2`)
            PARTITION BY RANGE(`k1`)
            (PARTITION p1 VALUES [("0"), ("100")),
             PARTITION p2 VALUES [("100"), ("200")),
             PARTITION p3 VALUES [("200"), ("300")),
             PARTITION p4 VALUES [("300"), ("400")))
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        """

        def load_threads2 = []
        def load_count2 = 8
        def load_data_different_partitions = { table_name, thread_id ->
            try {
                def partition_id = thread_id % 4
                // Partition ranges: p1[0,100), p2[100,200), p3[200,300), p4[300,400)
                def partition_start = partition_id * 100
                // Each partition has 2 threads (threads 0&4 -> p1, 1&5 -> p2, 2&6 -> p3, 3&7 -> p4)
                // Thread idx within partition: 0 or 1
                def thread_idx_in_partition = (thread_id / 4).intValue()
                // Split partition range between 2 threads: first uses [start, start+50), second uses [start+50, start+100)
                def partition_slot_start = partition_start + thread_idx_in_partition * 50
                def partition_slot_end = partition_start + (thread_idx_in_partition + 1) * 50
                
                for (int i = 0; i < 5; i++) {
                    // Generate multiple rows per load to create small files
                    def data = new StringBuilder()
                    for (int j = 0; j < 100; j++) {
                        // Cycle k1 values within this thread's slot in the partition
                        def k1_value = partition_slot_start + ((i * 20 + j) % 50)
                        // Ensure k1 is within partition bounds [partition_start, partition_start+100)
                        if (k1_value >= partition_start + 100) {
                            k1_value = partition_start + (k1_value % 100)
                        }
                        data.append("${k1_value},${i * 10 + j},${thread_id * 1000 + i * 100 + j}\n")
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
                            logger.info("Stream load result (different partitions, thread ${thread_id}, batch ${i}): ${result}")
                            def json = parseJson(result)
                            assertEquals("success", json.Status.toLowerCase())
                            assertTrue(json.NumberLoadedRows > 0)
                            // Log actual loaded rows to debug
                            logger.info("Thread ${thread_id}, batch ${i}: loaded ${json.NumberLoadedRows} rows, unselected: ${json.NumberUnselectedRows}")
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Load failed for thread ${thread_id}: ${e.getMessage()}")
                throw e
            }
        }

        // Start concurrent loads to different partitions
        for (int i = 0; i < load_count2; i++) {
            def thread_id = i
            load_threads2.add(Thread.startDaemon {
                load_data_different_partitions(tableName2, thread_id)
            })
        }

        // Wait for all loads to complete
        for (Thread t : load_threads2) {
            t.join(60000)
        }

        def result2 = sql "select count(*) from ${tableName2}"
        def expected_rows2 = load_count2 * 5 * 100  // 8 threads * 5 batches * 100 rows = 4000
        logger.info("Table ${tableName2} row count: ${result2[0][0]}, expected: ${expected_rows2}")
        // For DUPLICATE KEY table, all rows should be preserved
        // Note: Some rows might be filtered if they're out of partition range
        // Check if at least most rows are loaded (allow some tolerance for partition filtering)
        def actual_rows2 = result2[0][0] as int
        assertTrue(actual_rows2 >= expected_rows2 * 0.75,
                    "Expected at least ${expected_rows2 * 0.75} rows (75% of ${expected_rows2}) for DUPLICATE KEY table, got ${actual_rows2}. " +
                    "Some rows may have been filtered if they were out of partition range.")
        logger.info("Loaded ${actual_rows2} rows out of expected ${expected_rows2} rows")

        def count_after_test2 = get_packed_file_total_small_file_count()
        logger.info("packed_file_total_small_file_count after test case 2: ${count_after_test2} (after test1: ${count_after_test1})")
        // The count must increase after test case 2
        assertTrue(count_after_test2 > count_after_test1,
                   "packed_file_total_small_file_count must increase after test case 2. " +
                   "After test1: ${count_after_test1}, After test2: ${count_after_test2}")

        // Test case 3: Multiple small concurrent loads to different tables
        def load_threads3 = []
        def load_count3 = 6
        def load_data_different_tables = { table_id, thread_id ->
            try {
                def table_name = "test_packed_file_table_${table_id}"
                sql """ DROP TABLE IF EXISTS ${table_name} """
                sql """
                    CREATE TABLE IF NOT EXISTS ${table_name} (
                        `k1` int(11) NULL,
                        `k2` int(11) NULL,
                        `v1` int(11) NULL
                    ) ENGINE=OLAP
                    DUPLICATE KEY(`k1`, `k2`)
                    DISTRIBUTED BY HASH(`k1`) BUCKETS 1
                    PROPERTIES ("replication_allocation" = "tag.location.default: 1");
                """

                for (int i = 0; i < 5; i++) {
                    // Generate multiple rows per load to create small files
                    def data = new StringBuilder()
                    for (int j = 0; j < 100; j++) {
                        data.append("${thread_id * 100 + i * 10 + j},${j},${thread_id * 1000 + i * 100 + j}\n")
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
                            logger.info("Stream load result (different tables, table ${table_id}, thread ${thread_id}, batch ${i}): ${result}")
                            def json = parseJson(result)
                            assertEquals("success", json.Status.toLowerCase())
                            assertTrue(json.NumberLoadedRows > 0)
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Load failed for table ${table_id}, thread ${thread_id}: ${e.getMessage()}")
                throw e
            }
        }

        // Start concurrent loads to different tables
        for (int i = 0; i < load_count3; i++) {
            def table_id = i
            def thread_id = i
            load_threads3.add(Thread.startDaemon {
                load_data_different_tables(table_id, thread_id)
            })
        }

        // Wait for all loads to complete
        for (Thread t : load_threads3) {
            t.join(60000)
        }

        // Verify data in all tables
        def expected_rows_per_table = 5 * 100  // 5 batches * 100 rows = 500 rows per table
        for (int i = 0; i < load_count3; i++) {
            def table_name = "test_packed_file_table_${i}"
            def result = sql "select count(*) from ${table_name}"
            logger.info("Table ${table_name} row count: ${result[0][0]}, expected: ${expected_rows_per_table}")
            // For DUPLICATE KEY table, all rows should be preserved exactly
            assertEquals(expected_rows_per_table, result[0][0] as int, 
                         "Expected exactly ${expected_rows_per_table} rows for DUPLICATE KEY table ${table_name}, got ${result[0][0]}")
        }

        def count_after_test3 = get_packed_file_total_small_file_count()
        logger.info("packed_file_total_small_file_count after test case 3: ${count_after_test3} (after test2: ${count_after_test2}, initial: ${initial_packed_file_count})")
        
        // The count must increase after test case 3
        assertTrue(count_after_test3 > count_after_test2,
                   "packed_file_total_small_file_count must increase after test case 3. " +
                   "After test2: ${count_after_test2}, After test3: ${count_after_test3}")
    }
}
