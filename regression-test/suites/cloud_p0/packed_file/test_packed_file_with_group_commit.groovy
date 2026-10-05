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

suite("test_packed_file_with_group_commit", "p0, nonConcurrent") {
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

        // Test case: Packed file with group commit enabled
        // This test verifies that packed file logic works correctly when group commit is enabled
        def tableName = "test_packed_file_with_group_commit"
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
                "group_commit_interval_ms" = "200"
            );
        """

        def count_before_test = get_packed_file_total_small_file_count()
        logger.info("packed_file_total_small_file_count before test (with group commit): ${count_before_test}")

        def load_threads = []
        def load_count = 8
        def load_data_with_group_commit = { table_name, thread_id ->
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
                        set 'group_commit', 'async_mode'
                        unset 'label'
                        inputText data.toString()
                        time 10000
                        check { result, exception, startTime, endTime ->
                            if (exception != null) {
                                throw exception
                            }
                            logger.info("Stream load result (with group commit, thread ${thread_id}, batch ${i}): ${result}")
                            def json = parseJson(result)
                            assertEquals("success", json.Status.toLowerCase())
                            assertTrue(json.NumberLoadedRows > 0)
                            // Verify group commit is enabled
                            assertTrue(json.GroupCommit != null && json.GroupCommit,
                                       "Group commit should be enabled. Result: ${result}")
                            assertTrue(json.Label.startsWith("group_commit_"),
                                       "Label should start with 'group_commit_'. Label: ${json.Label}")
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Load failed for thread ${thread_id}: ${e.getMessage()}")
                throw e
            }
        }

        // Start concurrent loads with group commit enabled
        for (int i = 0; i < load_count; i++) {
            def thread_id = i
            load_threads.add(Thread.startDaemon {
                load_data_with_group_commit(tableName, thread_id)
            })
        }

        // Wait for all loads to complete
        for (Thread t : load_threads) {
            t.join(60000)
        }

        // Wait a bit for group commit to finish and packed operations to complete
        sleep(5000)

        def result = sql "select count(*) from ${tableName}"
        def expected_rows = load_count * 5 * 100  // 8 threads * 5 batches * 100 rows = 4000
        assertEquals(expected_rows, result[0][0] as int, 
                    "Expected exactly ${expected_rows} rows for DUPLICATE KEY table with group commit, got ${result[0][0]}")

        def count_after_test = get_packed_file_total_small_file_count()
        logger.info("packed_file_total_small_file_count after test (with group commit): ${count_after_test} (before: ${count_before_test})")
        
        // The count must increase after test, verifying that packed file works with group commit
        assertTrue(count_after_test > count_before_test,
                   "packed_file_total_small_file_count must increase after test (with group commit). " +
                   "Before: ${count_before_test}, After: ${count_after_test}. " +
                   "This verifies that packed file logic works correctly when group commit is enabled.")
    }
}
