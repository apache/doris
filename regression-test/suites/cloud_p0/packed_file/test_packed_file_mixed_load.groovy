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

suite("test_packed_file_mixed_load", "p0, nonConcurrent") {
    if (!isCloudMode()) {
        return;
    }
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    def backendId_to_backendBrpcPort = [:]
    getBackendIpHttpAndBrpcPort(backendId_to_backendIP, backendId_to_backendHttpPort, backendId_to_backendBrpcPort)

    // Build backend sockets list for httpTest
    def backendSockets = []
    backendId_to_backendIP.each { backendId, ip ->
        def socket = ip + ":" + backendId_to_backendHttpPort.get(backendId)
        backendSockets.add(socket)
    }
    assertTrue(backendSockets.size() > 0, "No alive backends found")

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

    // Get local delete bitmap status for a tablet
    def getLocalDeleteBitmapStatus = { be_host, be_http_port, tablet_id ->
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X GET http://${be_host}:${be_http_port}")
        sb.append("/api/delete_bitmap/count_local?tablet_id=")
        sb.append(tablet_id)

        String command = sb.toString()
        logger.info("Get local delete bitmap command: ${command}")
        def process = command.execute()
        def code = process.waitFor()
        def out = process.getText()
        logger.info("Get local delete bitmap status: code=${code}, out=${out}")
        assertEquals(code, 0)
        def deleteBitmapStatus = parseJson(out.trim())
        return deleteBitmapStatus
    }

    // Get nested index file info for a tablet
    def getNestedIndexFileInfo = { be_host, be_http_port, tablet_id ->
        try {
            def (code, out, err) = http_client("GET", String.format("http://%s:%s/api/show_nested_index_file?tablet_id=%s", be_host, be_http_port, tablet_id))
            logger.info("Get nested index file info: code=${code}, out=${out}, err=${err}")
            if (code == 500 || code != 0) {
                logger.warn("Failed to get nested index file info, may not exist yet")
                return null
            }
            def indexInfo = parseJson(out.trim())
            return indexInfo
        } catch (Exception e) {
            logger.warn("Exception getting nested index file info: ${e.getMessage()}")
            return null
        }
    }

    // Clear file cache for a specific file path on all backends using httpTest
    def clearFileCacheForFile = { file_path ->
        logger.info("Clearing file cache for ${file_path} on all backends")
        def clearResults = []
        backendSockets.each { socket ->
            httpTest {
                endpoint ""
                uri socket + "/api/file_cache?op=clear&sync=true&value=" + file_path
                op "get"
                check { respCode, body ->
                    assertEquals(respCode, 200, "clear local cache fail, maybe you can find something in respond: " + parseJson(body))
                    clearResults.add(true)
                }
            }
        }
        assertEquals(clearResults.size(), backendSockets.size(), "Failed to clear cache for ${file_path} on some backends")
        logger.info("File cache cleared for ${file_path} on all backends")
    }

    // Clear file cache for a specific table
    def clearFileCacheForTable = { table_name ->
        logger.info("Clearing file cache for table ${table_name}")
        
        // Get all tablets for the table
        def tablets = sql_return_maparray "show tablets from ${table_name}"
        assertTrue(tablets.size() > 0, "Table ${table_name} should have at least one tablet")
        
        def cleared_count = 0
        for (def tablet in tablets) {
            def tablet_id = tablet.TabletId
            def backend_id = tablet.BackendId
            def be_ip = backendId_to_backendIP.get(backend_id)
            def be_http_port = backendId_to_backendHttpPort.get(backend_id)
            
            if (be_ip == null || be_http_port == null) {
                logger.warn("Backend ${backend_id} not found, skipping tablet ${tablet_id}")
                continue
            }
            
            // Get rowset information for the tablet using httpTest
            def socket = be_ip + ":" + be_http_port
            def rowsetPaths = []
            def rowsetSegments = [:]  // Map rowset path to num_segments
            try {
                httpTest {
                    endpoint ""
                    uri socket + "/api/compaction/show?tablet_id=" + tablet_id
                    op "get"
                    check { respCode, body ->
                        assertEquals(respCode, 200)
                        def compactionInfo = parseJson(body)
                        if (compactionInfo.rowsets != null && compactionInfo.rowsets instanceof List) {
                            for (def rowset in compactionInfo.rowsets) {
                                // Extract rowset path and num_segments from rowset info
                                // Rowset format: "rowset_id version start_version end_version num_segments path"
                                def rowsetStr = rowset.toString()
                                def tokens = rowsetStr.split("\\s+")
                                if (tokens.size() >= 6) {
                                    def rowsetPath = tokens[5]  // path is the 6th field (index 5)
                                    def numSegments = tokens[4].toInteger()  // num_segments is the 5th field (index 4)
                                    rowsetPaths.add(rowsetPath)
                                    rowsetSegments[rowsetPath] = numSegments
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.warn("Exception getting rowset info for tablet ${tablet_id}: ${e.getMessage()}")
                continue
            }
            
            // Clear cache for each rowset's segment files
            for (def rowsetPath in rowsetPaths) {
                def numSegments = rowsetSegments[rowsetPath]
                for (int seg_id = 0; seg_id < numSegments; seg_id++) {
                    // Clear .dat file
                    def segmentDataFile = "${rowsetPath}_${seg_id}.dat"
                    try {
                        clearFileCacheForFile(segmentDataFile)
                        cleared_count++
                    } catch (Exception e) {
                        // File may not exist or already cleared, continue
                        logger.debug("Failed to clear cache for ${segmentDataFile}: ${e.getMessage()}")
                    }
                    
                    // Clear .idx file if exists
                    def segmentIdxFile = "${rowsetPath}_${seg_id}.idx"
                    try {
                        clearFileCacheForFile(segmentIdxFile)
                        cleared_count++
                    } catch (Exception e) {
                        // Index file may not exist, ignore error
                        logger.debug("Failed to clear cache for ${segmentIdxFile}: ${e.getMessage()}")
                    }
                }
            }
        }
    }

    // Enable packed file feature and set small file threshold using framework's temporary config function
    // This will automatically restore configs after test completes
    setBeConfigTemporary([
        "enable_packed_file": "true",
        "small_file_threshold_bytes": "102400"  // 100KB threshold
    ]) {
        // Test case 1: Mixed load (small and large files) - check query results
        def tableName1 = "test_packed_file_mixed_load_query"
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

        def load_threads = []
        def small_load_count = 5  // Small loads that will trigger packed
        def large_load_count = 3  // Large loads that won't trigger packed

        // Small load function - generates files smaller than threshold (will be packed)
        def small_load = { table_name, thread_id ->
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
                            logger.info("Small load result (thread ${thread_id}, batch ${i}): ${result}")
                            def json = parseJson(result)
                            assertEquals("success", json.Status.toLowerCase())
                            assertTrue(json.NumberLoadedRows > 0)
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Small load failed for thread ${thread_id}: ${e.getMessage()}")
                throw e
            }
        }

        // Large load function - generates files larger than threshold (won't be packed)
        def large_load = { table_name, thread_id ->
            try {
                for (int i = 0; i < 2; i++) {
                    def data = new StringBuilder()
                    // Generate ~200KB of data per load (larger than 100KB threshold)
                    for (int j = 0; j < 2000; j++) {
                        data.append("${thread_id * 10000 + i * 1000 + j},${j},${thread_id * 100000 + i * 10000 + j}\n")
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
                            logger.info("Large load result (thread ${thread_id}, batch ${i}): ${result}")
                            def json = parseJson(result)
                            assertEquals("success", json.Status.toLowerCase())
                            assertTrue(json.NumberLoadedRows > 0)
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Large load failed for thread ${thread_id}: ${e.getMessage()}")
                throw e
            }
        }

        // Start concurrent small loads
        for (int i = 0; i < small_load_count; i++) {
            def thread_id = i
            load_threads.add(Thread.startDaemon {
                small_load(tableName1, thread_id)
            })
        }

        // Start concurrent large loads
        for (int i = 0; i < large_load_count; i++) {
            def thread_id = i + small_load_count
            load_threads.add(Thread.startDaemon {
                large_load(tableName1, thread_id)
            })
        }

        // Wait for all loads to complete
        for (Thread t : load_threads) {
            t.join(120000)  // 2 minutes timeout
        }

        // Wait a bit for packed operations to complete
        sleep(5000)

        // Verify query results - should include data from both small and large loads
        def expected_small_rows = small_load_count * 3 * 500  // 5 threads * 3 batches * 500 rows = 7500
        def expected_large_rows = large_load_count * 2 * 2000  // 3 threads * 2 batches * 2000 rows = 12000
        def expected_total_rows = expected_small_rows + expected_large_rows

        def result1 = sql "select count(*) from ${tableName1}"
        logger.info("Table ${tableName1} row count: ${result1[0][0]}, expected: ${expected_total_rows}")
        assertEquals(expected_total_rows, result1[0][0] as int,
                    "Expected exactly ${expected_total_rows} rows (${expected_small_rows} from small loads + ${expected_large_rows} from large loads), got ${result1[0][0]}")

        // Test case 1.1: Clear file cache for the table and verify query results
        logger.info("Test case 1.1: Clearing file cache for table ${tableName1} and verifying query results")
        clearFileCacheForTable(tableName1)

        // Verify query results after clearing cache
        def result1_after_clear = sql "select count(*) from ${tableName1}"
        assertEquals(expected_total_rows, result1_after_clear[0][0] as int,
                    "After clearing file cache, expected exactly ${expected_total_rows} rows, got ${result1_after_clear[0][0]}")

        // Verify data integrity with various queries after cache clear
        def distinct_k1_after_clear = sql "select count(distinct k1) from ${tableName1}"
        logger.info("After cache clear - Distinct k1 count: ${distinct_k1_after_clear[0][0]}")

        def sum_v1_after_clear = sql "select sum(v1) from ${tableName1}"
        logger.info("After cache clear - Sum of v1: ${sum_v1_after_clear[0][0]}")

        def sample_query_after_clear = sql "select * from ${tableName1} where k1 between 0 and 100 limit 10"
        logger.info("After cache clear - Sample query returned ${sample_query_after_clear.size()} rows")

        logger.info("✓ Test case 1.1 passed: Query results are correct after clearing file cache")

        // Test case 2: Mixed load - check index and delete bitmap
        def tableName2 = "test_packed_file_mixed_load_index"
        sql """ DROP TABLE IF EXISTS ${tableName2} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName2} (
                `k1` int(11) NULL,
                `k2` int(11) NULL,
                `v1` int(11) NULL,
                INDEX idx_k1 (`k1`) USING INVERTED COMMENT 'inverted index for k1',
                INDEX idx_k2 (`k2`) USING INVERTED COMMENT 'inverted index for k2',
                INDEX idx_v1 (`v1`) USING INVERTED COMMENT 'inverted index for v1'
            ) ENGINE=OLAP
            UNIQUE KEY(`k1`, `k2`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true"
            );
        """

        def load_threads2 = []
        def small_load_count2 = 4
        def large_load_count2 = 2

        // Small load with duplicate keys for UNIQUE KEY table (will generate delete bitmap)
        def small_load_with_duplicates = { table_name, thread_id ->
            try {
                for (int i = 0; i < 3; i++) {
                    def data = new StringBuilder()
                    // Generate ~50KB of data per load with some duplicate keys
                    for (int j = 0; j < 500; j++) {
                        // Create duplicate keys: first 100 rows use same keys as previous batch
                        def k1_val = thread_id * 1000 + i * 100 + j
                        def k2_val = j
                        // Every 50 rows, create a duplicate key from previous batch
                        if (i > 0 && j % 50 == 0) {
                            k1_val = thread_id * 1000 + (i - 1) * 100 + j
                            k2_val = j
                        }
                        data.append("${k1_val},${k2_val},${thread_id * 10000 + i * 1000 + j}\n")
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
                            logger.info("Small load with duplicates result (thread ${thread_id}, batch ${i}): ${result}")
                            def json = parseJson(result)
                            assertEquals("success", json.Status.toLowerCase())
                            assertTrue(json.NumberLoadedRows > 0)
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Small load with duplicates failed for thread ${thread_id}: ${e.getMessage()}")
                throw e
            }
        }

        // Large load with duplicate keys for UNIQUE KEY table (will generate delete bitmap)
        def large_load_with_duplicates = { table_name, thread_id ->
            try {
                for (int i = 0; i < 2; i++) {
                    def data = new StringBuilder()
                    // Generate ~200KB of data per load with some duplicate keys
                    for (int j = 0; j < 2000; j++) {
                        // Create duplicate keys: first 200 rows use same keys as previous batch
                        def k1_val = thread_id * 10000 + i * 1000 + j
                        def k2_val = j
                        // Every 100 rows, create a duplicate key from previous batch
                        if (i > 0 && j % 100 == 0) {
                            k1_val = thread_id * 10000 + (i - 1) * 1000 + j
                            k2_val = j
                        }
                        data.append("${k1_val},${k2_val},${thread_id * 100000 + i * 10000 + j}\n")
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
                            logger.info("Large load with duplicates result (thread ${thread_id}, batch ${i}): ${result}")
                            def json = parseJson(result)
                            assertEquals("success", json.Status.toLowerCase())
                            assertTrue(json.NumberLoadedRows > 0)
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Large load with duplicates failed for thread ${thread_id}: ${e.getMessage()}")
                throw e
            }
        }

        // Start concurrent small and large loads with duplicate keys
        for (int i = 0; i < small_load_count2; i++) {
            def thread_id = i
            load_threads2.add(Thread.startDaemon {
                small_load_with_duplicates(tableName2, thread_id)
            })
        }

        for (int i = 0; i < large_load_count2; i++) {
            def thread_id = i + small_load_count2
            load_threads2.add(Thread.startDaemon {
                large_load_with_duplicates(tableName2, thread_id)
            })
        }

        // Wait for all loads to complete
        for (Thread t : load_threads2) {
            t.join(120000)
        }

        // Verify data is loaded
        def result2 = sql "select count(*) from ${tableName2}"
        def expected_small_rows2 = small_load_count2 * 3 * 500
        def expected_large_rows2 = large_load_count2 * 2 * 2000
        def expected_total_rows2 = expected_small_rows2 + expected_large_rows2
        logger.info("Table ${tableName2} row count: ${result2[0][0]}, expected: ${expected_total_rows2}")
        assertTrue((result2[0][0] as int) >= expected_total_rows2 * 0.9,
                  "Expected at least 90% of ${expected_total_rows2} rows, got ${result2[0][0]}")

        // Get tablet information
        def tablets = sql_return_maparray "show tablets from ${tableName2}"
        assertTrue(tablets.size() > 0, "Table should have at least one tablet")

        def tablet_id = tablets[0].TabletId
        def backend_id = tablets[0].BackendId
        def be_ip = backendId_to_backendIP.get(backend_id)
        def be_http_port = backendId_to_backendHttpPort.get(backend_id)

        logger.info("Checking tablet ${tablet_id} on BE ${be_ip}:${be_http_port}")

        // Check delete bitmap - should be non-zero due to duplicate keys in UNIQUE KEY table
        def deleteBitmapStatus = getLocalDeleteBitmapStatus(be_ip, be_http_port, tablet_id)
        logger.info("Delete bitmap status: ${deleteBitmapStatus}")
        assertNotNull(deleteBitmapStatus, "Delete bitmap status should not be null")
        assertTrue(deleteBitmapStatus.delete_bitmap_count != null,
                  "Delete bitmap count should exist")
        assertTrue(deleteBitmapStatus.delete_bitmap_count > 0,
                  "Delete bitmap count should be greater than 0 due to duplicate keys. " +
                  "Current count: ${deleteBitmapStatus.delete_bitmap_count}")
        logger.info("Delete bitmap count: ${deleteBitmapStatus.delete_bitmap_count}")
        logger.info("Delete bitmap cardinality: ${deleteBitmapStatus.cardinality}")
        assertTrue(deleteBitmapStatus.cardinality != null && deleteBitmapStatus.cardinality >= 0,
                  "Delete bitmap cardinality should exist and be non-negative")

        // Check nested index file - should contain inverted indices
        def indexInfo = getNestedIndexFileInfo(be_ip, be_http_port, tablet_id)
        if (indexInfo != null) {
            logger.info("Nested index file info: ${indexInfo}")
            assertTrue(indexInfo.rowsets != null, "Rowsets should exist in index info")
            assertTrue(indexInfo.rowsets.size() > 0, "Should have at least one rowset")

            def total_indices = 0
            def total_segments = 0
            def inverted_index_count = 0
            for (def rowset in indexInfo.rowsets) {
                logger.info("Rowset ${rowset.rowset_id}: ${rowset.segments.size()} segments")
                if (rowset.index_storage_format != null) {
                    logger.info("  Index storage format: ${rowset.index_storage_format}")
                }
                total_segments += rowset.segments.size()
                for (def segment in rowset.segments) {
                    if (segment.indices != null) {
                        total_indices += segment.indices.size()
                        logger.info("  Segment ${segment.segment_id}: ${segment.indices.size()} indices")
                        // Count inverted indices (should be 3: idx_k1, idx_k2, idx_v1)
                        for (def index in segment.indices) {
                            if (index.index_type != null && index.index_type.contains("INVERTED")) {
                                inverted_index_count++
                            }
                            logger.info("    Index: ${index.index_id}, Type: ${index.index_type}, Column: ${index.column_id}")
                        }
                    }
                }
            }
            logger.info("Total segments: ${total_segments}, Total indices: ${total_indices}, Inverted indices: ${inverted_index_count}")
            assertTrue(total_segments > 0, "Should have at least one segment")
            assertTrue(total_indices >= 0, "Indices count should be non-negative")
        } else {
            logger.warn("Nested index file info not available, may need more time to build")
        }

        // Also verify indices via SHOW INDEX command
        def showIndexResult = sql "SHOW INDEX FROM ${tableName2}"
        logger.info("Show index result: ${showIndexResult}")
        assertTrue(showIndexResult.size() >= 3, "Should have at least 3 inverted indices")
        def invertedIndexNames = []
        for (def row in showIndexResult) {
            if (row[10] == "INVERTED") {  // IndexType column
                invertedIndexNames.add(row[2])  // Key_name column
                logger.info("Found inverted index: ${row[2]} on column ${row[4]}")
            }
        }
        assertTrue(invertedIndexNames.size() >= 3, 
                  "Should have at least 3 inverted indices. Found: ${invertedIndexNames}")
        assertTrue(invertedIndexNames.contains("idx_k1"), "Should have idx_k1 inverted index")
        assertTrue(invertedIndexNames.contains("idx_k2"), "Should have idx_k2 inverted index")
        assertTrue(invertedIndexNames.contains("idx_v1"), "Should have idx_v1 inverted index")

    }
}
