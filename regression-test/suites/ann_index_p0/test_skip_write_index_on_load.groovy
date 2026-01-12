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

// Test skip_write_index_on_load property for ANN index
// When enabled, ANN index construction is skipped during data loading
// and built during compaction
//
// Verification strategy using show_nested_index_file API:
// 1. skip_write_index_on_load=true + before compaction: no ann.faiss
// 2. skip_write_index_on_load=true + after compaction: ann.faiss exists
// 3. skip_write_index_on_load=false: ann.faiss exists immediately after insert

suite("test_skip_write_index_on_load") {
    if (isCloudMode()) {
        return
    }

    // Setup backend mappings
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)

    // Helper to call show_nested_index_file API
    def showNestedIndexFile = { ip, port, tabletId ->
        def (code, out, err) = http_client("GET", String.format("http://%s:%s/api/show_nested_index_file?tablet_id=%s", ip, port, tabletId))
        logger.info("show_nested_index_file response: ${out}")
        return out
    }

    // Helper to check if ANN index exists
    def hasAnnIndex = { ip, port, tabletId ->
        def out = showNestedIndexFile(ip, port, tabletId)
        return out.contains("ann.faiss")
    }

    // Helper to check if index file is empty
    def isIndexFileEmpty = { ip, port, tabletId ->
        def out = showNestedIndexFile(ip, port, tabletId)
        return out.contains("is empty")
    }

    // Generate dataset for testing
    def generateInsertValues = { int count, int seed, int startId = 0 ->
        def random = new Random(seed)
        def values = []
        for (int i = 0; i < count; i++) {
            def vec = (0..<8).collect { (random.nextFloat() * 100).round(6) }
            values << "(${startId + i}, [${vec.join(',')}])"
        }
        return values.join(',')
    }

    def tableName = "tbl_skip_ann_index"
    def normalTableName = "tbl_normal_ann_index"

    // ============================================================
    // Case 1: skip_write_index_on_load=true, before compaction
    // Expected: no ann.faiss in index file
    // ============================================================
    logger.info("Case 1: skip_write_index_on_load=true, verify no ANN index before compaction")

    sql "drop table if exists ${tableName}"
    sql """
    CREATE TABLE ${tableName} (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
            "index_type"="hnsw",
            "metric_type"="l2_distance",
            "dim"="8"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES (
        "replication_num" = "1",
        "disable_auto_compaction" = "true",
        "skip_write_index_on_load" = "true"
    );
    """

    // Insert data in multiple batches
    for (int batch = 0; batch < 5; batch++) {
        sql "INSERT INTO ${tableName} VALUES ${generateInsertValues(100, 10000 + batch, batch * 100)}"
    }

    // Get tablet info
    def tablets = sql_return_maparray "SHOW TABLETS FROM ${tableName}"
    def tabletId = tablets[0].TabletId
    def backendId = tablets[0].BackendId
    def beIp = backendId_to_backendIP.get(backendId)
    def bePort = backendId_to_backendHttpPort.get(backendId)
    logger.info("Tablet ID: ${tabletId}, Backend: ${beIp}:${bePort}")

    // Verify: no ANN index before compaction
    assertTrue(isIndexFileEmpty(beIp, bePort, tabletId),
        "Case 1 FAILED: index file should be empty when skip_write_index_on_load=true")
    logger.info("Case 1 PASSED: no ANN index before compaction")

    // ============================================================
    // Case 2: skip_write_index_on_load=true, after compaction
    // Expected: ann.faiss exists in index file
    // ============================================================
    logger.info("Case 2: skip_write_index_on_load=true, verify ANN index after compaction")

    trigger_and_wait_compaction(tableName, "full")

    // Verify: ANN index exists after compaction
    assertTrue(hasAnnIndex(beIp, bePort, tabletId),
        "Case 2 FAILED: ann.faiss should exist after compaction")
    logger.info("Case 2 PASSED: ANN index exists after compaction")

    // ============================================================
    // Case 3: skip_write_index_on_load=false (default)
    // Expected: ann.faiss exists immediately after insert
    // ============================================================
    logger.info("Case 3: skip_write_index_on_load=false, verify ANN index exists after insert")

    sql "drop table if exists ${normalTableName}"
    sql """
    CREATE TABLE ${normalTableName} (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
            "index_type"="hnsw",
            "metric_type"="l2_distance",
            "dim"="8"
        )
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES (
        "replication_num" = "1",
        "disable_auto_compaction" = "true",
        "skip_write_index_on_load" = "false"
    );
    """

    // Insert same data
    for (int batch = 0; batch < 5; batch++) {
        sql "INSERT INTO ${normalTableName} VALUES ${generateInsertValues(100, 10000 + batch, batch * 100)}"
    }

    // Get tablet info
    def normalTablets = sql_return_maparray "SHOW TABLETS FROM ${normalTableName}"
    def normalTabletId = normalTablets[0].TabletId
    def normalBackendId = normalTablets[0].BackendId
    def normalBeIp = backendId_to_backendIP.get(normalBackendId)
    def normalBePort = backendId_to_backendHttpPort.get(normalBackendId)
    logger.info("Normal table Tablet ID: ${normalTabletId}, Backend: ${normalBeIp}:${normalBePort}")

    // Verify: ANN index exists immediately after insert
    assertTrue(hasAnnIndex(normalBeIp, normalBePort, normalTabletId),
        "Case 3 FAILED: ann.faiss should exist immediately when skip_write_index_on_load=false")
    logger.info("Case 3 PASSED: ANN index exists immediately after insert")

    // Cleanup
    sql "drop table if exists ${tableName}"
    sql "drop table if exists ${normalTableName}"

    logger.info("All test cases passed!")
}
