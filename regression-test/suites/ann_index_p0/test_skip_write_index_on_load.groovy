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
// and built during compaction or via BUILD INDEX
//
// Verification strategy using show_nested_index_file API:
// 1. skip_write_index_on_load=true + before compaction: no ann.faiss
// 2. skip_write_index_on_load=true + after compaction: ann.faiss exists
// 3. skip_write_index_on_load=false: ann.faiss exists immediately after insert
// 4. skip_write_index_on_load=true + BUILD INDEX: ann.faiss exists after BUILD INDEX
// 5. Query correctness: l2_distance_approximate returns valid results
// 6. Inverted index preservation: inverted index is NOT skipped (only ANN is skipped)

suite("test_skip_write_index_on_load") {
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

    // Query to trigger sync in cloud mode
    def count = sql "SELECT count(*) FROM ${tableName}"
    logger.info("Inserted ${count[0][0]} rows")

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

    // Query to trigger sync in cloud mode
    def normalCount = sql "SELECT count(*) FROM ${normalTableName}"
    logger.info("Inserted ${normalCount[0][0]} rows into normal table")

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

    // ============================================================
    // Case 4: skip_write_index_on_load=true, BUILD INDEX can force build
    // Expected: ann.faiss exists after BUILD INDEX
    // This verifies that BUILD INDEX can generate ANN index even when
    // skip_write_index_on_load=true (not only compaction)
    // ============================================================
    logger.info("Case 4: skip_write_index_on_load=true, verify BUILD INDEX can force build ANN index")

    def buildIndexTableName = "tbl_build_index_ann"
    sql "drop table if exists ${buildIndexTableName}"
    sql """
    CREATE TABLE ${buildIndexTableName} (
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

    // Insert data
    for (int batch = 0; batch < 3; batch++) {
        sql "INSERT INTO ${buildIndexTableName} VALUES ${generateInsertValues(100, 20000 + batch, batch * 100)}"
    }

    // Query to trigger sync in cloud mode
    def buildCount = sql "SELECT count(*) FROM ${buildIndexTableName}"
    logger.info("Inserted ${buildCount[0][0]} rows into build index table")

    // Get tablet info
    def buildTablets = sql_return_maparray "SHOW TABLETS FROM ${buildIndexTableName}"
    def buildTabletId = buildTablets[0].TabletId
    def buildBackendId = buildTablets[0].BackendId
    def buildBeIp = backendId_to_backendIP.get(buildBackendId)
    def buildBePort = backendId_to_backendHttpPort.get(buildBackendId)
    logger.info("Build index table Tablet ID: ${buildTabletId}, Backend: ${buildBeIp}:${buildBePort}")

    // Verify: no ANN index before BUILD INDEX
    assertTrue(isIndexFileEmpty(buildBeIp, buildBePort, buildTabletId),
        "Case 4 pre-check FAILED: index file should be empty before BUILD INDEX")
    logger.info("Case 4 pre-check: no ANN index before BUILD INDEX")

    // Execute BUILD INDEX (use BUILD INDEX ON table for cloud mode compatibility)
    sql "BUILD INDEX ON ${buildIndexTableName}"

    // Wait for BUILD INDEX to complete
    def maxWaitSeconds = 120
    def waitInterval = 2000
    def waited = 0
    def buildIndexSuccess = false

    while (waited < maxWaitSeconds * 1000) {
        def indexState = sql_return_maparray "SHOW BUILD INDEX FROM ${context.dbName} WHERE TableName='${buildIndexTableName}'"
        logger.info("BUILD INDEX state: ${indexState}")

        if (indexState.size() > 0) {
            // Check the most recent job (last entry in the list)
            def latestJob = indexState[indexState.size() - 1]
            def state = latestJob.State
            logger.info("Latest BUILD INDEX job state: ${state}, JobId: ${latestJob.JobId}")
            if (state == "FINISHED") {
                buildIndexSuccess = true
                break
            } else if (state == "CANCELLED") {
                logger.warn("BUILD INDEX was cancelled: ${latestJob.Msg}")
                break
            }
        }
        sleep(waitInterval)
        waited += waitInterval
    }

    assertTrue(buildIndexSuccess, "Case 4 FAILED: BUILD INDEX did not complete successfully")

    // Verify: ANN index exists after BUILD INDEX
    // This is the expected behavior - BUILD INDEX can force build ANN index
    // even when skip_write_index_on_load=true
    assertTrue(hasAnnIndex(buildBeIp, buildBePort, buildTabletId),
        "Case 4 FAILED: ann.faiss should exist after BUILD INDEX")
    logger.info("Case 4 PASSED: BUILD INDEX successfully built ANN index")

    // ============================================================
    // Case 5: Query correctness test - verify ANN queries work
    // Expected: l2_distance_approximate returns correct results
    // ============================================================
    logger.info("Case 5: Query correctness test - verify ANN queries work after index is built")

    // Query the table where ANN index was built via compaction (Case 2)
    // Use a known vector and verify results are reasonable
    def queryResult = sql """
        SELECT id FROM ${tableName}
        ORDER BY l2_distance_approximate(embedding, [50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0])
        LIMIT 5
    """
    logger.info("Query result from compacted table: ${queryResult}")
    assertTrue(queryResult.size() == 5, "Case 5 FAILED: Expected 5 results from ANN query")
    logger.info("Case 5 PASSED: ANN query returns correct number of results")

    // ============================================================
    // Case 6: Inverted index preservation test
    // Expected: inverted index is built even when skip_write_index_on_load=true
    // (skip_write_index_on_load only affects ANN index, not inverted index)
    // ============================================================
    logger.info("Case 6: Verify inverted index is NOT skipped when skip_write_index_on_load=true")

    def mixedTableName = "tbl_mixed_index"
    sql "drop table if exists ${mixedTableName}"
    sql """
    CREATE TABLE ${mixedTableName} (
        id INT NOT NULL,
        embedding ARRAY<FLOAT> NOT NULL,
        text_col VARCHAR(100),
        INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
            "index_type"="hnsw",
            "metric_type"="l2_distance",
            "dim"="8"
        ),
        INDEX idx_text (`text_col`) USING INVERTED
    ) ENGINE=OLAP
    DUPLICATE KEY(id)
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES (
        "replication_num" = "1",
        "disable_auto_compaction" = "true",
        "skip_write_index_on_load" = "true"
    );
    """

    // Insert data with text
    def random = new Random(30000)
    def mixedValues = []
    for (int i = 0; i < 100; i++) {
        def vec = (0..<8).collect { (random.nextFloat() * 100).round(6) }
        mixedValues << "(${i}, [${vec.join(',')}], 'text_${i}')"
    }
    sql "INSERT INTO ${mixedTableName} VALUES ${mixedValues.join(',')}"

    // Query to trigger sync
    def mixedCount = sql "SELECT count(*) FROM ${mixedTableName}"
    logger.info("Inserted ${mixedCount[0][0]} rows into mixed index table")

    // Get tablet info
    def mixedTablets = sql_return_maparray "SHOW TABLETS FROM ${mixedTableName}"
    def mixedTabletId = mixedTablets[0].TabletId
    def mixedBackendId = mixedTablets[0].BackendId
    def mixedBeIp = backendId_to_backendIP.get(mixedBackendId)
    def mixedBePort = backendId_to_backendHttpPort.get(mixedBackendId)

    // Check index files
    def mixedIndexResponse = showNestedIndexFile(mixedBeIp, mixedBePort, mixedTabletId)

    // ANN index should NOT exist (skip_write_index_on_load=true)
    assertFalse(mixedIndexResponse.contains("ann.faiss"),
        "Case 6 FAILED: ann.faiss should NOT exist when skip_write_index_on_load=true")

    // Inverted index SHOULD exist (not affected by skip_write_index_on_load)
    // The inverted index file pattern for text columns typically contains the column name or index id
    assertTrue(mixedIndexResponse.contains("idx_text") || !mixedIndexResponse.contains("is empty"),
        "Case 6 FAILED: inverted index should exist even when skip_write_index_on_load=true")

    // Verify inverted index works by running a MATCH query
    def matchResult = sql "SELECT count(*) FROM ${mixedTableName} WHERE text_col MATCH 'text_50'"
    logger.info("MATCH query result: ${matchResult}")
    assertTrue(matchResult[0][0] >= 1, "Case 6 FAILED: MATCH query should return results")
    logger.info("Case 6 PASSED: Inverted index is preserved and works correctly")

    // Cleanup
    sql "drop table if exists ${tableName}"
    sql "drop table if exists ${normalTableName}"
    sql "drop table if exists ${buildIndexTableName}"
    sql "drop table if exists ${mixedTableName}"

    logger.info("All test cases passed!")
}
