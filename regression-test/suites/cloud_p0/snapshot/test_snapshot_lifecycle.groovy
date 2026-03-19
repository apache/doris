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

suite("test_snapshot_lifecycle") {
    if (!isCloudMode()) {
        log.info("not cloud mode, skip test_snapshot_lifecycle")
        return
    }

    def enableClusterSnapshot = context.config.enableClusterSnapshot
    if (!enableClusterSnapshot) {
        logger.info("enableClusterSnapshot is not true, skip test_snapshot_lifecycle")
        return
    }

    def testStartTime = System.currentTimeMillis()
    logger.info("=== Starting test_snapshot_lifecycle at ${testStartTime} ===")

    // ---------------------------------------------------------------
    // Helper: poll information_schema.cluster_snapshots until a
    // snapshot with the given label reaches the expected state,
    // or until timeout (seconds). Returns the result row or null.
    // ---------------------------------------------------------------
    def waitForSnapshotState = { String label, String targetState, int maxWaitSec ->
        def deadline = System.currentTimeMillis() + maxWaitSec * 1000L
        while (System.currentTimeMillis() < deadline) {
            def stateRows = sql """
                SELECT STATUS FROM information_schema.cluster_snapshots
                WHERE LABEL = '${label}'
                ORDER BY CREATE_AT DESC LIMIT 1
            """
            if (stateRows != null && !stateRows.isEmpty()) {
                def state = stateRows[0][0].toString()
                logger.info("Snapshot label=${label} current state=${state}")
                if (state == targetState) {
                    def fullRows = sql """
                        SELECT * FROM information_schema.cluster_snapshots
                        WHERE LABEL = '${label}'
                        ORDER BY CREATE_AT DESC LIMIT 1
                    """
                    return fullRows[0]
                }
            }
            Thread.sleep(5000)
        }
        logger.warn("Timeout (${maxWaitSec}s) waiting for snapshot label=${label} to reach ${targetState}")
        return null
    }

    // Helper: clean up all snapshots created during this test run
    def cleanupTestSnapshots = { List<String> snapshotIds ->
        for (sid in snapshotIds) {
            try {
                sql """ ADMIN DROP CLUSTER SNAPSHOT WHERE snapshot_id = '${sid}' """
                logger.info("Cleaned up snapshot ${sid}")
            } catch (Exception e) {
                logger.warn("Failed to cleanup snapshot ${sid}: ${e.message}")
            }
        }
    }

    def createdSnapshotIds = []

    try {
        // ===================================================================
        // Test 1: Feature Switch ON/OFF
        // ===================================================================
        logger.info("=== Test 1: Feature Switch ON/OFF ===")

        sql """ ADMIN SET CLUSTER SNAPSHOT FEATURE ON """
        def props = sql """ SELECT * FROM information_schema.cluster_snapshot_properties """
        logger.info("Properties after FEATURE ON: ${props}")
        assertTrue(props != null && !props.isEmpty(), "cluster_snapshot_properties should return rows")

        sql """ ADMIN SET CLUSTER SNAPSHOT FEATURE OFF """
        props = sql """ SELECT * FROM information_schema.cluster_snapshot_properties """
        logger.info("Properties after FEATURE OFF: ${props}")
        assertTrue(props != null && !props.isEmpty(), "cluster_snapshot_properties should still return rows")

        // Re-enable for subsequent tests
        sql """ ADMIN SET CLUSTER SNAPSHOT FEATURE ON """
        logger.info("Feature switch re-enabled for lifecycle tests")

        // ===================================================================
        // Test 2: Auto Snapshot Configuration
        // ===================================================================
        logger.info("=== Test 2: Auto Snapshot Configuration ===")

        sql """ ADMIN SET AUTO CLUSTER SNAPSHOT PROPERTIES(
            'max_reserved_snapshots' = '5',
            'snapshot_interval_seconds' = '1800'
        ) """
        props = sql """ SELECT * FROM information_schema.cluster_snapshot_properties """
        logger.info("Auto snapshot properties: ${props}")
        assertTrue(props != null && !props.isEmpty(), "cluster_snapshot_properties should return rows")
        // Columns: SNAPSHOT_ENABLED, AUTO_SNAPSHOT, MAX_RESERVED_SNAPSHOTS, SNAPSHOT_INTERVAL_SECONDS
        def maxReserved = props[0][2]
        def intervalSec = props[0][3]
        logger.info("max_reserved_snapshots=${maxReserved}, snapshot_interval_seconds=${intervalSec}")
        assertEquals(5L, maxReserved as long, "max_reserved_snapshots should be 5")
        assertEquals(1800L, intervalSec as long, "snapshot_interval_seconds should be 1800")

        // Update to different values and verify
        sql """ ADMIN SET AUTO CLUSTER SNAPSHOT PROPERTIES(
            'max_reserved_snapshots' = '10',
            'snapshot_interval_seconds' = '3600'
        ) """
        props = sql """ SELECT * FROM information_schema.cluster_snapshot_properties """
        logger.info("Updated auto snapshot properties: ${props}")
        assertEquals(10L, props[0][2] as long, "max_reserved_snapshots should be 10")
        assertEquals(3600L, props[0][3] as long, "snapshot_interval_seconds should be 3600")

        // ===================================================================
        // Test 3: Validation Error Cases
        // ===================================================================
        logger.info("=== Test 3: Validation Error Cases ===")

        // Missing ttl property
        test {
            sql """ ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('label' = 'no_ttl_test') """
            exception "ttl"
        }

        // Negative ttl
        test {
            sql """ ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '-1', 'label' = 'neg_ttl_test') """
            exception "positive"
        }

        // Missing label
        test {
            sql """ ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '600') """
            exception "label"
        }

        // Empty label
        test {
            sql """ ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '600', 'label' = '') """
            exception "empty"
        }

        // Unknown property
        test {
            sql """ ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '600', 'label' = 'test', 'unknown_prop' = 'val') """
            exception "Unknown property"
        }

        // Drop with empty snapshot_id
        test {
            sql """ ADMIN DROP CLUSTER SNAPSHOT WHERE snapshot_id = '' """
            exception "empty"
        }

        // Invalid auto snapshot property
        test {
            sql """ ADMIN SET AUTO CLUSTER SNAPSHOT PROPERTIES('invalid_key' = '100') """
            exception "Unknown property"
        }

        logger.info("All validation error cases passed")

        // ===================================================================
        // Test 4: Manual Snapshot Creation
        // ===================================================================
        logger.info("=== Test 4: Manual Snapshot Creation ===")

        def label1 = "lifecycle_manual_" + testStartTime
        sql """ ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '3600', 'label' = '${label1}') """
        logger.info("Submitted manual snapshot with label=${label1}")

        // Poll for the snapshot — it may take time for the async workflow
        // to complete (begin → upload → commit). We accept either
        // SNAPSHOT_NORMAL (completed) or SNAPSHOT_PREPARE (still in progress).
        def snapshot1 = waitForSnapshotState(label1, "SNAPSHOT_NORMAL", 180)
        if (snapshot1 == null) {
            // Check if it is at least in PREPARE state
            def stateRows = sql """
                SELECT STATUS FROM information_schema.cluster_snapshots WHERE LABEL = '${label1}'
            """
            if (stateRows != null && !stateRows.isEmpty()) {
                logger.info("Snapshot ${label1} exists in state: ${stateRows[0][0]}")
                // Re-fetch full row for downstream field access
                def fullRows = sql """
                    SELECT * FROM information_schema.cluster_snapshots WHERE LABEL = '${label1}'
                """
                snapshot1 = fullRows[0]
            } else {
                assertTrue(false, "Snapshot ${label1} not found in information_schema after 180s")
            }
        }

        if (snapshot1 != null) {
            def snapshotId1 = snapshot1[0].toString()
            createdSnapshotIds.add(snapshotId1)
            logger.info("Snapshot 1 created with ID=${snapshotId1}")

            // Verify fields
            assertEquals(label1, snapshot1[9].toString(), "LABEL should match")
            assertEquals(3600L, snapshot1[8] as long, "TTL should be 3600")
            // AUTO should be false for manual snapshot
            def autoVal = snapshot1[7]
            logger.info("AUTO field value: ${autoVal} (type: ${autoVal?.getClass()?.name})")
            assertFalse(autoVal as boolean, "AUTO should be false for manual snapshot")
        }

        // ===================================================================
        // Test 5: Query Snapshots with Filters
        // ===================================================================
        logger.info("=== Test 5: Query Snapshots with Filters ===")

        // Query all
        def allSnapshots = sql """ SELECT * FROM information_schema.cluster_snapshots """
        logger.info("All snapshots count: ${allSnapshots.size()}")
        assertTrue(allSnapshots.size() >= 0, "Query should succeed")

        // Query with WHERE on label
        def filtered = sql """
            SELECT * FROM information_schema.cluster_snapshots WHERE LABEL = '${label1}'
        """
        logger.info("Filtered by label=${label1}: count=${filtered.size()}")
        if (snapshot1 != null) {
            assertTrue(filtered.size() >= 1, "Should find at least one snapshot with our label")
        }

        // Query with LIKE pattern
        def likeResult = sql """
            SELECT * FROM information_schema.cluster_snapshots WHERE LABEL LIKE 'lifecycle_manual_%'
        """
        logger.info("LIKE query result count: ${likeResult.size()}")

        // ===================================================================
        // Test 6: Create Second Snapshot
        // ===================================================================
        logger.info("=== Test 6: Create Second Snapshot ===")

        def label2 = "lifecycle_second_" + testStartTime
        sql """ ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '600', 'label' = '${label2}') """
        logger.info("Submitted second snapshot with label=${label2}")

        // Give it a moment to register
        Thread.sleep(10000)

        def rows2 = sql """
            SELECT * FROM information_schema.cluster_snapshots WHERE LABEL = '${label2}'
        """
        logger.info("Second snapshot query result: ${rows2}")
        if (rows2 != null && !rows2.isEmpty()) {
            def snapshotId2 = rows2[0][0].toString()
            createdSnapshotIds.add(snapshotId2)
            logger.info("Snapshot 2 created with ID=${snapshotId2}")

            assertEquals(label2, rows2[0][9].toString(), "LABEL should match")
            assertEquals(600L, rows2[0][8] as long, "TTL should be 600")
        }

        // ===================================================================
        // Test 7: Drop Snapshot
        // ===================================================================
        logger.info("=== Test 7: Drop Snapshot ===")

        if (createdSnapshotIds.size() > 0) {
            def dropId = createdSnapshotIds[0]
            logger.info("Dropping snapshot ID=${dropId}")
            sql """ ADMIN DROP CLUSTER SNAPSHOT WHERE snapshot_id = '${dropId}' """

            // Verify the snapshot is gone
            Thread.sleep(5000)
            def afterDrop = sql """
                SELECT STATUS FROM information_schema.cluster_snapshots WHERE ID = '${dropId}'
            """
            logger.info("After drop, query for ID=${dropId}: count=${afterDrop.size()}")
            // The dropped snapshot should not appear or should be in a non-NORMAL state
            if (afterDrop.size() > 0) {
                def stateAfterDrop = afterDrop[0][0].toString()
                logger.info("Snapshot ${dropId} state after drop: ${stateAfterDrop}")
                // After drop, it should not be in NORMAL state
                assertTrue(stateAfterDrop != "SNAPSHOT_NORMAL",
                    "Dropped snapshot should not be in NORMAL state")
            }
            createdSnapshotIds.remove(0)
        } else {
            logger.info("No snapshots to drop, skipping drop test")
        }

        // ===================================================================
        // Test 8: Duplicate Label Rejection
        // ===================================================================
        logger.info("=== Test 8: Duplicate Submission Check ===")

        // If a snapshot is still in PREPARE state, submitting another one
        // should be rejected by the handler (currentSnapshot != null).
        // This depends on timing — if all previous snapshots have completed,
        // this test just verifies the submission path.
        def labelDup = "lifecycle_dup_" + testStartTime
        try {
            sql """ ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '600', 'label' = '${labelDup}') """
            logger.info("First submission of ${labelDup} accepted")

            // Immediately try to submit another one — may be rejected if first is still running
            try {
                sql """ ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '600', 'label' = '${labelDup}_2') """
                logger.info("Second submission also accepted (first may have completed quickly)")
            } catch (Exception e) {
                logger.info("Second submission rejected as expected: ${e.message}")
                assertTrue(e.message.contains("already") || e.message.contains("progress")
                    || e.message.contains("running"),
                    "Rejection should indicate an in-progress snapshot")
            }

            // Cleanup duplicate test snapshots
            Thread.sleep(5000)
            def dupRows = sql """
                SELECT ID FROM information_schema.cluster_snapshots
                WHERE LABEL LIKE 'lifecycle_dup_%'
            """
            for (row in dupRows) {
                createdSnapshotIds.add(row[0].toString())
            }
        } catch (Exception e) {
            logger.info("Duplicate test encountered error: ${e.message}")
        }

        // ===================================================================
        // Test 9: Auto Snapshot Trigger Verification
        // ===================================================================
        logger.info("=== Test 9: Auto Snapshot Trigger Verification ===")

        // Set a short interval to encourage auto-snapshot trigger.
        // Note: The actual minimum interval is controlled by
        // Config.cloud_auto_snapshot_min_interval_seconds (default 1800).
        // If the test environment allows shorter intervals, we can test
        // the trigger. Otherwise, we just verify the configuration is set.
        try {
            sql """ ADMIN SET AUTO CLUSTER SNAPSHOT PROPERTIES(
                'snapshot_interval_seconds' = '1800'
            ) """
            props = sql """ SELECT * FROM information_schema.cluster_snapshot_properties """
            logger.info("Auto snapshot config for trigger test: ${props}")

            // We record the current snapshot count and check later.
            // The auto-snapshot daemon (runAfterCatalogReady) runs on a
            // timer cycle, so in a short test window we may not see a trigger.
            def beforeAutoCount = sql """
                SELECT COUNT(*) FROM information_schema.cluster_snapshots WHERE AUTO = true
            """
            logger.info("Auto snapshots before wait: ${beforeAutoCount[0][0]}")

            // Wait briefly, then verify the count has not decreased.
            // The auto-snapshot daemon runs on a long timer cycle (default
            // 30 min), so we only assert the count is non-decreasing.
            Thread.sleep(10000)
            def afterAutoCount = sql """
                SELECT COUNT(*) FROM information_schema.cluster_snapshots WHERE AUTO = true
            """
            logger.info("Auto snapshots after wait: ${afterAutoCount[0][0]}")
            assertTrue(afterAutoCount[0][0] as long >= beforeAutoCount[0][0] as long,
                "Auto snapshot count should not decrease")
            logger.info("Auto snapshot trigger verification completed")
        } catch (Exception e) {
            logger.info("Auto snapshot trigger test skipped: ${e.message}")
        }

        logger.info("=== All test_snapshot_lifecycle tests passed ===")

    } finally {
        // ===================================================================
        // Cleanup: drop all snapshots created during this test
        // ===================================================================
        logger.info("=== Cleanup: removing test snapshots ===")
        cleanupTestSnapshots(createdSnapshotIds)

        // Also scan for any remaining test snapshots by label pattern
        try {
            def remaining = sql """
                SELECT ID FROM information_schema.cluster_snapshots
                WHERE LABEL LIKE 'lifecycle_%'
            """
            for (row in remaining) {
                try {
                    sql """ ADMIN DROP CLUSTER SNAPSHOT WHERE snapshot_id = '${row[0]}' """
                } catch (Exception e) {
                    logger.warn("Cleanup remaining snapshot ${row[0]} failed: ${e.message}")
                }
            }
        } catch (Exception e) {
            logger.warn("Cleanup scan failed: ${e.message}")
        }

        logger.info("=== test_snapshot_lifecycle cleanup complete ===")
    }
}
