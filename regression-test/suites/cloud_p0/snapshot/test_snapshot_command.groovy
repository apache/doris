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

suite('test_snapshot_command') {
    if (!isCloudMode()) {
        log.info("not cloud mode just return")
        return
    }

    def label = "test_snapshot_cmd_" + System.currentTimeMillis()
    def createdSnapshotId = null

    def waitSnapshotByLabel = { String targetLabel, int timeoutSec ->
        def deadline = System.currentTimeMillis() + timeoutSec * 1000L
        while (System.currentTimeMillis() < deadline) {
            def rows = sql """
                SELECT ID, STATUS, LABEL, TTL
                FROM information_schema.cluster_snapshots
                WHERE LABEL = '${targetLabel}'
                ORDER BY CREATE_AT DESC
                LIMIT 1
            """
            if (rows != null && !rows.isEmpty()) {
                return rows[0]
            }
            Thread.sleep(2000)
        }
        return null
    }

    try {
        sql """ ADMIN CREATE CLUSTER SNAPSHOT PROPERTIES('ttl' = '600', 'label' = '${label}') """
        def snapshotRow = waitSnapshotByLabel(label, 120)
        assertTrue(snapshotRow != null, "snapshot should be visible in information_schema")
        createdSnapshotId = snapshotRow[0].toString()
        def snapshotStatus = snapshotRow[1].toString()
        assertTrue(snapshotStatus == "SNAPSHOT_PREPARE" || snapshotStatus == "SNAPSHOT_NORMAL",
                "snapshot status should be PREPARE or NORMAL, actual=${snapshotStatus}")
        assertEquals(label, snapshotRow[2].toString(), "snapshot label should match")
        assertEquals(600L, snapshotRow[3] as long, "snapshot ttl should match")

        // snapshot feature off
        sql """ ADMIN SET CLUSTER SNAPSHOT FEATURE OFF """

        // snapshot feature on
        sql """ ADMIN SET CLUSTER SNAPSHOT FEATURE ON """

        // set auto snapshot properties
        sql """ ADMIN SET AUTO CLUSTER SNAPSHOT PROPERTIES(
            'max_reserved_snapshots' = '10',
            'snapshot_interval_seconds' = '3600'
        ) """

        // show snapshot properties
        def props = sql """ SELECT * FROM information_schema.cluster_snapshot_properties """
        assertTrue(props != null && !props.isEmpty(), "cluster_snapshot_properties query should succeed")
        assertEquals(10L, props[0][2] as long, "max_reserved_snapshots should be 10")
        assertEquals(3600L, props[0][3] as long, "snapshot_interval_seconds should be 3600")

        // list and filter snapshot
        def allSnapshots = sql """ SELECT ID, LABEL FROM information_schema.cluster_snapshots """
        assertTrue(allSnapshots != null, "cluster_snapshots query should succeed")
        def filtered = sql """ SELECT ID FROM information_schema.cluster_snapshots WHERE LABEL = '${label}' """
        assertTrue(filtered.any { it[0].toString() == createdSnapshotId },
                "created snapshot should be queryable by label")

        sql """ ADMIN DROP CLUSTER SNAPSHOT WHERE snapshot_id = '${createdSnapshotId}' """
        Thread.sleep(2000)
        def afterDrop = sql """ SELECT STATUS FROM information_schema.cluster_snapshots WHERE ID = '${createdSnapshotId}' """
        if (afterDrop != null && !afterDrop.isEmpty()) {
            assertTrue(afterDrop[0][0].toString() != "SNAPSHOT_NORMAL",
                    "dropped snapshot should not remain NORMAL")
        }
        createdSnapshotId = null
    } finally {
        if (createdSnapshotId != null) {
            try {
                sql """ ADMIN DROP CLUSTER SNAPSHOT WHERE snapshot_id = '${createdSnapshotId}' """
            } catch (Exception e) {
                logger.info("cleanup snapshot failed: ${e.message}")
            }
        }
    }

}
