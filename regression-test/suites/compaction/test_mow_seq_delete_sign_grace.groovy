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

// Verifies the "deleted key resurrection" problem on MOW unique-key tables that
// have a sequence column, and the grace-watermark fix.
//
// Scenario (sequence column; a delete at seq=S must suppress every later write
// with seq < S):
//   1. delete key k=1 with a HIGH sequence (seq=1000) via delete sign
//   2. base/full compaction physically prunes the delete-sign tombstone
//   3. a late, out-of-order write for k=1 arrives with a LOWER sequence (seq=900)
// Correct result: k=1 stays deleted (900 < 1000). Resurrection (k=1 visible) is
// the bug.
//
// Root cause: pruning the tombstone drops the seq=1000 value that suppresses the
// late write, so lookup_row_key returns KEY_NOT_FOUND and the seq=900 row becomes
// visible. The fix (config `mow_sequence_delete_sign_grace_seconds` > 0) keeps the
// tombstone until its rowset is older than the grace window, so the late write is
// still suppressed.
//
// This single suite runs the scenario twice against the same (fixed) BE:
//   - grace = 0    -> legacy behavior, reproduces the resurrection
//   - grace = 3600 -> fix retains the tombstone, suppresses the late write
//
// NOTE: requires a BE built with `mow_sequence_delete_sign_grace_seconds`.
suite("test_mow_seq_delete_sign_grace", "nonConcurrent") {
    // Compaction-control HTTP APIs used here are the local-mode ones.
    if (isCloudMode()) {
        return
    }

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)

    def set_be_param = { paramName, paramValue ->
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            def (code, out, err) = curl("POST", String.format(
                    "http://%s:%s/api/update_config?%s=%s", beIp, bePort, paramName, paramValue))
            assertTrue(out.contains("OK"),
                    "failed to set ${paramName}=${paramValue} on ${beIp}:${bePort}, out=${out}")
        }
    }

    def triggerFullCompaction = { tablet ->
        String tablet_id = tablet.TabletId
        String backend_id = tablet.BackendId
        def be_host = backendId_to_backendIP[backend_id]
        def be_port = backendId_to_backendHttpPort[backend_id]
        def (code, out, err) = be_run_full_compaction(be_host, be_port, tablet_id)
        logger.info("full compaction: code=${code}, out=${out}, err=${err}")
        assertEquals(code, 0)
        assertEquals("success", parseJson(out.trim()).status.toLowerCase())
    }

    def waitForCompaction = { tablet ->
        String tablet_id = tablet.TabletId
        String backend_id = tablet.BackendId
        def be_host = backendId_to_backendIP[backend_id]
        def be_port = backendId_to_backendHttpPort[backend_id]
        def running = true
        do {
            Thread.sleep(1000)
            def (code, out, err) = be_get_compaction_status(be_host, be_port, tablet_id)
            assertEquals(code, 0)
            def st = parseJson(out.trim())
            assertEquals("success", st.status.toLowerCase())
            running = st.run_status
        } while (running)
    }

    // Runs the full scenario and returns the number of visible rows for k=1
    // (0 = correctly suppressed, 1 = resurrected).
    def runScenario = { tableName ->
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE ${tableName} (
                `k` int NOT NULL,
                `c` int NOT NULL,
                `seq` int NOT NULL
            ) UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES (
                "enable_unique_key_merge_on_write" = "true",
                "replication_num" = "1",
                "function_column.sequence_col" = "seq",
                "disable_auto_compaction" = "true"
            );
        """
        def tablet = (sql_return_maparray(" show tablets from ${tableName}; "))[0]

        // 1. initial rows (k=1 at a low seq, plus another key to keep the tablet non-trivial)
        sql """ INSERT INTO ${tableName} VALUES (1, 1, 1); """
        sql """ INSERT INTO ${tableName} VALUES (2, 2, 1); """
        sql "sync"

        // 2. delete k=1 with a HIGH sequence (seq=1000) via delete sign
        sql """ INSERT INTO ${tableName} (k, c, seq, __DORIS_DELETE_SIGN__) VALUES (1, 0, 1000, 1); """
        sql "sync"
        def deletedBefore = sql " select count(*) from ${tableName} where k = 1; "
        assertEquals(0, deletedBefore[0][0] as int, "k=1 must be deleted before compaction")

        // 3. full compaction. With grace=0 the delete-sign tombstone (seq=1000) is pruned;
        //    with grace>0 it is retained. (base compaction goes through the same gate.)
        triggerFullCompaction(tablet)
        waitForCompaction(tablet)

        // 4. late, out-of-order write for k=1 with a LOWER sequence (seq=900 < 1000)
        sql """ INSERT INTO ${tableName} VALUES (1, 900, 900); """
        sql "sync"

        // 5. correct behavior: k=1 stays deleted (900 < 1000). Resurrection = bug.
        def visible = sql " select count(*) from ${tableName} where k = 1; "
        return visible[0][0] as int
    }

    try {
        // --- Scenario A: grace = 0 (legacy / default) -> reproduces resurrection ---
        set_be_param("mow_sequence_delete_sign_grace_seconds", "0")
        def resurrectedCount = runScenario("test_mow_seq_grace_bug")
        logger.info("grace=0   -> visible rows for k=1: ${resurrectedCount} (expect 1 = resurrected, documents the bug)")

        // --- Scenario B: grace large -> fix retains tombstone, suppresses the late write ---
        set_be_param("mow_sequence_delete_sign_grace_seconds", "3600")
        def fixedCount = runScenario("test_mow_seq_grace_fix")
        logger.info("grace=3600 -> visible rows for k=1: ${fixedCount} (expect 0 = suppressed, the fix)")

        // --- Scenario C: grace < 0 -> never prune tombstones for sequence-column tables
        //     (fallback for sequences that are not correlated with wall-clock time) ---
        set_be_param("mow_sequence_delete_sign_grace_seconds", "-1")
        def neverPruneCount = runScenario("test_mow_seq_grace_never")
        logger.info("grace=-1   -> visible rows for k=1: ${neverPruneCount} (expect 0 = suppressed, never prune)")

        assertEquals(1, resurrectedCount, "grace=0 should reproduce the resurrection (k=1 visible)")
        assertEquals(0, fixedCount, "grace>0 should keep k=1 deleted (late lower-seq write suppressed)")
        assertEquals(0, neverPruneCount, "grace<0 should keep k=1 deleted (tombstone never pruned)")
    } finally {
        set_be_param("mow_sequence_delete_sign_grace_seconds", "0")
    }
}
