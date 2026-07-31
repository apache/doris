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

// End-to-end test for `ADMIN COMPACT TABLE ... WHERE type = 'BASE'|'CUMULATIVE'|'FULL'`
// under the cloud (storage-compute split) engine.
//
// Mirrors test_admin_compact_table.groovy but accounts for cloud specifics:
//   - each tablet has a single CloudReplica; FE routes via CloudReplica.getBackendId()
//   - the BE does not hold the full rowset list until something forces a sync,
//     so we issue a SELECT before polling
suite("test_cloud_admin_compact_table", "p0") {
    if (!isCloudMode()) {
        logger.info("not cloud mode, skip this test")
        return
    }

    def tableName = "test_cloud_admin_compact_table"
    def partName = tableName

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            k INT,
            v INT
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        )
    """

    def tablets = sql_return_maparray "SHOW TABLETS FROM ${tableName}"
    assertEquals(1, tablets.size())
    def tabletId = tablets[0].TabletId
    def backendId = tablets[0].BackendId

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)
    def beHost = backendId_to_backendIP["${backendId}"]
    def bePort = backendId_to_backendHttpPort["${backendId}"]

    def showTabletCompaction = {
        // Force a read so BE syncs rowsets from MS before we inspect them.
        sql "SELECT COUNT(*) FROM ${tableName}"
        def (code, stdout, stderr) = be_show_tablet_status(beHost, bePort, tabletId)
        assertEquals(0, code)
        return parseJson(stdout.trim())
    }

    def countDataRowsets = { json ->
        return json.rowsets.findAll { it.contains(" DATA ") }.size()
    }

    def EPOCH_TIME = "1970-01-01 08:00:00.000"

    // Poll until BOTH `last_<type>_success_time` is updated AND the rowset count
    // has decreased. Cloud rowset list refreshes after MS confirms the merge, so
    // requiring both prevents false positives when only the schedule time ticks.
    def waitCompaction = { String type, int beforeCount, int timeoutSec = 90 ->
        def deadline = System.currentTimeMillis() + timeoutSec * 1000L
        def last
        while (System.currentTimeMillis() < deadline) {
            last = showTabletCompaction()
            def success = last["last ${type} success time"]
            def timeUpdated = success != null && success != EPOCH_TIME
            def merged = countDataRowsets(last) < beforeCount
            if (timeUpdated && merged) {
                return last
            }
            sleep(1000)
        }
        return last
    }

    // ---------- load data ----------
    for (int i = 1; i <= 5; i++) {
        sql "INSERT INTO ${tableName} VALUES (${i}, ${i})"
    }

    def beforeCumu = showTabletCompaction()
    def rowsetsBeforeCumu = countDataRowsets(beforeCumu)
    assertTrue(rowsetsBeforeCumu >= 5,
            "expected >= 5 data rowsets before cumulative, got ${rowsetsBeforeCumu}")

    // ---------- case 1: cumulative ----------
    sql "ADMIN COMPACT TABLE ${tableName} PARTITION (${partName}) WHERE type = 'cumulative'"
    def afterCumu = waitCompaction("cumulative", rowsetsBeforeCumu)
    assertNotEquals(EPOCH_TIME, afterCumu["last cumulative success time"])
    assertTrue(countDataRowsets(afterCumu) < rowsetsBeforeCumu,
            "cumulative did not reduce rowset count: ${afterCumu.rowsets}")

    // ---------- case 2: full ----------
    for (int i = 6; i <= 8; i++) {
        sql "INSERT INTO ${tableName} VALUES (${i}, ${i})"
    }
    def beforeFull = showTabletCompaction()
    def rowsetsBeforeFull = countDataRowsets(beforeFull)
    assertTrue(rowsetsBeforeFull >= 2,
            "expected >= 2 data rowsets before full, got ${rowsetsBeforeFull}")

    sql "ADMIN COMPACT TABLE ${tableName} PARTITION (${partName}) WHERE type = 'full'"
    def afterFull = waitCompaction("full", rowsetsBeforeFull)
    assertNotEquals(EPOCH_TIME, afterFull["last full success time"])
    assertEquals("[OK]", afterFull["last full status"])
    assertTrue(countDataRowsets(afterFull) < rowsetsBeforeFull,
            "cloud full did not reduce rowset count: ${afterFull.rowsets}")

    // ---------- case 3: base ----------
    sql "ADMIN COMPACT TABLE ${tableName} PARTITION (${partName}) WHERE type = 'base'"
    def afterBase = null
    for (int i = 0; i < 30; i++) {
        afterBase = showTabletCompaction()
        if (afterBase["last base status"] != "" ||
            afterBase["last base success time"] != EPOCH_TIME ||
            afterBase["last base failure time"] != EPOCH_TIME) {
            break
        }
        sleep(1000)
    }
    // Cloud base may legitimately be rejected for insufficient input rowsets
    // (e.g. `[E-808]insufficent compaction input rowset`). We only verify the
    // FE->BE path dispatched the task.
    def baseStatus = afterBase["last base status"]
    assertTrue(baseStatus == "[OK]" || baseStatus.startsWith("[E-"),
            "unexpected base status in cloud: ${baseStatus}")

    // ---------- data correctness ----------
    qt_select_count "SELECT COUNT(*) FROM ${tableName}"
    qt_select_all "SELECT * FROM ${tableName} ORDER BY k"

    // ---------- negative: unknown type ----------
    test {
        sql "ADMIN COMPACT TABLE ${tableName} PARTITION (${partName}) WHERE type = 'UNKNOWN'"
        exception "BASE/CUMULATIVE/FULL"
    }

    // ---------- negative: no WHERE clause ----------
    test {
        sql "ADMIN COMPACT TABLE ${tableName} PARTITION (${partName})"
        exception "type"
    }
}
