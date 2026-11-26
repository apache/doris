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

suite("test_admin_compact_tablet", "p0") {
    if (isCloudMode()) {
        return
    }

    def tableName = "test_admin_compact_tablet"
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
    // SHOW TABLETS returns one row per replica. BUCKETS 1 still has only one
    // logical tablet when force_olap_table_replication_num creates more replicas.
    def tabletIds = tablets.collect { it.TabletId }.unique()
    assertEquals(1, tabletIds.size())
    def tabletId = tabletIds[0]
    def replicas = tablets.findAll { it.TabletId == tabletId }
    assertTrue(replicas.size() >= 1)

    def backendIdToBackendIp = [:]
    def backendIdToBackendHttpPort = [:]
    getBackendIpHttpPort(backendIdToBackendIp, backendIdToBackendHttpPort)
    def targets = replicas.collect { replica ->
        def backendId = replica.BackendId
        def target = [
                backendId: backendId,
                host: backendIdToBackendIp["${backendId}"],
                port: backendIdToBackendHttpPort["${backendId}"]
        ]
        assertNotNull(target.host, "backend ${backendId} has no host")
        assertNotNull(target.port, "backend ${backendId} has no HTTP port")
        return target
    }

    def showTabletCompaction = { target ->
        def (code, stdout, stderr) = be_show_tablet_status(target.host, target.port, tabletId)
        assertEquals(0, code)
        return parseJson(stdout.trim())
    }

    def countDataRowsets = { json ->
        return json.rowsets.findAll { it.contains(" DATA ") }.size()
    }

    def epochTime = "1970-01-01 08:00:00.000"
    for (int i = 1; i <= 8; i++) {
        sql "INSERT INTO ${tableName} VALUES (${i}, ${i})"
    }

    def beforeByBackendId = [:]
    targets.each { target ->
        def before = showTabletCompaction(target)
        def rowsetsBefore = countDataRowsets(before)
        assertTrue(rowsetsBefore >= 8,
                "backend ${target.backendId}: expected >= 8 data rowsets before tablet compaction, "
                        + "got ${rowsetsBefore}")
        assertEquals(epochTime, before["last cumulative success time"])
        beforeByBackendId["${target.backendId}"] = [
                successTime: before["last cumulative success time"],
                rowsetCount: rowsetsBefore
        ]
    }

    sql "ADMIN COMPACT TABLET ${tabletId} WHERE TYPE = 'CUMULATIVE'"
    def afterByBackendId = [:]
    def pendingBackendIds = targets.collect { "${it.backendId}" } as Set
    def deadline = System.currentTimeMillis() + 90 * 1000L
    while (System.currentTimeMillis() < deadline && !pendingBackendIds.isEmpty()) {
        targets.findAll { pendingBackendIds.contains("${it.backendId}") }.each { target ->
            def backendId = "${target.backendId}"
            def before = beforeByBackendId[backendId]
            def after = showTabletCompaction(target)
            afterByBackendId[backendId] = after
            if (after["last cumulative success time"] != before.successTime
                    && after["last cumulative status"] == "[OK]"
                    && countDataRowsets(after) < before.rowsetCount) {
                pendingBackendIds.remove(backendId)
            }
        }
        if (!pendingBackendIds.isEmpty()) {
            sleep(500)
        }
    }

    assertTrue(pendingBackendIds.isEmpty(),
            "tablet cumulative compaction did not finish on backends ${pendingBackendIds}; "
                    + "last statuses: ${afterByBackendId}")
    targets.each { target ->
        def backendId = "${target.backendId}"
        def before = beforeByBackendId[backendId]
        def after = afterByBackendId[backendId]
        assertNotNull(after)
        assertNotEquals(before.successTime, after["last cumulative success time"])
        assertEquals("[OK]", after["last cumulative status"])
        assertTrue(countDataRowsets(after) < before.rowsetCount,
                "backend ${backendId}: tablet cumulative did not reduce rowset count: ${after.rowsets}")
    }

    test {
        sql "ADMIN COMPACT TABLET ${tabletId} WHERE TYPE = 'UNKNOWN'"
        exception "BASE/CUMULATIVE/FULL"
    }

    test {
        sql "ADMIN COMPACT TABLET ${tabletId}"
        exception "WHERE"
    }

    def rowCount = sql "SELECT COUNT(*) FROM ${tableName}"
    assertEquals(8, rowCount[0][0])
    def rows = sql "SELECT * FROM ${tableName} ORDER BY k"
    assertEquals((1..8).collect { [it, it] }, rows)
}
