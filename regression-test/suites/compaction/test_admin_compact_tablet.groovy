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
    assertEquals(1, tablets.size())
    def tabletId = tablets[0].TabletId
    def backendId = tablets[0].BackendId

    def backendIdToBackendIp = [:]
    def backendIdToBackendHttpPort = [:]
    getBackendIpHttpPort(backendIdToBackendIp, backendIdToBackendHttpPort)
    def beHost = backendIdToBackendIp["${backendId}"]
    def bePort = backendIdToBackendHttpPort["${backendId}"]

    def showTabletCompaction = {
        def (code, stdout, stderr) = be_show_tablet_status(beHost, bePort, tabletId)
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

    def before = showTabletCompaction()
    def rowsetsBefore = countDataRowsets(before)
    assertTrue(rowsetsBefore >= 8,
            "expected >= 8 data rowsets before tablet compaction, got ${rowsetsBefore}")
    assertEquals(epochTime, before["last cumulative success time"])

    sql "ADMIN COMPACT TABLET ${tabletId} WHERE TYPE = 'CUMULATIVE'"
    def after = null
    def deadline = System.currentTimeMillis() + 60 * 1000L
    while (System.currentTimeMillis() < deadline) {
        after = showTabletCompaction()
        if (after["last cumulative success time"] != epochTime
                && countDataRowsets(after) < rowsetsBefore) {
            break
        }
        sleep(500)
    }

    assertNotNull(after)
    assertNotEquals(epochTime, after["last cumulative success time"])
    assertEquals("[OK]", after["last cumulative status"])
    assertTrue(countDataRowsets(after) < rowsetsBefore,
            "tablet cumulative did not reduce rowset count: ${after.rowsets}")

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
