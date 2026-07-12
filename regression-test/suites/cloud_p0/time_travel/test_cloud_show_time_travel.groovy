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

// End-to-end tests for SHOW TIME TRAVEL ON <table> [BETWEEN T1 AND T2].
suite("test_cloud_show_time_travel", "p0,nonConcurrent") {
    if (!isCloudMode()) {
        logger.info("not cloud mode, skip")
        return
    }

    def tbl = "test_cloud_stt_dup"
    sql "DROP TABLE IF EXISTS ${tbl} FORCE"
    sql """
        CREATE TABLE ${tbl} (
            id  INT,
            val VARCHAR(50)
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num"            = "1",
            "enable_time_travel"         = "true",
            "time_travel_retention_days" = "7"
        )
    """

    // Insert batch 1
    sql "INSERT INTO ${tbl} VALUES (1, 'a'), (2, 'b')"
    sleep(2000)

    // Insert batch 2
    sql "INSERT INTO ${tbl} VALUES (3, 'c')"
    sleep(2000)

    // ── SHOW TIME TRAVEL returns all snapshots (2 inserts = 2 snapshots) ──

    def result = sql "SHOW TIME TRAVEL ON ${tbl}"

    // Output structure: header row, queryable row, blank, snapshot rows..., blank, footer
    // snapshot rows have col[2] = positive integer version number
    def snapshotRows = result.findAll { row ->
        row[2] != null && row[2].toString().matches("[0-9]+") && row[2].toString().toInteger() > 0
    }

    // Both inserts must appear
    assertTrue(snapshotRows.size() >= 2,
            "expected 2 snapshot rows (one per insert), found ${snapshotRows.size()}")

    // Snapshots are newest-first
    long v1 = snapshotRows[0][2].toString().toLong()
    long v2 = snapshotRows[1][2].toString().toLong()
    assertTrue(v1 > v2, "snapshots must be newest-first: v1=${v1} v2=${v2}")

    // query_at is 1 second after snapshot_time
    def snapTime = snapshotRows[0][0].toString()
    def queryAt  = snapshotRows[0][1].toString()
    assertTrue(queryAt > snapTime, "query_at must be after snapshot_time")

    // Header row contains table name and snapshot count
    def headerRow = result.find { row ->
        row[0] != null && row[0].toString().contains(tbl)
    }
    assertNotNull(headerRow, "header row with table name must exist")
    assertTrue(headerRow[0].toString().contains("snapshot"),
            "header must mention snapshot count: ${headerRow[0]}")

    // Footer row: "All N snapshots shown. ..." — starts with "All", contains "snapshot"
    def footerRow = result.find { row ->
        row[0] != null && row[0].toString().startsWith("All") &&
                row[0].toString().contains("snapshot")
    }
    assertNotNull(footerRow, "footer row must exist")
    assertTrue(footerRow[0].toString().contains("shown"),
            "footer must say 'All N snapshots shown', got: ${footerRow[0]}")

    // query_at works correctly — use first snapshot's query_at
    def qt = snapshotRows[0][1].toString()
    def qtResult = sql "SELECT * FROM ${tbl} FOR SYSTEM_TIME AS OF '${qt}' ORDER BY id"
    assertTrue(qtResult.size() >= 3, "query at newest snapshot's query_at must return all 3 inserted rows, got ${qtResult.size()}")

    // ── BETWEEN filter narrows window ─────────────────────────────────────

    // Use older snapshot's query_at (not snapshot_time) as BETWEEN end.
    // snapshot_time is floor-second; actual commit is at snapshot_time.XXX ms.
    // parseTs(snapshot_time) → snapshot_time.000 ms < actual commit ms → version excluded.
    // query_at = snapshot_time + 1 second, guaranteed ≥ actual commit ms.
    def olderSnapQueryAt = snapshotRows[snapshotRows.size() - 1][1].toString()

    def filtered = sql """
        SHOW TIME TRAVEL ON ${tbl}
        BETWEEN '2000-01-01 00:00:00' AND '${olderSnapQueryAt}'
    """

    def filteredSnaps = filtered.findAll { row ->
        row[2] != null && row[2].toString().matches("[0-9]+") && row[2].toString().toInteger() > 0
    }
    assertTrue(filteredSnaps.size() >= 1, "BETWEEN must return at least 1 snapshot")
    assertTrue(filteredSnaps.size() <= snapshotRows.size(),
            "BETWEEN result must not exceed full result count")

    // Window row must appear when BETWEEN is used
    def windowRow = filtered.find { row ->
        row[0] != null && row[0].toString().contains("Window:")
    }
    assertNotNull(windowRow, "Window: row must appear when BETWEEN is used")

    // ── Error: non-TT table ───────────────────────────────────────────────

    def noTT = "test_cloud_stt_no_tt"
    sql "DROP TABLE IF EXISTS ${noTT} FORCE"
    sql """
        CREATE TABLE ${noTT} (id INT) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    test {
        sql "SHOW TIME TRAVEL ON ${noTT}"
        exception "not enabled"
    }

    // ── Error: table doesn't exist ────────────────────────────────────────

    test {
        sql "SHOW TIME TRAVEL ON nonexistent_table_xyz_999"
        exception ""
    }

    // ── retention_days=0 rejected at CREATE TABLE time ───────────────────────
    // PropertyAnalyzer.analyzeTimeTravelConfig() rejects retention_days < 1 at parse time,
    // so the error surfaces on CREATE TABLE, not on SHOW TIME TRAVEL.
    def zeroRet = "test_cloud_stt_zero_retention"
    sql "DROP TABLE IF EXISTS ${zeroRet} FORCE"
    test {
        sql """
            CREATE TABLE ${zeroRet} (id INT) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num"            = "1",
                "enable_time_travel"         = "true",
                "time_travel_retention_days" = "0"
            )
        """
        exception "retention_days"
    }

    // ── MOW (UNIQUE KEY) time travel — UPDATE and DELETE must not bleed into past ──
    // Regression for: sync_rowsets short-circuit (_max_version >= query_version) skips
    // re-sync on hot-cache tablets, causing capture_read_source({0, tt_version}) to return
    // an incomplete rowset chain and 0 rows from tablets touched by UPDATE/DELETE.

    def mow = "test_cloud_stt_mow"
    sql "DROP TABLE IF EXISTS ${mow} FORCE"
    sql """
        CREATE TABLE ${mow} (
            id     BIGINT,
            status VARCHAR(20),
            score  INT
        ) UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES (
            "replication_num"            = "1",
            "enable_time_travel"         = "true",
            "time_travel_retention_days" = "7"
        )
    """

    sql "INSERT INTO ${mow} VALUES (1, 'active', 10), (2, 'active', 20), (3, 'active', 30)"
    sleep(2000)

    def mowResult = sql "SHOW TIME TRAVEL ON ${mow}"
    def mowSnaps = mowResult.findAll { row ->
        row[2] != null && row[2].toString().matches("[0-9]+") && row[2].toString().toInteger() > 0
    }
    assertTrue(mowSnaps.size() >= 1, "MOW INSERT must produce at least 1 snapshot")

    // Use query_at from the INSERT snapshot for time travel
    def insertQueryAt = mowSnaps[0][1].toString()

    // UPDATE one row and DELETE another
    sql "UPDATE ${mow} SET status = 'closed', score = 99 WHERE id = 2"
    sleep(2000)
    sql "DELETE FROM ${mow} WHERE id = 3"
    sleep(2000)

    // Verify current state: only id=1 and id=2 (updated)
    def currentRows = sql "SELECT id FROM ${mow} ORDER BY id"
    assertEquals(2, currentRows.size(), "after DELETE, table must have 2 rows")

    // Time travel to INSERT snapshot: all 3 original rows must appear regardless of
    // whether compaction has merged the rowsets for tablets touched by UPDATE/DELETE.
    def ttRows = sql "SELECT id, status, score FROM ${mow} FOR SYSTEM_TIME AS OF '${insertQueryAt}' ORDER BY id"
    assertEquals(3, ttRows.size(),
            "time travel to INSERT snapshot must return all 3 rows (including subsequently updated/deleted rows)")
    assertEquals(1L,  ttRows[0][0] as long)
    assertEquals(2L,  ttRows[1][0] as long)
    assertEquals(3L,  ttRows[2][0] as long)
    assertEquals("active", ttRows[0][1])
    assertEquals("active", ttRows[1][1])  // id=2 must be original (not updated) value
    assertEquals("active", ttRows[2][1])  // id=3 must exist (not deleted)

    sql "DROP TABLE IF EXISTS ${mow} FORCE"

    // cleanup
    sql "DROP TABLE IF EXISTS ${tbl} FORCE"
    sql "DROP TABLE IF EXISTS ${noTT} FORCE"
}
