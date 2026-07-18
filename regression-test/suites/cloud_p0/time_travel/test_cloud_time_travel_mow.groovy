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

// Cloud-mode time travel tests for UNIQUE KEY (Merge-on-Write) tables.
// Verifies that UPDATE and DELETE are correctly excluded from historical reads.
suite("test_cloud_time_travel_mow", "p0,nonConcurrent") {
    if (!isCloudMode()) {
        logger.info("not cloud mode, skip")
        return
    }

    def tbl = "test_cloud_tt_mow"
    sql "DROP TABLE IF EXISTS ${tbl} FORCE"
    sql """
        CREATE TABLE ${tbl} (
            id     INT,
            status VARCHAR(20),
            amount DECIMAL(10,2)
        ) UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES (
            "replication_num"                  = "1",
            "enable_unique_key_merge_on_write" = "true",
            "enable_time_travel"               = "true",
            "time_travel_retention_days"       = "7"
        )
    """

    // ── baseline ──────────────────────────────────────────────────
    sql "INSERT INTO ${tbl} VALUES (1, 'pending', 100.00)"
    sql "INSERT INTO ${tbl} VALUES (2, 'pending', 200.00)"
    sleep(3000)
    def t0 = sql("SELECT NOW()")[0][0] as String
    sleep(1000)

    // ── UPDATE after T0 ───────────────────────────────────────────
    sql "UPDATE ${tbl} SET status = 'shipped' WHERE id = 1"
    sleep(3000)
    def t1 = sql("SELECT NOW()")[0][0] as String
    sleep(1000)

    // ── DELETE after T1 ───────────────────────────────────────────
    sql "DELETE FROM ${tbl} WHERE id = 2"
    sleep(3000)
    def t2 = sql("SELECT NOW()")[0][0] as String

    // ── verify current state ──────────────────────────────────────
    def cur = sql "SELECT id, status FROM ${tbl} ORDER BY id"
    assertEquals([[1, 'shipped']], cur)

    // ── at T0: original values, both rows ─────────────────────────
    def r0 = sql "SELECT id, status FROM ${tbl} FOR SYSTEM_TIME AS OF '${t0}' ORDER BY id"
    assertEquals([[1, 'pending'], [2, 'pending']], r0)

    // ── at T1: row 1 updated, row 2 still present ─────────────────
    def r1 = sql "SELECT id, status FROM ${tbl} FOR SYSTEM_TIME AS OF '${t1}' ORDER BY id"
    assertEquals([[1, 'shipped'], [2, 'pending']], r1)

    // ── at T2: row 2 deleted ──────────────────────────────────────
    def r2 = sql "SELECT id, status FROM ${tbl} FOR SYSTEM_TIME AS OF '${t2}' ORDER BY id"
    assertEquals([[1, 'shipped']], r2)

    // ── historical read is unaffected by current state ────────────
    // Still 2 rows at T0 even though current has only 1
    def r0again = sql "SELECT COUNT(*) FROM ${tbl} FOR SYSTEM_TIME AS OF '${t0}'"
    assertEquals(2L, r0again[0][0] as Long)

    sql "DROP TABLE IF EXISTS ${tbl} FORCE"
}
