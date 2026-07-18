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

// Cloud-mode time travel: post-compaction correctness.
// Verifies that FOR SYSTEM_TIME AS OF returns correct results after cumulative
// compaction has merged and deleted the original rowsets.
// This is the critical correctness test — without compaction checkpoints
// the query would fail with "missed versions".
suite("test_cloud_time_travel_post_compaction", "p0,nonConcurrent") {
    if (!isCloudMode()) {
        logger.info("not cloud mode, skip")
        return
    }

    def tbl = "test_cloud_tt_compact"
    sql "DROP TABLE IF EXISTS ${tbl} FORCE"
    sql """
        CREATE TABLE ${tbl} (
            id     INT,
            status VARCHAR(20),
            amount DECIMAL(10,2)
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES (
            "replication_num"            = "1",
            "enable_time_travel"         = "true",
            "time_travel_retention_days" = "7",
            "disable_auto_compaction"    = "false"
        )
    """

    // ── batch 1: rows before T1 ───────────────────────────────────
    sql "INSERT INTO ${tbl} VALUES (1, 'pending', 100.00)"
    sql "INSERT INTO ${tbl} VALUES (2, 'pending', 200.00)"
    sleep(5000)   // give MS time to record commit timestamps
    def t1 = sql("SELECT NOW()")[0][0] as String
    sleep(1000)

    // ── batch 2: rows after T1 (used to accumulate rowsets) ───────
    sql "INSERT INTO ${tbl} VALUES (3, 'shipped',   300.00)"
    sql "INSERT INTO ${tbl} VALUES (4, 'shipped',   400.00)"
    sql "INSERT INTO ${tbl} VALUES (5, 'delivered', 500.00)"
    sql "INSERT INTO ${tbl} VALUES (6, 'delivered', 600.00)"
    sql "INSERT INTO ${tbl} VALUES (7, 'delivered', 700.00)"
    sql "INSERT INTO ${tbl} VALUES (8, 'delivered', 800.00)"

    // ── verify T1 BEFORE compaction ───────────────────────────────
    def before = sql "SELECT id FROM ${tbl} FOR SYSTEM_TIME AS OF '${t1}' ORDER BY id"
    assertEquals([[1], [2]], before)

    // ── trigger cumulative compaction ─────────────────────────────
    def tablets = sql_return_maparray "SHOW TABLETS FROM ${tbl}"
    def backendId_to_backendIP  = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)

    tablets.each { tablet ->
        def beHost = backendId_to_backendIP["${tablet.BackendId}"]
        def bePort = backendId_to_backendHttpPort["${tablet.BackendId}"]
        if (beHost && bePort) {
            def compactResult = be_run_compaction(beHost, bePort, tablet.TabletId as String, "cumulative")
            logger.info("compaction result for tablet ${tablet.TabletId}: ${compactResult}")
        }
    }
    sleep(10000)  // wait for compaction to finish

    // ── verify T1 AFTER compaction — must return same 2 rows ─────
    // Without compaction checkpoints this would throw "missed versions".
    def after = sql "SELECT id FROM ${tbl} FOR SYSTEM_TIME AS OF '${t1}' ORDER BY id"
    assertEquals([[1], [2]], after)

    // ── current state unaffected ──────────────────────────────────
    def cur = sql "SELECT COUNT(*) FROM ${tbl}"
    assertEquals(8L, cur[0][0] as Long)

    // ── aggregate at T1 after compaction ──────────────────────────
    def agg = sql "SELECT COUNT(*), SUM(amount) FROM ${tbl} FOR SYSTEM_TIME AS OF '${t1}'"
    assertEquals(2L,    agg[0][0] as Long)
    assertEquals(300.0, agg[0][1] as Double, 0.01)

    sql "DROP TABLE IF EXISTS ${tbl} FORCE"
}
