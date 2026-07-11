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

// Cloud-mode time travel tests for DUPLICATE KEY tables.
// Uses FOR SYSTEM_TIME AS OF '<timestamp>' (cloud-only; no binlog required).
suite("test_cloud_time_travel_dup", "p0,nonConcurrent") {
    if (!isCloudMode()) {
        logger.info("not cloud mode, skip")
        return
    }

    def tbl = "test_cloud_tt_dup"
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
            "time_travel_retention_days" = "7"
        )
    """

    // ── batch 1 ──────────────────────────────────────────────────
    sql "INSERT INTO ${tbl} VALUES (1, 'pending',  100.00)"
    sql "INSERT INTO ${tbl} VALUES (2, 'pending',  200.00)"
    sleep(3000)   // let MS commit timestamps settle
    def t1 = sql("SELECT NOW()")[0][0] as String
    sleep(1000)

    // ── batch 2 ──────────────────────────────────────────────────
    sql "INSERT INTO ${tbl} VALUES (3, 'shipped',  300.00)"
    sql "INSERT INTO ${tbl} VALUES (4, 'shipped',  400.00)"
    sleep(3000)
    def t2 = sql("SELECT NOW()")[0][0] as String

    // ── basic correctness ─────────────────────────────────────────
    // At T1: only rows 1,2
    def r1 = sql "SELECT id FROM ${tbl} FOR SYSTEM_TIME AS OF '${t1}' ORDER BY id"
    assertEquals([[1], [2]], r1)

    // At T2: all four rows
    def r2 = sql "SELECT id FROM ${tbl} FOR SYSTEM_TIME AS OF '${t2}' ORDER BY id"
    assertEquals([[1], [2], [3], [4]], r2)

    // ── aggregate at T1 ───────────────────────────────────────────
    def agg = sql "SELECT COUNT(*), SUM(amount) FROM ${tbl} FOR SYSTEM_TIME AS OF '${t1}'"
    assertEquals(2L,    agg[0][0] as Long)
    assertEquals(300.0, agg[0][1] as Double, 0.01)

    // ── CTAS from historical snapshot ─────────────────────────────
    def snap = "test_cloud_tt_dup_snap"
    sql "DROP TABLE IF EXISTS ${snap} FORCE"
    sql "CREATE TABLE ${snap} AS SELECT * FROM ${tbl} FOR SYSTEM_TIME AS OF '${t1}'"
    def snapRows = sql "SELECT id FROM ${snap} ORDER BY id"
    assertEquals([[1], [2]], snapRows)

    // ── INSERT INTO … SELECT FOR SYSTEM_TIME AS OF ───────────────────────
    def dest = "test_cloud_tt_dup_dest"
    sql "DROP TABLE IF EXISTS ${dest} FORCE"
    sql """
        CREATE TABLE ${dest} LIKE ${tbl}
    """
    sql "INSERT INTO ${dest} SELECT * FROM ${tbl} FOR SYSTEM_TIME AS OF '${t1}'"
    def destRows = sql "SELECT id FROM ${dest} ORDER BY id"
    assertEquals([[1], [2]], destRows)

    // ── error: future timestamp ───────────────────────────────────
    test {
        sql "SELECT * FROM ${tbl} FOR SYSTEM_TIME AS OF '2099-01-01 00:00:00'"
        exception "is in the future"
    }

    // ── error: non-TT table ────────────────────────────────────────
    // ── error: TT query on table with retention_days=0 ───────────────
    def zeroRetention = "test_cloud_tt_zero_retention"
    sql "DROP TABLE IF EXISTS ${zeroRetention} FORCE"
    sql """
        CREATE TABLE ${zeroRetention} (id INT) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "enable_time_travel" = "true",
                    "time_travel_retention_days" = "0")
    """
    test {
        sql "SELECT * FROM ${zeroRetention} FOR SYSTEM_TIME AS OF '${t1}'"
        exception "time_travel_retention_days must be >= 1"
    }
    sql "DROP TABLE IF EXISTS ${zeroRetention} FORCE"

    def noTT = "test_cloud_tt_dup_nott"
    sql "DROP TABLE IF EXISTS ${noTT} FORCE"
    sql """
        CREATE TABLE ${noTT} (id INT) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    test {
        sql "SELECT * FROM ${noTT} FOR SYSTEM_TIME AS OF '${t1}'"
        exception "does not have time travel enabled"
    }

    // ── error: FOR VERSION AS OF not supported on TT tables ───────
    test {
        sql "SELECT * FROM ${tbl} FOR VERSION AS OF 1"
        exception "FOR VERSION AS OF is not supported"
    }

    // cleanup
    sql "DROP TABLE IF EXISTS ${tbl} FORCE"
    sql "DROP TABLE IF EXISTS ${snap} FORCE"
    sql "DROP TABLE IF EXISTS ${dest} FORCE"
    sql "DROP TABLE IF EXISTS ${noTT} FORCE"
}
