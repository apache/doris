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

suite("test_spm_review_round26", "spm") {

    // The manual plan may not choose its own SCAN SELECTION.
    //
    //  - #6: bind 'SELECT k FROM t' WITH 'SELECT k FROM t PARTITION(p1)' is initially
    //    equivalent (only p1 exists), but after ADD PARTITION p2 the UNPINNED caller
    //    still matches the bind (the digest carries no selector) and the fingerprint
    //    hashes table identity + base columns only - the frozen plan keeps reading p1
    //    and every row in p2 silently disappears. The mirrored pair silently WIDENS the
    //    result. Such a CREATE is now rejected; the symmetric pair keeps working.
    // #1 (audit publication horizon), #2 (forwarded-session import gate), #3 (scan
    // bounds in the audit writer's zone), #4 (load-slot coalescing) and #5 (indexed
    // create-path dedup) are covered by AuditLogScannerCursorTest,
    // SPMRound26SafetyTest and BaselineManagerConcurrencyTest: none of them has a SQL
    // surface a single sandbox cluster can exercise.

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r26_") }
    }
    def dropOwnBaselines = {
        ownBaselines().each { row ->
            sql """DROP BASELINE PLAN ${row[0]}"""
        }
    }
    dropOwnBaselines()

    def explainOf = { String query -> sql("""EXPLAIN ${query}""").toString() }
    def createBaseline = { String bind, String plan ->
        (sql('CREATE GLOBAL BASELINE PLAN "' + bind + '" WITH "' + plan + '"')[0][0] as Long)
    }

    // ==================== setup ====================
    sql """DROP TABLE IF EXISTS spm_r26_p"""
    sql """
        CREATE TABLE spm_r26_p (k INT)
        PARTITION BY RANGE(k) (
            PARTITION p1 VALUES LESS THAN (10)
        )
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r26_p VALUES (1), (2)"""

    // ==================== #6: a plan-only partition pin is rejected ====================
    // (the statement must stay on ONE line: a test{} block treats every line as its own
    // statement)
    test {
        sql 'CREATE GLOBAL BASELINE PLAN "SELECT k FROM spm_r26_p" WITH "SELECT k FROM spm_r26_p PARTITION(p1)"'
        exception "scan selectors"
    }
    test {
        sql 'CREATE GLOBAL BASELINE PLAN "SELECT k FROM spm_r26_p PARTITION(p1)" WITH "SELECT k FROM spm_r26_p"'
        exception "scan selectors"
    }
    assertEquals(0, ownBaselines().size(),
            "a rejected CREATE must not leave a baseline behind: " + ownBaselines())

    // ==================== the symmetric pair keeps working ====================
    String pinned = "SELECT k FROM spm_r26_p PARTITION(p1)"
    long pinnedId = createBaseline(pinned, pinned)
    assertTrue(explainOf(pinned).contains("SPM baseline hit: id=${pinnedId}"),
            "the pinned caller must hit its own baseline: " + explainOf(pinned))
    assertEquals([[1], [2]], sql(pinned).sort(),
            "the pinned baseline reads exactly p1")

    // a NEW partition arrives: the pinned caller still reads only p1 (its contract), and
    // an UNPINNED caller never matches this baseline - no silent row loss either way
    sql """ALTER TABLE spm_r26_p ADD PARTITION p2 VALUES LESS THAN (20)"""
    sql """INSERT INTO spm_r26_p VALUES (11)"""
    assertEquals([[1], [2]], sql(pinned).sort(),
            "the pinned baseline must keep reading only its own partition")
    String unpinned = "SELECT k FROM spm_r26_p"
    assertTrue(!explainOf(unpinned).contains("SPM baseline hit: id=${pinnedId}"),
            "an unpinned caller must not reuse a pinned baseline: " + explainOf(unpinned))
    assertEquals([[1], [2], [11]], sql(unpinned).sort(),
            "the unpinned caller must see every partition")

    // ... and an unpinned baseline stays usable after the partition change
    long unpinnedId = createBaseline(unpinned, unpinned)
    assertTrue(explainOf(unpinned).contains("SPM baseline hit: id=${unpinnedId}"),
            "the unpinned baseline must be hit: " + explainOf(unpinned))
    assertEquals([[1], [2], [11]], sql(unpinned).sort())
}
