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

suite("test_spm_review_round12", "spm") {

    // Twelfth review round: end-to-end checks for
    //  - the checkpoint column layout: the durable UPSERT must address its columns by
    //    NAME (a table upgraded from the old layout carries cursor_tail after update_time,
    //    where a positional INSERT wrote the tail JSON into failed_attempts), and the
    //    physical order of the checkpoint table must be the canonical one
    //  - SELECT-hint payloads in the baseline match: SET_VAR(time_zone='+08:00') and
    //    SET_VAR(time_zone='-08:00') are different queries (the hint changes the result);
    //    the variant must NOT replay the baseline captured under the other setting
    //  - the SHOW BASELINE PLANS LIKE literal decoding: a pattern with an escaped quote
    //    ('%a''''b%' -> %a''b%) must reach PatternMatcher decoded and find the stored SQL

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    sql """DROP TABLE IF EXISTS spm_r12_t"""
    sql """DROP TABLE IF EXISTS spm_r12_ckpt"""
    sql """
        CREATE TABLE spm_r12_t (k INT, s VARCHAR(16))
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r12_t VALUES (1, 'a''b'), (2, 'xy'), (3, 'z')"""
    // The "upgraded" physical layout: cursor_tail sits LAST (the old upgrade APPENDED it
    // after update_time) - the production UPSERT must not bind its values by position
    sql """
        CREATE TABLE spm_r12_ckpt (
            id BIGINT NOT NULL,
            last_scan_timestamp BIGINT NOT NULL,
            pending_window_start BIGINT NOT NULL,
            pending_window_end BIGINT NOT NULL,
            cursor_query_time BIGINT NOT NULL,
            cursor_time VARCHAR(4096) NOT NULL,
            cursor_query_id VARCHAR(1024) NOT NULL,
            failed_attempts STRING NOT NULL,
            retry_queue STRING NOT NULL,
            update_time DATETIME NOT NULL,
            cursor_tail VARCHAR(4096) NOT NULL
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")
    """

    def ownBaselines = {
        sql("""SHOW BASELINE PLANS""").findAll { it[1].toString().contains("spm_r12_") }
    }
    def dropOwnBaselines = {
        ownBaselines().each { row ->
            sql """DROP BASELINE PLAN ${row[0]}"""
        }
    }
    dropOwnBaselines()
    assertEquals(0, ownBaselines().size(), "no spm_r12_ baseline should be left after cleanup")

    def explainOf = { String query -> sql("""EXPLAIN ${query}""").toString() }
    def createBaseline = { String text ->
        (sql('CREATE GLOBAL BASELINE PLAN "' + text + '" WITH "' + text + '"')[0][0] as Long)
    }

    // ==================== R12-1: the checkpoint UPSERT is layout-independent ====================

    // The exact column list of PlanCaptureManager#CHECKPOINT_INSERT_SQL against the
    // reordered layout: every value must land in the NAMED column, so cursor_tail keeps
    // the tail JSON even though it is physically the last column.
    sql """
        INSERT INTO spm_r12_ckpt
            (`id`, `last_scan_timestamp`, `pending_window_start`, `pending_window_end`,
             `cursor_query_time`, `cursor_time`, `cursor_query_id`, `cursor_tail`,
             `failed_attempts`, `retry_queue`, `update_time`)
        VALUES (1, 1700000000000, 11, 22, 33, '2024-05-01 00:00:00', 'qid-tail-check',
                '{"page":7}', '2', '["r1"]', NOW())
    """
    List<Object> ckptRow = sql("""SELECT cursor_tail, failed_attempts, retry_queue, update_time
            FROM spm_r12_ckpt WHERE id = 1""")[0]
    assertEquals('{"page":7}', ckptRow[0].toString(),
            "cursor_tail must keep the tail JSON on a table whose physical order differs:"
                    + " a positional INSERT wrote it into failed_attempts instead")
    assertEquals('2', ckptRow[1].toString(),
            "failed_attempts must keep the retry count, not the tail JSON")
    assertEquals('["r1"]', ckptRow[2].toString(),
            "retry_queue must keep the retry JSON, not a shifted value")
    assertNotNull(ckptRow[3], "update_time must still be filled by NOW()")

    // The durable statement UPSERTs by key: re-inserting id = 1 replaces the row.
    sql """
        INSERT INTO spm_r12_ckpt
            (`id`, `last_scan_timestamp`, `pending_window_start`, `pending_window_end`,
             `cursor_query_time`, `cursor_time`, `cursor_query_id`, `cursor_tail`,
             `failed_attempts`, `retry_queue`, `update_time`)
        VALUES (1, 1700000000001, 11, 22, 33, '2024-05-01 00:00:00', 'qid-tail-check-2',
                '{"page":9}', '0', '[]', NOW())
    """
    List<Object> replaced = sql("""SELECT cursor_query_id, cursor_tail, failed_attempts
            FROM spm_r12_ckpt WHERE id = 1""")[0]
    assertEquals('qid-tail-check-2', replaced[0].toString(), "the row must be replaced in place")
    assertEquals('{"page":9}', replaced[1].toString(), "the tail must follow the replacement")
    assertEquals('0', replaced[2].toString(), "the retry count must follow the replacement")

    // The REAL checkpoint table must carry the canonical physical order: the upgrade adds
    // cursor_tail AFTER cursor_query_id (before failed_attempts) and scan_zone AFTER
    // exclude_pattern (before update_time), like a fresh create.
    List<String> ckptColumns = sql(
            "SHOW COLUMNS FROM __internal_schema.spm_capture_checkpoint")
                    .collect { it[0].toString().toLowerCase() }
    assertEquals(["id", "last_scan_timestamp", "pending_window_start", "pending_window_end",
            "cursor_query_time", "cursor_time", "cursor_query_id", "cursor_tail",
            "failed_attempts", "retry_queue", "min_query_time_ms", "min_scan_rows",
            "include_pattern", "exclude_pattern", "scan_zone", "update_time"], ckptColumns,
            "the checkpoint layout must match the canonical schema order (see"
                    + " InternalSchema.SPM_CAPTURE_CHECKPOINT_SCHEMA)")

    // ==================== R12-2: hint payloads are part of the baseline match ====================

    // A baseline captured WITH a SET_VAR hint serves the same SQL with the same hint ...
    String tzPlus = "SELECT /*+ SET_VAR(time_zone='+08:00') */ k FROM spm_r12_t ORDER BY k"
    long tzPlusId = createBaseline(tzPlus)
    assertTrue(explainOf(tzPlus).contains("SPM baseline hit: id=${tzPlusId}"),
            "the identical hint must still replay the baseline: " + explainOf(tzPlus))

    // ... but NOT the same SQL with a different setting: time_zone changes the RESULT,
    // so the replay would carry the CREATOR's frozen expression / plan under the caller's
    // time zone.
    String tzMinus = "SELECT /*+ SET_VAR(time_zone='-08:00') */ k FROM spm_r12_t ORDER BY k"
    String tzMinusExplain = explainOf(tzMinus)
    assertFalse(tzMinusExplain.contains("SPM baseline hit: id=${tzPlusId}"),
            "a different time_zone must not match the baseline: " + tzMinusExplain)
    assertFalse(tzMinusExplain.contains("SPM baseline hit"),
            "no baseline at all may serve the -08:00 variant: " + tzMinusExplain)
    assertEquals(3, sql(tzMinus).size(), "the unhit variant must execute normally")

    // sql_mode is result-affecting the same way (a captured PIPES_AS_CONCAT query must
    // not be replayed under ONLY_FULL_GROUP_BY semantics).
    String modeConcat = "SELECT /*+ SET_VAR(sql_mode='PIPES_AS_CONCAT') */ k FROM spm_r12_t WHERE k > 0 ORDER BY k"
    long modeConcatId = createBaseline(modeConcat)
    assertTrue(explainOf(modeConcat).contains("SPM baseline hit: id=${modeConcatId}"),
            "the identical sql_mode hint must still replay: " + explainOf(modeConcat))
    String modeOther = "SELECT /*+ SET_VAR(sql_mode='ONLY_FULL_GROUP_BY') */ k FROM spm_r12_t WHERE k > 0 ORDER BY k"
    assertFalse(explainOf(modeOther).contains("SPM baseline hit: id=${modeConcatId}"),
            "a different sql_mode must not match: " + explainOf(modeOther))

    // A query without the hint is a different query as well, and the replayed result must
    // agree with the direct execution for the matching variant.
    String noHint = "SELECT k FROM spm_r12_t ORDER BY k"
    assertFalse(explainOf(noHint).contains("SPM baseline hit: id=${tzPlusId}"),
            "an unhinted query must not match the hinted baseline")
    sql """set enable_spm_rewrite = false"""
    List<List<Object>> directTzPlus = sql(tzPlus)
    sql """set enable_spm_rewrite = true"""
    List<List<Object>> replayedTzPlus = sql(tzPlus)
    assertEquals(directTzPlus.sort(), replayedTzPlus.sort(),
            "the hinted replay must agree with the direct execution")

    // ==================== R12-3: SHOW BASELINE PLANS LIKE decodes the literal ====================

    // The bind text stores the doubled quote exactly as typed ('a''b'); the LIKE literal
    // '%a''''b%' decodes to the pattern %a''b% and must find that row. Before the fix the
    // pattern kept its escape characters and never matched.
    String quotedSql = "SELECT k FROM spm_r12_t WHERE s = 'a''b' ORDER BY k"
    long quotedId = createBaseline(quotedSql)
    List<List<Object>> likeHit = sql("SHOW BASELINE PLANS LIKE '%a''''b%'")
    assertTrue(likeHit.any { it[0].toString() == quotedId.toString() },
            "the decoded pattern must find the baseline storing 'a''b', got: " + likeHit)
    List<List<Object>> likeAny = sql("SHOW BASELINE PLANS LIKE '%spm_r12_t%'")
    assertTrue(likeAny.any { it[0].toString() == quotedId.toString() },
            "a plain pattern must keep working, got: " + likeAny)

    // ==================== deterministic results ====================

    order_qt_spm12_tz_hint """SELECT k FROM spm_r12_t ORDER BY k"""
    order_qt_spm12_like_quote """SELECT k FROM spm_r12_t WHERE s = 'a''b' ORDER BY k"""
}
