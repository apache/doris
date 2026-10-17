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

    // End-to-end checks for
    //  - SELECT-hint payloads in the baseline match: SET_VAR(time_zone='+08:00') and
    //    SET_VAR(time_zone='-08:00') are different queries (the hint changes the result);
    //    the variant must NOT replay the baseline captured under the other setting
    //  - the SHOW BASELINE PLANS LIKE literal decoding: a pattern with an escaped quote
    //    ('%a''''b%' -> %a''b%) must reach PatternMatcher decoded and find the stored SQL

    sql """set enable_spm_rewrite = true"""
    sql """set enable_spm_fallback = false"""

    sql """DROP TABLE IF EXISTS spm_r12_t"""
    sql """
        CREATE TABLE spm_r12_t (k INT, s VARCHAR(16))
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO spm_r12_t VALUES (1, 'a''b'), (2, 'xy'), (3, 'z')"""

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
