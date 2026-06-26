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

import java.util.Base64
import java.util.regex.Pattern
import groovy.json.JsonSlurper

suite("test_multi_stage_predicate_lm", "p0") {
    def getProfileList = {
        def dst = 'http://' + context.config.feHttpAddress
        def conn = new URL(dst + "/rest/v1/query_profile").openConnection()
        conn.setRequestMethod("GET")
        def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" +
                (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encoding}")
        return conn.getInputStream().getText()
    }

    def getProfile = { id ->
        def dst = 'http://' + context.config.feHttpAddress
        def conn = new URL(dst + "/api/profile/text/?query_id=$id").openConnection()
        conn.setRequestMethod("GET")
        def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" +
                (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encoding}")
        return conn.getInputStream().getText()
    }

    def getProfileWithToken = { token ->
        String profileId = ""
        int attempts = 0
        while (attempts < 10 && (profileId == null || profileId == "")) {
            List profileData = new JsonSlurper().parseText(getProfileList()).data.rows
            for (def profileItem in profileData) {
                if (profileItem["Sql Statement"].toString().contains(token)) {
                    profileId = profileItem["Profile ID"].toString()
                    break
                }
            }
            if (profileId == null || profileId == "") {
                Thread.sleep(300)
            }
            attempts++
        }
        assertTrue(profileId != null && profileId != "")
        Thread.sleep(800)
        return getProfile(profileId).toString()
    }

    def extractProfileBlockMetrics = { String profileText, String blockName ->
        List<String> lines = profileText.readLines()

        Map<String, String> metrics = [:]
        boolean inBlock = false
        int blockIndent = -1

        lines.each { line ->
            if (!inBlock) {
                def m = line =~ /^(\s*)\s+${Pattern.quote(blockName)}:/
                if (m.find()) {
                    inBlock = true
                    blockIndent = m.group(1).length()
                }
            } else {
                def indent = (line =~ /^(\s*)/)[0][1].length()
                if (indent > blockIndent) {
                    def kv = line =~ /^\s*-\s*([^:]+):\s*(.+)$/
                    if (kv.matches()) {
                        metrics[kv[0][1].trim()] = kv[0][2].trim()
                    }
                } else {
                    inBlock = false
                }
            }
        }

        return metrics
    }

    def parseLongOrZero = { String v ->
        if (v == null) {
            return 0L
        }
        def s = v.trim()
        if (s.isEmpty()) {
            return 0L
        }
        return Long.parseLong(s)
    }

    def tbl = "tbl_multi_stage_predicate_lm"

    sql """ DROP TABLE IF EXISTS ${tbl} """

    sql """
        CREATE TABLE IF NOT EXISTS ${tbl} (
            `k` INT NOT NULL,
            `a` INT NULL,
            `b` INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
        );
    """

    sql """
        INSERT INTO ${tbl}
        SELECT
            number AS k,
            number % 20 AS a,
            number % 3 AS b
        FROM numbers("number" = "200");
    """

    def vars = sql """ show variables like '%multi_stage_predicate%'; """
    assertTrue(vars.toString().contains("enable_multi_stage_predicate_lm"))

    // Baseline (feature off)
    sql """ set enable_multi_stage_predicate_lm = false; """
    sql """ set predicate_lm_stage1_cols = ''; """

    qt_baseline_cnt """ SELECT count(*) FROM ${tbl} WHERE a = 1 AND b = 2; """
    order_qt_baseline_rows """ SELECT k FROM ${tbl} WHERE a = 1 AND b = 2 ORDER BY k LIMIT 10; """

    // Enable profile for strong assertions
    sql """ set enable_profile=true; """

    // Feature on + choose stage1 cols via session variable
    sql """ set enable_multi_stage_predicate_lm = true; """
    sql """ set predicate_lm_stage1_cols = 'a'; """

    qt_lm_on_cnt """ SELECT count(*) FROM ${tbl} WHERE a = 1 AND b = 2; """
    order_qt_lm_on_rows """ SELECT k FROM ${tbl} WHERE a = 1 AND b = 2 ORDER BY k LIMIT 10; """

    // Strong assert 1: selective stage1 predicate => stage2 by rowids
    def tokenRowids = "test_multi_stage_predicate_lm_rowids_" + System.currentTimeMillis()
    sql """ SELECT /* ${tokenRowids} */ count(*) FROM ${tbl} WHERE a = 1 AND b = 2; """
    def profileRowids = getProfileWithToken(tokenRowids)
    def metricsRowids = extractProfileBlockMetrics(profileRowids, "SegmentIterator")

    assertTrue(metricsRowids.containsKey("PredicateLMStage2ByRowIdsBatches"),
            "Profile missing PredicateLMStage2ByRowIdsBatches\n" + profileRowids)
    assertTrue(metricsRowids.containsKey("PredicateLMStage2ByAllRowsBatches"),
            "Profile missing PredicateLMStage2ByAllRowsBatches\n" + profileRowids)

    assertTrue(parseLongOrZero(metricsRowids["PredicateLMStage2ByRowIdsBatches"]) > 0,
            "Expected stage2-by-rowids but got: " + metricsRowids.toString() + "\n" + profileRowids)

    // Strong assert 2: high-survival stage1 predicate => stage2 all-rows fallback
    // Use a < 19 so stage1 survival ratio ~= 0.95 (> default threshold 0.8) while still being a non-trivial predicate.
    def tokenAllRows = "test_multi_stage_predicate_lm_allrows_" + System.currentTimeMillis()
    sql """ SELECT /* ${tokenAllRows} */ count(*) FROM ${tbl} WHERE a < 19 AND b = 2; """
    def profileAllRows = getProfileWithToken(tokenAllRows)
    def metricsAllRows = extractProfileBlockMetrics(profileAllRows, "SegmentIterator")

    assertTrue(metricsAllRows.containsKey("PredicateLMStage2ByAllRowsBatches"),
            "Profile missing PredicateLMStage2ByAllRowsBatches\n" + profileAllRows)
    assertTrue(parseLongOrZero(metricsAllRows["PredicateLMStage2ByAllRowsBatches"]) > 0,
            "Expected stage2-by-all-rows but got: " + metricsAllRows.toString() + "\n" + profileAllRows)

    // Strong assert 3: threshold tuning should change stage2 strategy decision
    // 3.1 Force stage2-by-rowids even for a high-survival stage1 case
    sql """ set predicate_lm_stage1_survival_ratio_threshold = 1.0; """
    def tokenForceRowids = "test_multi_stage_predicate_lm_force_rowids_" + System.currentTimeMillis()
    sql """ SELECT /* ${tokenForceRowids} */ count(*) FROM ${tbl} WHERE a < 19 AND b = 2; """
    def profileForceRowids = getProfileWithToken(tokenForceRowids)
    def metricsForceRowids = extractProfileBlockMetrics(profileForceRowids, "SegmentIterator")

    assertTrue(metricsForceRowids.containsKey("PredicateLMStage2ByRowIdsBatches"),
            "Profile missing PredicateLMStage2ByRowIdsBatches\n" + profileForceRowids)
    assertTrue(parseLongOrZero(metricsForceRowids["PredicateLMStage2ByRowIdsBatches"]) > 0,
            "Expected stage2-by-rowids after setting threshold=1.0 but got: "
                    + metricsForceRowids.toString() + "\n" + profileForceRowids)

    // 3.2 Force stage2-by-all-rows even for a selective stage1 case
    sql """ set predicate_lm_stage1_survival_ratio_threshold = 0.0; """
    def tokenForceAllRows = "test_multi_stage_predicate_lm_force_allrows_" + System.currentTimeMillis()
    sql """ SELECT /* ${tokenForceAllRows} */ count(*) FROM ${tbl} WHERE a = 1 AND b = 2; """
    def profileForceAllRows = getProfileWithToken(tokenForceAllRows)
    def metricsForceAllRows = extractProfileBlockMetrics(profileForceAllRows, "SegmentIterator")

    assertTrue(metricsForceAllRows.containsKey("PredicateLMStage2ByAllRowsBatches"),
            "Profile missing PredicateLMStage2ByAllRowsBatches\n" + profileForceAllRows)
    assertTrue(parseLongOrZero(metricsForceAllRows["PredicateLMStage2ByAllRowsBatches"]) > 0,
            "Expected stage2-by-all-rows after setting threshold=0.0 but got: "
                    + metricsForceAllRows.toString() + "\n" + profileForceAllRows)

    // Reset to default for subsequent cases
    sql """ set predicate_lm_stage1_survival_ratio_threshold = 0.8; """

    // Stage1 cols parsing: whitespace/backticks/dedup should be tolerated
    sql """ set predicate_lm_stage1_cols = ' a ,`b`, a '; """
    qt_lm_on_cnt2 """ SELECT count(*) FROM ${tbl} WHERE a = 1 AND b = 2; """

    // Per-statement override via SET_VAR hint
    order_qt_lm_hint_rows """
        SELECT /*+ SET_VAR(enable_multi_stage_predicate_lm=true,predicate_lm_stage1_cols='a') */
               k
        FROM ${tbl}
        WHERE a = 1 AND b = 2
        ORDER BY k
        LIMIT 10;
    """

    // Unknown stage1 cols should be ignored (do not fail the query).
    def invalidStage1ColsRes = sql """
        SELECT /*+ SET_VAR(enable_multi_stage_predicate_lm=true,predicate_lm_stage1_cols='not_exist') */
               count(*)
        FROM ${tbl}
        WHERE a = 1 AND b = 2;
    """
    assertEquals("3", invalidStage1ColsRes[0][0].toString())

    // Qualified stage1 cols: support `table.col` and `db.table.col` scoping
    def tblQual = "tbl_multi_stage_predicate_lm_qual"

    sql """ DROP TABLE IF EXISTS ${tblQual} """

    sql """
        CREATE TABLE IF NOT EXISTS ${tblQual} (
            `k` INT NOT NULL,
            `a` INT NULL,
            `b` INT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
        );
    """

    // Make `a` non-selective and `b` selective to differentiate stage1 choice.
    // Rows: 10000, a is always 1, b alternates 0/1.
    sql """
        INSERT INTO ${tblQual}
        SELECT
            number AS k,
            1 AS a,
            number % 2 AS b
        FROM numbers("number" = "10000");
    """

    def currentDbRes = sql """ SELECT database(); """
    def currentDb = currentDbRes[0][0].toString()

    // table-qualified matches current table => stage1=b (survival_ratio ~= 0.5) => stage2-by-rowids
    sql """ set predicate_lm_stage1_survival_ratio_threshold = 0.8; """
    sql """ set predicate_lm_stage1_cols = '${tblQual}.b'; """

    def tokenTableQualified = "test_multi_stage_predicate_lm_table_qualified_" + System.currentTimeMillis()
    def cntTableQualified = sql """ SELECT /* ${tokenTableQualified} */ count(*) FROM ${tblQual} WHERE a = 1 AND b = 0; """
    assertEquals("5000", cntTableQualified[0][0].toString())

    def profileTableQualified = getProfileWithToken(tokenTableQualified)
    def metricsTableQualified = extractProfileBlockMetrics(profileTableQualified, "SegmentIterator")
    assertTrue(metricsTableQualified.containsKey("PredicateLMStage2ByRowIdsBatches"),
            "Profile missing PredicateLMStage2ByRowIdsBatches\n" + profileTableQualified)
    assertTrue(parseLongOrZero(metricsTableQualified["PredicateLMStage2ByRowIdsBatches"]) > 0,
            "Expected stage2-by-rowids for table-qualified stage1 cols but got: "
                    + metricsTableQualified.toString() + "\n" + profileTableQualified)

    // mismatched table qualifier should be ignored.
    // When stage1 cols are not explicitly applicable and there is no runtime filter,
    // the implementation falls back to single-stage behavior (no stage2).
    sql """ set predicate_lm_stage1_cols = 'other_tbl.b'; """
    
    def tokenTableMismatch = "test_multi_stage_predicate_lm_table_mismatch_" + System.currentTimeMillis()
    def cntTableMismatch = sql """ SELECT /* ${tokenTableMismatch} */ count(*) FROM ${tblQual} WHERE a = 1 AND b = 0; """
    assertEquals("5000", cntTableMismatch[0][0].toString())
    
    def profileTableMismatch = getProfileWithToken(tokenTableMismatch)
    def metricsTableMismatch = extractProfileBlockMetrics(profileTableMismatch, "SegmentIterator")
    assertTrue(metricsTableMismatch.containsKey("PredicateLMStage2ByAllRowsBatches"),
            "Profile missing PredicateLMStage2ByAllRowsBatches\n" + profileTableMismatch)
    assertTrue(metricsTableMismatch.containsKey("PredicateLMStage2ByRowIdsBatches"),
            "Profile missing PredicateLMStage2ByRowIdsBatches\n" + profileTableMismatch)
    
    assertEquals(0L, parseLongOrZero(metricsTableMismatch["PredicateLMStage2ByAllRowsBatches"]),
            "Expected no stage2 (single-stage fallback) but got: "
                    + metricsTableMismatch.toString() + "\n" + profileTableMismatch)
    assertEquals(0L, parseLongOrZero(metricsTableMismatch["PredicateLMStage2ByRowIdsBatches"]),
            "Expected no stage2 (single-stage fallback) but got: "
                    + metricsTableMismatch.toString() + "\n" + profileTableMismatch)

    // db.table-qualified matches current db/table => stage1=b => stage2-by-rowids
    sql """ set predicate_lm_stage1_cols = '${currentDb}.${tblQual}.b'; """

    def tokenDbTableQualified = "test_multi_stage_predicate_lm_db_table_qualified_" + System.currentTimeMillis()
    def cntDbTableQualified = sql """ SELECT /* ${tokenDbTableQualified} */ count(*) FROM ${tblQual} WHERE a = 1 AND b = 0; """
    assertEquals("5000", cntDbTableQualified[0][0].toString())

    def profileDbTableQualified = getProfileWithToken(tokenDbTableQualified)
    def metricsDbTableQualified = extractProfileBlockMetrics(profileDbTableQualified, "SegmentIterator")
    assertTrue(metricsDbTableQualified.containsKey("PredicateLMStage2ByRowIdsBatches"),
            "Profile missing PredicateLMStage2ByRowIdsBatches\n" + profileDbTableQualified)
    assertTrue(parseLongOrZero(metricsDbTableQualified["PredicateLMStage2ByRowIdsBatches"]) > 0,
            "Expected stage2-by-rowids for db.table-qualified stage1 cols but got: "
                    + metricsDbTableQualified.toString() + "\n" + profileDbTableQualified)

    // Reset for cleanliness
    sql """ set predicate_lm_stage1_cols = ''; """
    sql """ set enable_multi_stage_predicate_lm = false; """
    sql """ set predicate_lm_stage1_survival_ratio_threshold = 0.8; """
    sql """ set enable_profile = false; """

}
