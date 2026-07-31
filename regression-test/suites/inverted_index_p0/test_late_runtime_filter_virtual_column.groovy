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

import java.util.regex.Matcher
import java.util.regex.Pattern
import org.apache.doris.regression.action.ProfileAction

suite("test_late_runtime_filter_virtual_column", "nonConcurrent") {
    sql "set enable_profile = true"
    sql "set profile_level = 2"
    sql "set enable_runtime_filter_prune = false"
    sql "set runtime_filter_mode = 'GLOBAL'"
    sql "set runtime_filter_wait_infinitely = false"
    sql "set runtime_filter_wait_time_ms = 0"
    sql "set disable_join_reorder = true"
    sql "set parallel_pipeline_task_num = 1"
    sql "set enable_parallel_scan = false"
    sql "set batch_size = 1024"
    sql "set runtime_filter_max_in_num = 1024"
    sql "set enable_common_expr_pushdown = true"

    sql "drop table if exists late_rf_virtual_column_probe"
    sql """
        CREATE TABLE late_rf_virtual_column_probe (
            id INT NOT NULL,
            content TEXT NOT NULL,
            payload VARCHAR(128) NOT NULL,
            INDEX idx_content (content) USING INVERTED PROPERTIES("parser" = "english")
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(payload) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "storage_page_size" = "4096",
            "compression" = "no_compression"
        )
    """
    // Keep two independent rowsets so the RF can arrive after the first SegmentIterator is lazily
    // initialized but before the second SegmentIterator is initialized.
    sql """
        INSERT INTO late_rf_virtual_column_probe
        SELECT CAST(number AS INT),
               IF(number % 2 = 0, 'hello world', 'other text'),
               REPEAT('x', 64)
        FROM numbers("number" = "4096")
    """
    sql """
        INSERT INTO late_rf_virtual_column_probe
        SELECT CAST(number + 4096 AS INT),
               IF(number % 2 = 0, 'hello world', 'other text'),
               REPEAT('x', 64)
        FROM numbers("number" = "4096")
    """

    sql "drop table if exists late_rf_virtual_column_build"
    sql """
        CREATE TABLE late_rf_virtual_column_build (
            matched BOOLEAN NOT NULL
        )
        DUPLICATE KEY(matched)
        DISTRIBUTED BY HASH(matched) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        )
    """
    sql "insert into late_rf_virtual_column_build values (true)"
    sql "sync"

    def token = "late_rf_virtual_column_" + UUID.randomUUID().toString()
    def querySql = """
        SELECT /*+ SET_VAR(runtime_filter_type='IN') */ /* ${token} */ COUNT(*)
        FROM (
            SELECT id, content MATCH_ANY 'hello' AS matched
            FROM late_rf_virtual_column_probe
        ) p
        JOIN [shuffle] (
            SELECT matched
            FROM late_rf_virtual_column_build
            WHERE SLEEP(2) = 0
        ) b ON p.matched = b.matched
        WHERE SLEEP(IF(p.id = 0, 4, 0)) = 0
    """

    def explainResult = sql "EXPLAIN VERBOSE ${querySql}"
    def explainText = explainResult.collect { it.toString() }.join("\n")
    assertTrue(explainText.contains("virtualColumn=") &&
            explainText.contains("__DORIS_VIRTUAL_COL__"),
            "MATCH projection should be materialized as a virtual column")
    assertTrue(explainText.contains("MATCH_ANY"),
            "Runtime filter target should retain the MATCH expression")
    assertTrue(explainText.readLines().any { String line ->
        String lowerLine = line.toLowerCase()
        return lowerLine.contains("runtime filters:") &&
                lowerLine.contains("[in]") &&
                line.contains("->") &&
                line.contains("MATCH_ANY")
    }, "Join should generate an IN runtime filter targeting the virtual column")

    // VMatchPredicate.execute fails only when MATCH falls back to row evaluation. A late RF over a
    // virtual slot must therefore stay in Scanner residual conjuncts, where it reads the canonical
    // virtual column after SegmentIterator has materialized the inverted-index result.
    try {
        GetDebugPoint().enableDebugPointForAllBEs("VMatchPredicate.execute")
        order_qt_late_rf_virtual_column querySql
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("VMatchPredicate.execute")
    }

    def counterSum = { String profile, String counterName ->
        Pattern pattern = Pattern.compile(Pattern.quote(counterName) + ":\\s*([0-9,]+)")
        Matcher matcher = pattern.matcher(profile)
        long sum = 0
        while (matcher.find()) {
            sum += Long.parseLong(matcher.group(1).replace(",", ""))
        }
        return sum
    }

    def profileAction = new ProfileAction(context)
    String profile = profileAction.getProfileBySql(token, [
        "PublishedLateRuntimeFilters",
        "LateRuntimeFiltersInstalled",
        "LateRuntimeFiltersInstalledAfterLazyInit",
        "RowsLateRuntimeFilterRowFiltered",
        "RowsLateRuntimeFilterZoneMapFiltered",
        "FilterRows",
        "reached_timeout: true"
    ])
    assertTrue(profile.contains("reached_timeout: true"),
            "The RF should time out at scan open and arrive later")

    long published = counterSum(profile, "PublishedLateRuntimeFilters")
    long installed = counterSum(profile, "LateRuntimeFiltersInstalled")
    long installedAfterLazyInit =
            counterSum(profile, "LateRuntimeFiltersInstalledAfterLazyInit")
    long filteredRows = counterSum(profile, "RowsLateRuntimeFilterRowFiltered")
    long zonemapFilteredRows = counterSum(profile, "RowsLateRuntimeFilterZoneMapFiltered")
    Pattern rfFilterRowsPattern = Pattern.compile("RF\\d+ FilterRows:\\s*([0-9,]+)")
    Matcher rfFilterRowsMatcher = rfFilterRowsPattern.matcher(profile)
    long residualFilteredRows = 0
    while (rfFilterRowsMatcher.find()) {
        residualFilteredRows +=
                Long.parseLong(rfFilterRowsMatcher.group(1).replace(",", ""))
    }
    logger.info("Late virtual RF profile: published=${published}, installed=${installed}, " +
            "installedAfterLazyInit=${installedAfterLazyInit}, filteredRows=${filteredRows}, " +
            "zonemapFilteredRows=${zonemapFilteredRows}, " +
            "residualFilteredRows=${residualFilteredRows}")
    assertEquals(0L, published, "A virtual late RF must not be published to storage")
    assertEquals(0L, installed, "A virtual late RF must not be installed in SegmentIterator")
    assertEquals(0L, installedAfterLazyInit,
            "A virtual late RF must not be installed after SegmentIterator lazy initialization")
    assertEquals(0L, filteredRows,
            "A virtual late RF must not contribute SegmentIterator row-filter counters")
    assertEquals(0L, zonemapFilteredRows,
            "A virtual late RF must not contribute SegmentIterator zone-map counters")
    assertTrue(residualFilteredRows > 0,
            "The virtual late RF should still filter rows as a Scanner residual conjunct")
}
