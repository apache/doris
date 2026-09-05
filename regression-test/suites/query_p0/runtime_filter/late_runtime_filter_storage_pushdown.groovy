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

suite("late_runtime_filter_storage_pushdown", "nonConcurrent") {
    sql "set enable_profile = true"
    sql "set profile_level = 2"
    sql "set enable_runtime_filter_prune = false"
    sql "set runtime_filter_mode = 'GLOBAL'"
    sql "set runtime_filter_wait_infinitely = false"
    sql "set runtime_filter_wait_time_ms = 0"
    sql "set disable_join_reorder = true"
    sql "set parallel_pipeline_task_num = 1"
    sql "set batch_size = 1024"
    sql "set runtime_filter_max_in_num = 1024"
    sql "set enable_common_expr_pushdown = true"

    sql "drop table if exists late_rf_storage_probe"
    // Keep the probe table's physical distribution different from the join key so the
    // shuffle join places its scan in a different fragment. The resulting RF target is
    // remote and honors runtime_filter_wait_time_ms = 0.
    sql """
        CREATE TABLE late_rf_storage_probe (
            id INT NOT NULL,
            payload VARCHAR(128) NOT NULL
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
    sql """
        INSERT INTO late_rf_storage_probe
        SELECT CAST(number AS INT), REPEAT('x', 64)
        FROM numbers("number" = "262144")
    """

    sql "drop table if exists late_rf_storage_build"
    sql """
        CREATE TABLE late_rf_storage_build (
            id INT NOT NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        )
    """
    sql "insert into late_rf_storage_build values (65536)"
    sql "sync"

    def profileAction = new ProfileAction(context)
    def lateRfCounters = [
        "PublishedLateRuntimeFilters",
        "LateRuntimeFiltersInstalled",
        "LateRuntimeFiltersInstalledAfterLazyInit",
        "RowsLateRuntimeFilterRowFiltered",
        "RowsLateRuntimeFilterZoneMapFiltered"
    ]

    def counterSum = { String profile, String counterName ->
        Pattern pattern = Pattern.compile(Pattern.quote(counterName) + ":\\s*([0-9,]+)")
        Matcher matcher = pattern.matcher(profile)
        long sum = 0
        while (matcher.find()) {
            sum += Long.parseLong(matcher.group(1).replace(",", ""))
        }
        return sum
    }

    def assertLateRuntimeFilterApplied = { String token, boolean expectPagePruning ->
        def requiredCounters = lateRfCounters + [
            "ExprZoneMapFilteredPages",
            "reached_timeout: true"
        ]
        String profile = profileAction.getProfileBySql(token, requiredCounters)
        assertTrue(profile.contains("reached_timeout: true"),
                "Runtime filter should reach the zero wait timeout for ${token}")
        [
            "PublishedLateRuntimeFilters",
            "LateRuntimeFiltersInstalled",
            "LateRuntimeFiltersInstalledAfterLazyInit"
        ].each { String counterName ->
            long value = counterSum(profile, counterName)
            logger.info("Late RF profile [${token}]: ${counterName}=${value}")
            assertTrue(value > 0, "${counterName} should be positive for ${token}")
        }

        long rowFilteredRows = counterSum(profile, "RowsLateRuntimeFilterRowFiltered")
        logger.info("Late RF profile [${token}]: " +
                "RowsLateRuntimeFilterRowFiltered=${rowFilteredRows}")
        long zonemapFilteredRows = counterSum(profile, "RowsLateRuntimeFilterZoneMapFiltered")
        logger.info("Late RF profile [${token}]: " +
                "RowsLateRuntimeFilterZoneMapFiltered=${zonemapFilteredRows}")
        if (expectPagePruning) {
            assertTrue(zonemapFilteredRows > 0,
                    "RowsLateRuntimeFilterZoneMapFiltered should be positive for ${token}")
        } else {
            assertEquals(0L, zonemapFilteredRows,
                    "RowsLateRuntimeFilterZoneMapFiltered should remain zero for ${token}")
        }

        long filteredPages = counterSum(profile, "ExprZoneMapFilteredPages")
        logger.info("Late RF profile [${token}]: ExprZoneMapFilteredPages=${filteredPages}")
        if (expectPagePruning) {
            assertTrue(filteredPages > 0,
                    "ExprZoneMapFilteredPages should be positive for ${token}")
        } else {
            assertEquals(0L, filteredPages,
                    "ExprZoneMapFilteredPages should remain zero for ${token}")
        }
    }

    def assertLateRuntimeFilterKeptAboveStorage = { String token ->
        def requiredCounters = lateRfCounters + ["reached_timeout: true"]
        String profile = profileAction.getProfileBySql(token, requiredCounters)
        assertTrue(profile.contains("reached_timeout: true"),
                "Runtime filter should reach the zero wait timeout for ${token}")
        lateRfCounters.each { String counterName ->
            long value = counterSum(profile, counterName)
            logger.info("Late RF fallback profile [${token}]: ${counterName}=${value}")
            assertEquals(0L, value,
                    "${counterName} should remain zero for a non-key AGG value column")
        }
    }

    // The build-side sleep keeps the RF unresolved when the probe scan opens. Sleeping on the
    // first probe row then backpressures the scanner long enough for the RF to arrive before all
    // remaining pages are read. LateRuntimeFiltersInstalledAfterLazyInit proves that installation
    // happened at a later SegmentIterator batch boundary rather than during initial lazy init.
    sql "set enable_expr_zonemap_filter = true"
    def minMaxToken = "late_rf_storage_min_max_" + UUID.randomUUID().toString()
    def minMaxSql = """
        SELECT /*+ SET_VAR(runtime_filter_type='MIN_MAX') */ /* ${minMaxToken} */
               COUNT(*), COALESCE(SUM(LENGTH(p.payload)), 0)
        FROM late_rf_storage_probe p
        JOIN [shuffle] (
            SELECT id
            FROM late_rf_storage_build
            WHERE SLEEP(2) = 0
        ) b ON p.id = b.id
        WHERE SLEEP(IF(p.id = 0, 4, 0)) = 0
    """
    order_qt_late_rf_storage_min_max minMaxSql
    assertLateRuntimeFilterApplied(minMaxToken, true)

    // VERSION_COL stores a placeholder 0 in each single-version segment and is replaced with the
    // rowset version while reading. A late RF on this hidden column must not prune remaining pages
    // with the placeholder's physical zone map.
    sql "drop table if exists late_rf_storage_version_probe"
    sql """
        CREATE TABLE late_rf_storage_version_probe (
            id INT NOT NULL,
            payload VARCHAR(128) NOT NULL
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true",
            "storage_page_size" = "4096",
            "compression" = "no_compression"
        )
    """
    sql """
        INSERT INTO late_rf_storage_version_probe
        SELECT CAST(number AS INT), REPEAT('v', 64)
        FROM numbers("number" = "8192")
    """
    sql "sync"

    sql "drop table if exists late_rf_storage_version_build"
    sql """
        CREATE TABLE late_rf_storage_version_build (
            v BIGINT NOT NULL
        )
        DUPLICATE KEY(v)
        DISTRIBUTED BY HASH(v) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        )
    """
    sql "set show_hidden_columns = true"
    sql """
        INSERT INTO late_rf_storage_version_build (v)
        SELECT DISTINCT __DORIS_VERSION_COL__
        FROM late_rf_storage_version_probe
    """
    sql "sync"

    def versionToken = "late_rf_storage_version_" + UUID.randomUUID().toString()
    def versionSql = """
        SELECT /*+ SET_VAR(runtime_filter_type='MIN_MAX') */ /* ${versionToken} */
               COUNT(*), COALESCE(SUM(LENGTH(p.payload)), 0)
        FROM late_rf_storage_version_probe p
        JOIN [shuffle] (
            SELECT v
            FROM late_rf_storage_version_build
            WHERE SLEEP(2) = 0
        ) b ON p.__DORIS_VERSION_COL__ = b.v
        WHERE SLEEP(IF(p.id = 0, 4, 0)) = 0
    """
    order_qt_late_rf_storage_version versionSql
    assertLateRuntimeFilterApplied(versionToken, false)

    // The same filter ready at scan open is normalized into an ordinary column predicate on
    // __DORIS_VERSION_COL__. Segment and page pruning must evaluate it against the read-time
    // version rather than the physical [0,0] placeholder zonemap.
    sql "set runtime_filter_wait_infinitely = true"
    def versionReadyToken = "late_rf_storage_version_ready_" + UUID.randomUUID().toString()
    def versionReadySql = """
        SELECT /*+ SET_VAR(runtime_filter_type='MIN_MAX') */ /* ${versionReadyToken} */
               COUNT(*), COALESCE(SUM(LENGTH(p.payload)), 0)
        FROM late_rf_storage_version_probe p
        JOIN [shuffle] late_rf_storage_version_build b ON p.__DORIS_VERSION_COL__ = b.v
    """
    order_qt_late_rf_storage_version_ready_at_open versionReadySql
    sql "set runtime_filter_wait_infinitely = false"
    String versionReadyProfile = profileAction.getProfileBySql(versionReadyToken, lateRfCounters)
    assertEquals(0L, counterSum(versionReadyProfile, "PublishedLateRuntimeFilters"),
            "A filter ready at scan open must not be published as a late runtime filter")
    sql "set show_hidden_columns = false"

    // A value column of an AGG_KEYS table is merged above SegmentIterator. Its late RF must stay
    // above storage: filtering the raw values 10 and 20 before SUM would incorrectly remove the
    // visible value 30.
    sql "drop table if exists late_rf_storage_agg_probe"
    sql """
        CREATE TABLE late_rf_storage_agg_probe (
            k INT NOT NULL,
            v INT SUM NOT NULL
        )
        AGGREGATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "storage_page_size" = "4096",
            "compression" = "no_compression"
        )
    """
    sql """
        INSERT INTO late_rf_storage_agg_probe
        SELECT CAST(number AS INT), 10
        FROM numbers("number" = "8192")
    """
    sql """
        INSERT INTO late_rf_storage_agg_probe
        SELECT CAST(number AS INT), 20
        FROM numbers("number" = "8192")
    """

    sql "drop table if exists late_rf_storage_agg_build"
    sql """
        CREATE TABLE late_rf_storage_agg_build (
            id INT NOT NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        )
    """
    sql "insert into late_rf_storage_agg_build values (30)"
    sql "sync"

    def aggValueToken = "late_rf_storage_agg_value_" + UUID.randomUUID().toString()
    def aggValueSql = """
        SELECT /*+ SET_VAR(runtime_filter_type='IN') */ /* ${aggValueToken} */
               COUNT(*), COALESCE(SUM(p.v), 0)
        FROM late_rf_storage_agg_probe p
        JOIN [shuffle] (
            SELECT id
            FROM late_rf_storage_agg_build
            WHERE SLEEP(1) = 0
        ) b ON p.v = b.id
        WHERE SLEEP(IF(p.k = 0, 2, 0)) = 0
    """
    order_qt_late_rf_storage_agg_value_fallback aggValueSql
    assertLateRuntimeFilterKeptAboveStorage(aggValueToken)
}
