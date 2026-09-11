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

import org.junit.Assert;

/**
 * An MTMV built over a VIEW keeps the view's query-side guard (cacheGuard=false) in its definition plan.
 * When a cross-zone query needs the guarded rewrite cache, the cache builder must retain a cache-mismatch
 * marker AROUND that existing non-cache guard - otherwise the cache plan has no cache guard, the
 * isCacheGuard() rejection never fires, and FORCE_IN_RBO substitutes a day boundary materialized in the
 * MV's refresh session into a query evaluated in a different zone.
 *
 * Three pairwise-distinct zones are used: the view is created in +08:00, the MTMV over it is
 * created/refreshed in +00:00 (UTC), and the query runs in -05:00. For the instant 2024-01-01 02:30Z the
 * UTC day is 2024-01-01 while the -05:00 day is 2023-12-31, so substituting the UTC-materialized value
 * into the -05:00 query is wrong.
 */
suite("test_timestamptz_mtmv_view_three_zone_rewrite","mtmv") {
    def dbName = "timestamptz_mtmv_view_three_zone_rewrite"
    def tableName = "timestamptz_mtmv_view_three_zone_rewrite_table"
    def viewName = "timestamptz_mtmv_view_three_zone_rewrite_view"
    def mvName = "timestamptz_mtmv_view_three_zone_rewrite_mv"

    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE ${dbName}"
    sql "USE ${dbName}"

    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"

    def viewTz = '+08:00'    // view creation zone
    def mvTz = '+00:00'      // MTMV creation/refresh zone
    def queryTz = '-05:00'   // query zone

    // Base table.
    sql "SET time_zone = '${viewTz}'"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            ts TIMESTAMPTZ(6) NOT NULL,
            v INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num' = '1')
    """
    // The instant 2024-01-01 02:30Z is day 2024-01-01 in both +08:00 and +00:00, but day 2023-12-31 in
    // -05:00 (it renders as 2023-12-31 21:30-05:00).
    sql "INSERT INTO ${tableName} VALUES (1, '2024-01-01 02:30:00+00:00', 10)"
    sql "sync"

    // View created in +08:00: expanding it in any other session wraps the zone-sensitive date_trunc output
    // in a query-side (non-cache) view guard.
    sql "SET time_zone = '${viewTz}'"
    sql "DROP VIEW IF EXISTS ${viewName}"
    sql "CREATE VIEW ${viewName} AS SELECT id, ts, date_trunc(ts, 'day') AS d FROM ${tableName}"

    // MTMV over the view, created/refreshed in UTC: its definition plan carries the view's non-cache guard.
    sql "SET time_zone = '${mvTz}'"
    sql "DROP MATERIALIZED VIEW IF EXISTS ${mvName}"
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES('replication_num' = '1')
        AS
        SELECT id, d
        FROM ${viewName}
    """
    waitingMTMVTaskFinishedByMvName(mvName, dbName)

    // The MV materializes the view's day in the refresh (UTC) session: 2024-01-01.
    sql "SET time_zone = '${mvTz}'"
    def mvRes = sql "SELECT id, CAST(d AS STRING) FROM ${mvName} ORDER BY 1"
    Assert.assertEquals(1, mvRes.size())
    Assert.assertTrue("expected 2024-01-01 00:00:00.000000+00:00, got " + mvRes[0][1],
            mvRes[0][1].toString().contains("2024-01-01 00:00:00.000000+00:00"))

    // Same-zone (UTC) query rewrites to the MV: query and MV agree on the day boundary, so the rewrite
    // must succeed and return the direct-query rows.
    sql "SET time_zone = '${mvTz}'"
    sql "SET enable_materialized_view_rewrite=false"
    def sameZoneOff = sql "SELECT id, CAST(d AS STRING) FROM ${viewName} ORDER BY 1"
    sql "SET enable_materialized_view_rewrite=true"
    mv_rewrite_success("SELECT id, d FROM ${viewName}", mvName)
    def sameZoneOn = sql "SELECT id, CAST(d AS STRING) FROM ${viewName} ORDER BY 1"
    Assert.assertEquals("same-zone rewrite must return the direct-query rows", sameZoneOff, sameZoneOn)

    // Query in the third zone (-05:00): the direct evaluation truncates the instant to the -05:00 day
    // (2023-12-31), so the UTC-materialized day (2024-01-01) must NOT be substituted by the rewrite. The
    // guarded cache must keep a cache-mismatch marker around the view guard so the rewrite is rejected.
    sql "SET time_zone = '${queryTz}'"
    sql "SET enable_materialized_view_rewrite=false"
    def crossZoneOff = sql "SELECT id, CAST(d AS STRING) FROM ${viewName} ORDER BY 1"
    sql "SET enable_materialized_view_rewrite=true"
    mv_rewrite_fail("SELECT id, d FROM ${viewName}", mvName)
    def crossZoneOn = sql "SELECT id, CAST(d AS STRING) FROM ${viewName} ORDER BY 1"
    Assert.assertEquals("three-zone query must not read the UTC-materialized MV day",
            crossZoneOff, crossZoneOn)
    Assert.assertEquals(1, crossZoneOn.size())
    Assert.assertTrue("expected 2023-12-31 00:00:00.000000-05:00, got " + crossZoneOn[0][1],
            crossZoneOn[0][1].toString().contains("2023-12-31"))
}
