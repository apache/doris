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
 * A guarded ordering expression in a TopN (ORDER BY ... LIMIT) decides WHICH rows survive, exactly like a
 * filter or a group-by: the MV ordered and truncated its rows under its creation session, so a query in
 * another time zone cannot compensate that by re-evaluating the guard. The row-affecting guard scan of the
 * rewrite must therefore include LogicalTopN order keys, not only plain sorts.
 *
 * The MV projects the ordering expression but the query selects only the zone-invariant id, so the
 * projection-side guard check alone cannot reject the rewrite here.
 */
suite("test_timestamptz_mtmv_topn_cross_zone_rewrite","mtmv") {
    def dbName = "timestamptz_mtmv_topn_cross_zone_rewrite"
    def tableName = "timestamptz_mtmv_topn_cross_zone_rewrite_table"
    def viewName = "timestamptz_mtmv_topn_cross_zone_rewrite_view"
    def mvName = "timestamptz_mtmv_topn_cross_zone_rewrite_mv"

    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE ${dbName}"
    sql "USE ${dbName}"

    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"

    def mvTz = '+08:00'      // view and MTMV creation zone
    def queryTz = '-05:00'   // query zone

    sql "SET time_zone = '${mvTz}'"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            ts TIMESTAMPTZ(6)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num' = '1')
    """
    // 2023-12-31 20:00Z is 2024-01-01 in +08:00 but still 2023-12-31 in -05:00; 2023-12-31 10:00Z is
    // 2023-12-31 in both zones. The +08:00 TopN therefore orders id=2 (2023-12-31) before id=1
    // (2024-01-01), while in -05:00 both instants fall on the same day and the id tie-break picks id=1.
    sql """
        INSERT INTO ${tableName} VALUES
        (1, '2023-12-31 20:00:00+00:00'),
        (2, '2023-12-31 10:00:00+00:00')
    """
    sql "sync"

    sql "DROP VIEW IF EXISTS ${viewName}"
    sql "CREATE VIEW ${viewName} AS SELECT id, date_trunc(ts, 'day') AS d FROM ${tableName}"

    sql "DROP MATERIALIZED VIEW IF EXISTS ${mvName}"
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES('replication_num' = '1')
        AS
        SELECT id, d FROM ${viewName}
        ORDER BY d, id
        LIMIT 1
    """
    waitingMTMVTaskFinishedByMvName(mvName, dbName)

    // The MV materialized the +08:00 day boundary: it kept id=2.
    sql "SET time_zone = '${mvTz}'"
    def mvRes = sql "SELECT id FROM ${mvName} ORDER BY 1"
    Assert.assertEquals(1, mvRes.size())
    Assert.assertEquals(2, mvRes[0][0])

    // The query selects only the zone-invariant id, so the projection guard check passes and the rewrite
    // decision is made by the TopN order keys alone.
    def topNQuery = "SELECT id FROM ${viewName} ORDER BY d, id LIMIT 1"

    // Same zone: query and MV agree, so the TopN rewrite is used and returns the MV's row.
    sql "SET time_zone = '${mvTz}'"
    mv_rewrite_success(topNQuery, mvName)
    def sameZoneRes = sql(topNQuery)
    Assert.assertEquals(1, sameZoneRes.size())
    Assert.assertEquals(2, sameZoneRes[0][0])

    // Cross zone: in -05:00 both instants fall on 2023-12-31, so the direct query selects id=1 and the
    // +08:00-selected MV row (id=2) must not be substituted.
    sql "SET time_zone = '${queryTz}'"
    sql "SET enable_materialized_view_rewrite=false"
    def crossZoneOff = sql(topNQuery)
    sql "SET enable_materialized_view_rewrite=true"
    mv_rewrite_fail(topNQuery, mvName)
    def crossZoneOn = sql(topNQuery)
    Assert.assertEquals("cross-zone TopN must return the direct-query rows", crossZoneOff, crossZoneOn)
    Assert.assertEquals(1, crossZoneOn.size())
    Assert.assertEquals("cross-zone TopN must not read the +08:00-selected MV row", 1, crossZoneOn[0][0])
}
