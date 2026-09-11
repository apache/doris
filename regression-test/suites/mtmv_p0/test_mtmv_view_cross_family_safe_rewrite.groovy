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
 * The cache-mismatch marker around an existing non-cache guard is scoped to the guard family the cache
 * is built for (computeGuardMask separates the time-zone family from the other affectQueryResult-variable
 * family). A decimal multiplication depends on the "other" family (enable_decimal256), so a view created
 * with enable_decimal256=true wraps it in an "other"-family view guard when expanded in a session with
 * enable_decimal256=false. When a query differs from the MTMV only in time zone (time-zone cache mask) and
 * the wrapped expression has no time-zone dependency, the rewrite is SAFE: query and MV agree on the view's
 * persisted decimal semantics through the view guard. The cache builder must not add a time-zone cache
 * marker around that "other"-family view guard, otherwise the isCacheGuard() gates reject the safe rewrite.
 */
suite("test_mtmv_view_cross_family_safe_rewrite","mtmv") {
    def dbName = "mtmv_view_cross_family_safe_rewrite"
    def tableName = "mtmv_view_cross_family_safe_rewrite_table"
    def viewName = "mtmv_view_cross_family_safe_rewrite_view"
    def mvName = "mtmv_view_cross_family_safe_rewrite_mv"

    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE ${dbName}"
    sql "USE ${dbName}"

    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"

    def viewTz = '+00:00'    // view creation zone
    def mvTz = '+00:00'      // MTMV creation/refresh zone
    def queryTz = '+08:00'   // query zone (differs from the MTMV only in time zone)

    // Base table with decimal columns.
    sql "SET time_zone = '${viewTz}'"
    sql "SET enable_decimal256 = true"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            a DECIMAL(10, 2),
            b DECIMAL(10, 2),
            v INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num' = '1')
    """
    sql "INSERT INTO ${tableName} VALUES (1, 10.50, 2.00, 10)"
    sql "sync"

    // View created with enable_decimal256=true: the decimal multiply depends on the "other" session
    // variable family, so expanding it in a session with enable_decimal256=false wraps it in an
    // "other"-family view guard.
    sql "SET time_zone = '${viewTz}'"
    sql "SET enable_decimal256 = true"
    sql "DROP VIEW IF EXISTS ${viewName}"
    sql "CREATE VIEW ${viewName} AS SELECT id, a * b AS x, v FROM ${tableName}"

    // MTMV over the view created/refreshed with enable_decimal256=false in UTC: its definition plan carries
    // the same "other"-family view guard.
    sql "SET time_zone = '${mvTz}'"
    sql "SET enable_decimal256 = false"
    sql "DROP MATERIALIZED VIEW IF EXISTS ${mvName}"
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES('replication_num' = '1')
        AS
        SELECT id, x, v
        FROM ${viewName}
    """
    waitingMTMVTaskFinishedByMvName(mvName, dbName)

    // Query in +08:00 with enable_decimal256=false: it differs from the MTMV only in time zone (time-zone
    // cache mask) and the decimal multiply has no time-zone dependency, so the rewrite is SAFE and must
    // succeed - the cache-mismatch marker must stay scoped to the mask family and must NOT wrap the
    // existing "other"-family view guard.
    sql "SET time_zone = '${queryTz}'"
    sql "SET enable_decimal256 = false"
    sql "SET enable_materialized_view_rewrite=false"
    def off = sql "SELECT id, CAST(x AS STRING), v FROM ${viewName} ORDER BY 1"
    sql "SET enable_materialized_view_rewrite=true"
    mv_rewrite_success("SELECT id, x, v FROM ${viewName}", mvName)
    def on = sql "SELECT id, CAST(x AS STRING), v FROM ${viewName} ORDER BY 1"
    Assert.assertEquals("cross-family cross-zone rewrite must return the direct-query rows", off, on)
    Assert.assertEquals(1, on.size())
    Assert.assertEquals("21.0000", on[0][1].toString())
}
