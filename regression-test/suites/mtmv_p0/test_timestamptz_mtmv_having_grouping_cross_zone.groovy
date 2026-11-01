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
 * A materialized view whose definition carries a time-zone dependent expression only inside HAVING or only
 * inside its grouping-set keys materializes its row membership / grouping under its creation-session zone.
 * Reading it from a query in another zone must therefore never use it: a cache guard in those positions has
 * to be treated as row affecting even when the guarded expression is not projected.
 */
suite("test_timestamptz_mtmv_having_grouping_cross_zone","mtmv") {
    def dbName = "timestamptz_mtmv_having_grouping_cross_zone"
    def tableName = "timestamptz_mtmv_having_grouping_cross_zone_table"
    def havingMvName = "timestamptz_mtmv_having_grouping_cross_zone_having_mv"
    def groupingMvName = "timestamptz_mtmv_having_grouping_cross_zone_grouping_mv"

    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE ${dbName}"
    sql "USE ${dbName}"

    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"

    def creationTz = '+08:00'
    def queryTz = '+00:00'

    sql "SET time_zone = '${creationTz}'"
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
    // In +08:00 both instants are day 2024-01-01 / 2024-01-01 while in +00:00 the second one is
    // 2023-12-31, so grouping and HAVING membership differ between the two zones.
    sql "INSERT INTO ${tableName} VALUES (1, '2024-01-01 02:30:00+00:00', 10)"
    sql "INSERT INTO ${tableName} VALUES (2, '2023-12-31 20:00:00+00:00', 20)"
    sql "sync"

    // The guarded expression only exists in HAVING: neither the projection nor the group-by exposes it.
    sql "DROP MATERIALIZED VIEW IF EXISTS ${havingMvName}"
    sql """
        CREATE MATERIALIZED VIEW ${havingMvName}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES('replication_num' = '1')
        AS
        SELECT id, SUM(v) AS s
        FROM ${tableName}
        GROUP BY id
        HAVING date_trunc(MAX(ts), 'day') > '2023-12-31 00:00:00'
    """
    waitingMTMVTaskFinishedByMvName(havingMvName, dbName)

    // The guarded expression only exists in the grouping-set keys, and the only output is the aggregate.
    sql "DROP MATERIALIZED VIEW IF EXISTS ${groupingMvName}"
    sql """
        CREATE MATERIALIZED VIEW ${groupingMvName}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES('replication_num' = '1')
        AS
        SELECT SUM(v) AS s
        FROM ${tableName}
        GROUP BY GROUPING SETS ((date_trunc(ts, 'day')), ())
    """
    waitingMTMVTaskFinishedByMvName(groupingMvName, dbName)

    // Cross-zone reads must return exactly what the direct query returns, i.e. the creation-session rows and
    // groups must not be substituted.
    sql "SET time_zone = '${queryTz}'"
    def havingQuery = "SELECT id, SUM(v) AS s FROM ${tableName} GROUP BY id" +
            " HAVING date_trunc(MAX(ts), 'day') > '2023-12-31 00:00:00'"
    sql "SET enable_materialized_view_rewrite = false"
    def havingOff = sql "${havingQuery} ORDER BY 1"
    sql "SET enable_materialized_view_rewrite = true"
    mv_rewrite_fail(havingQuery, havingMvName)
    def havingOn = sql "${havingQuery} ORDER BY 1"
    Assert.assertEquals("a HAVING guard must not be substituted across zones", havingOff, havingOn)
    // In +00:00 the second instant belongs to 2023-12-31, so its group is filtered out.
    Assert.assertEquals(1, havingOn.size())
    Assert.assertEquals(1, havingOn[0][0])

    def groupingQuery = "SELECT SUM(v) AS s FROM ${tableName} GROUP BY GROUPING SETS" +
            " ((date_trunc(ts, 'day')), ())"
    sql "SET enable_materialized_view_rewrite = false"
    def groupingOff = sql "${groupingQuery} ORDER BY 1"
    sql "SET enable_materialized_view_rewrite = true"
    mv_rewrite_fail(groupingQuery, groupingMvName)
    def groupingOn = sql "${groupingQuery} ORDER BY 1"
    Assert.assertEquals("a grouping-set guard must not be substituted across zones", groupingOff, groupingOn)
    // Two day groups plus the grand total in +00:00.
    Assert.assertEquals(3, groupingOn.size())

    // The same queries in the creation zone are the control: the MV is eligible there, so the assertions
    // above really compare "MV not usable" rather than "MV never matched anything".
    sql "SET time_zone = '${creationTz}'"
    sql "SET enable_materialized_view_rewrite = false"
    def havingSameOff = sql "${havingQuery} ORDER BY 1"
    sql "SET enable_materialized_view_rewrite = true"
    def havingSameOn = sql "${havingQuery} ORDER BY 1"
    Assert.assertEquals("same-zone reads must agree as well", havingSameOff, havingSameOn)
}
