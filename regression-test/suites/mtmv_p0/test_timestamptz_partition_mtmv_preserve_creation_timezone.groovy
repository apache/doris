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
 * An MTMV that partitions by the RAW TIMESTAMPTZ slot (no time-zone converting partition expression such as
 * date_trunc) must preserve the definition's captured time zone: the background refresh and the rewrite
 * cache are built in the creation zone, so zone-sensitive content expressions (e.g. a string rendering of
 * date_trunc(ts, 'day')) are materialized per the creation session and NOT silently re-rendered in UTC.
 * The persisted zone must be compared consistently: a query in the same zone rewrites, a query in a
 * different zone does not.
 */
suite("test_timestamptz_partition_mtmv_preserve_creation_timezone","mtmv") {
    def dbName = "timestamptz_partition_mtmv_preserve_creation_timezone"
    def tableName = "timestamptz_partition_mtmv_preserve_creation_timezone_table"
    def mvName = "timestamptz_partition_mtmv_preserve_creation_timezone_mv"

    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE ${dbName}"
    sql "USE ${dbName}"

    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"

    // Pick a creation zone that is provably different from the FE default @@time_zone.
    def defaultTz = sql("SELECT @@time_zone")[0][0].toString()
    def utcZones = ['+00:00', 'UTC', 'Etc/UTC', 'GMT', 'Z']
    def creationTz = utcZones.contains(defaultTz) ? '+08:00' : '+00:00'
    def crossTz = utcZones.contains(creationTz) ? '+08:00' : '+00:00'
    logger.info("default @@time_zone = ${defaultTz}, creation zone = ${creationTz}")
    sql "SET time_zone = '${creationTz}'"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            ts TIMESTAMPTZ(6) NOT NULL,
            v INT
        )
        DUPLICATE KEY(id)
        PARTITION BY RANGE(ts) (
            PARTITION p0 VALUES [('2024-01-01 00:00:00+00:00'), ('2024-01-02 00:00:00+00:00')),
            PARTITION p1 VALUES [('2024-01-02 00:00:00+00:00'), ('2024-01-03 00:00:00+00:00'))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num' = '1')
    """

    // The row's UTC instant 2024-01-01 20:30Z falls into base partition p0; in the creation zone it is the
    // NEXT calendar day (e.g. 2024-01-02 04:30 +08:00), so a zone-correct content rendering must use the
    // creation day boundary, not the UTC day boundary.
    sql """
        INSERT INTO ${tableName} VALUES
        (1, '2024-01-01 20:30:00+00:00', 10)
    """
    sql "sync"

    // Partition by the RAW TIMESTAMPTZ slot; content includes a zone-sensitive string rendering.
    sql "DROP MATERIALIZED VIEW IF EXISTS ${mvName}"
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        PARTITION BY(ts)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES('replication_num' = '1')
        AS
        SELECT ts, CAST(date_trunc(ts, 'day') AS STRING) AS day_str, v
        FROM ${tableName}
    """
    waitingMTMVTaskFinishedByMvName(mvName, dbName)

    // The refresh must be SUCCESS and the MV must carry the row.
    def tasks = sql """
        SELECT Status, MvName, ErrorMsg
        FROM tasks('type' = 'mv')
        WHERE MvDatabaseName = '${dbName}' AND MvName = '${mvName}'
        ORDER BY CreateTime DESC
        LIMIT 1
    """
    Assert.assertEquals("SUCCESS", tasks[0][0].toString())

    // The direct content must be rendered in the CREATION zone: for 2024-01-01 20:30Z the day boundary is
    // the creation-zone day (2024-01-02 00:00:00.000000+08:00 in +08:00, 2024-01-01 00:00:00.000000+00:00
    // in +00:00), NOT the UTC day boundary.
    sql "SET time_zone = '${creationTz}'"
    def mvRes = sql "SELECT CAST(ts AS STRING), day_str, v FROM ${mvName} ORDER BY 1"
    Assert.assertEquals(1, mvRes.size())
    Assert.assertEquals(10, mvRes[0][2])
    if (creationTz == '+08:00') {
        Assert.assertTrue("expected 2024-01-02 00:00:00.000000+08:00, got " + mvRes[0][1],
                mvRes[0][1].toString().contains("2024-01-02 00:00:00.000000+08:00"))
    } else {
        Assert.assertTrue("expected 2024-01-01 00:00:00.000000+00:00, got " + mvRes[0][1],
                mvRes[0][1].toString().contains("2024-01-01 00:00:00.000000+00:00"))
    }

    // Same-zone query rewrites to the MV.
    sql "SET time_zone = '${creationTz}'"
    mv_rewrite_success("SELECT ts, CAST(date_trunc(ts, 'day') AS STRING), v FROM ${tableName}", mvName)

    // Cross-zone query must never surface the stale creation-zone materialized content: whether or not the
    // optimizer uses the MV, the result must equal the direct base-table computation in the query zone.
    sql "SET time_zone = '${crossTz}'"
    sql "SET enable_materialized_view_rewrite=false"
    def crossRewriteOff = sql "SELECT CAST(ts AS STRING), CAST(date_trunc(ts, 'day') AS STRING), v FROM ${tableName}"
    sql "SET enable_materialized_view_rewrite=true"
    def crossRewriteOn = sql "SELECT CAST(ts AS STRING), CAST(date_trunc(ts, 'day') AS STRING), v FROM ${tableName}"
    Assert.assertEquals("cross-zone rewrite must return the query-zone result", crossRewriteOff, crossRewriteOn)
    Assert.assertEquals(1, crossRewriteOn.size())
    if (crossTz == '+08:00') {
        Assert.assertTrue("expected 2024-01-02 00:00:00.000000+08:00, got " + crossRewriteOn[0][1],
                crossRewriteOn[0][1].toString().contains("2024-01-02 00:00:00.000000+08:00"))
    } else {
        Assert.assertTrue("expected 2024-01-01 00:00:00.000000+00:00, got " + crossRewriteOn[0][1],
                crossRewriteOn[0][1].toString().contains("2024-01-01 00:00:00.000000+00:00"))
    }
}
