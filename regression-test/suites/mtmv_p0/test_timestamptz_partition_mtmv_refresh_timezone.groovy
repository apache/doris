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
 * When an asynchronous partitioned materialized view is created in a session with a non-default time zone
 * and its partition key is a time-zone sensitive expression (date_trunc on a TIMESTAMPTZ column), the
 * background refresh must run with a consistent zone so that the computed partition key falls into the
 * UTC-aligned MV partition boundaries. Otherwise the refresh computes a partition key that does not fall
 * into any MV partition ("no partition for this tuple") and the MV stays empty.
 *
 * The creation session zone is chosen to be provably different from the FE default @@time_zone, so the
 * test also fails (pre-fix) when the FE default happens to equal the creation zone.
 */
suite("test_timestamptz_partition_mtmv_refresh_timezone","mtmv") {
    def dbName = "timestamptz_partition_mtmv_refresh_timezone"
    def tableName = "timestamptz_partition_mtmv_refresh_timezone_table"
    def mvName = "timestamptz_partition_mtmv_refresh_timezone_mv"

    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE ${dbName}"
    sql "USE ${dbName}"

    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"

    // Pick a creation zone that is provably different from the FE default @@time_zone.
    def defaultTz = sql("SELECT @@time_zone")[0][0].toString()
    def utcZones = ['+00:00', 'UTC', 'Etc/UTC', 'GMT', 'Z']
    def creationTz = utcZones.contains(defaultTz) ? '+08:00' : '+00:00'
    logger.info("default @@time_zone = ${defaultTz}, use creation zone ${creationTz}")
    sql "SET time_zone = '${creationTz}'"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            ts TIMESTAMPTZ(6),
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

    sql """
        INSERT INTO ${tableName} VALUES
        (1, '2024-01-01 00:30:00+00:00', 10)
    """
    sql "sync"

    sql "DROP MATERIALIZED VIEW IF EXISTS ${mvName}"
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        PARTITION BY(day_ts)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES('replication_num' = '1')
        AS
        SELECT date_trunc(ts, 'day') AS day_ts, SUM(v) AS total
        FROM ${tableName}
        GROUP BY date_trunc(ts, 'day')
    """
    waitingMTMVTaskFinishedByMvName(mvName, dbName)

    // The refresh task must be SUCCESS, not FAILED.
    def tasks = sql """
        SELECT Status, MvName, ErrorMsg
        FROM tasks('type' = 'mv')
        WHERE MvDatabaseName = '${dbName}' AND MvName = '${mvName}'
        ORDER BY CreateTime DESC
        LIMIT 1
    """
    Assert.assertEquals("SUCCESS", tasks[0][0].toString())

    // The MTMV must be NORMAL / SUCCESS and contain the row that falls into the UTC-aligned day partition.
    def mvInfos = sql """
        SELECT Name, State, RefreshState
        FROM mv_infos('database' = '${dbName}')
        WHERE Name = '${mvName}'
    """
    Assert.assertEquals("NORMAL", mvInfos[0][1].toString())
    Assert.assertEquals("SUCCESS", mvInfos[0][2].toString())

    // The effective session zone of a date_trunc-on-TIMESTAMPTZ partition is UTC (the partition key is a
    // UTC-aligned day boundary), so a UTC query rewrites to the MV while a +08:00 query must not.
    sql "SET time_zone = '+00:00'"
    mv_rewrite_success("""
        SELECT date_trunc(ts, 'day'), SUM(v)
        FROM ${tableName}
        GROUP BY date_trunc(ts, 'day')
    """, mvName)
    sql "SET time_zone = '+08:00'"
    mv_rewrite_fail("""
        SELECT date_trunc(ts, 'day'), SUM(v)
        FROM ${tableName}
        GROUP BY date_trunc(ts, 'day')
    """, mvName)

    // Read the MV in UTC so the asserted string is independent of the creation zone.
    sql "SET time_zone = '+00:00'"
    def mvRes = sql "SELECT CAST(day_ts AS STRING), total FROM ${mvName} ORDER BY 1"
    Assert.assertEquals(1, mvRes.size())
    Assert.assertTrue("expected 2024-01-01 00:00:00.000000+00:00, got " + mvRes[0][0],
            mvRes[0][0].toString().contains("2024-01-01 00:00:00.000000+00:00"))
    Assert.assertEquals(10, mvRes[0][1])
}
