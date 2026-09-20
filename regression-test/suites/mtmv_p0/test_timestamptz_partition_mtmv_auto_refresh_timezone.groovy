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
 * The effective creation zone of an MTMV partitioned by an aliased date_trunc(TIMESTAMPTZ) expression must
 * be persisted as UTC, because MTMVPartitionExprDateTrunc derives UTC-aligned partition boundaries. For
 * REFRESH AUTO the IVM probe rejects the EXPR partition (IVM only supports column partitions) and the
 * create then succeeds through the regular, non-IVM path: the UTC normalization must still be applied for
 * that arm. Otherwise the non-UTC DDL session zone stays persisted, the refresh computes the partition key
 * in that zone (e.g. a 2024-01-01 00:30Z row truncates to 2024-01-01 00:00+08 = 2023-12-31 16:00Z), and the
 * refreshed row falls into no UTC-aligned MV partition (or into the preceding UTC one).
 *
 * The creation session zone is chosen to be provably different from the FE default @@time_zone, so the
 * test also fails (pre-fix) when the FE default happens to equal the creation zone.
 */
suite("test_timestamptz_partition_mtmv_auto_refresh_timezone","mtmv") {
    def dbName = "timestamptz_partition_mtmv_auto_refresh_timezone"
    def tableName = "timestamptz_partition_mtmv_auto_refresh_timezone_table"
    def mvName = "timestamptz_partition_mtmv_auto_refresh_timezone_mv"

    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE ${dbName}"
    sql "USE ${dbName}"

    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"

    // The creation session MUST be a non-UTC zone with a POSITIVE offset: a positive offset renders the
    // UTC-aligned day boundary (2024-01-01 00:00Z) as an earlier instant (2023-12-31 16:00Z for +08:00),
    // so a creation-zone evaluation of the partition key finds no matching (UTC-aligned) MV partition.
    def defaultTz = sql("SELECT @@time_zone")[0][0].toString()
    def plusEightZones = ['+08:00', 'Asia/Shanghai', 'PRC', 'Hongkong', 'Asia/Hong_Kong', 'Singapore',
            'Asia/Singapore', 'Asia/Chongqing', 'Asia/Harbin', 'Asia/Macau', 'Asia/Taipei', 'ROC']
    def creationTz = plusEightZones.contains(defaultTz) ? '+09:00' : '+08:00'
    def utcTz = '+00:00'
    logger.info("default @@time_zone = ${defaultTz}, use creation zone ${creationTz}, UTC query zone ${utcTz}")
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

    // A row that only falls into the UTC-aligned day partition when the partition key is evaluated in UTC:
    // 2024-01-01 00:30Z is the day boundary 2024-01-01 00:00+00 (the MV partition), while in +08:00 it
    // renders as 2024-01-01 08:30+08 so a creation-zone evaluation would truncate it to 2023-12-31 16:00Z.
    sql """
        INSERT INTO ${tableName} VALUES
        (1, '2024-01-01 00:30:00+00:00', 10)
    """
    sql "sync"

    // REFRESH AUTO probes IVM, which rejects the EXPR partition, and then creates a regular MTMV.
    sql "DROP MATERIALIZED VIEW IF EXISTS ${mvName}"
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL
        PARTITION BY(day_ts)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES('replication_num' = '1')
        AS
        SELECT date_trunc(ts, 'day') AS day_ts, SUM(v) AS total
        FROM ${tableName}
        GROUP BY date_trunc(ts, 'day')
    """
    waitingMTMVTaskFinishedByMvName(mvName, dbName)

    // The refresh task must be SUCCESS, not FAILED with "no partition for this tuple".
    def tasks = sql """
        SELECT Status, MvName, ErrorMsg
        FROM tasks('type' = 'mv')
        WHERE MvDatabaseName = '${dbName}' AND MvName = '${mvName}'
        ORDER BY CreateTime DESC
        LIMIT 1
    """
    Assert.assertEquals("SUCCESS", tasks[0][0].toString());

    def mvInfos = sql """
        SELECT Name, State, RefreshState
        FROM mv_infos('database' = '${dbName}')
        WHERE Name = '${mvName}'
    """
    Assert.assertEquals("NORMAL", mvInfos[0][1].toString())
    Assert.assertEquals("SUCCESS", mvInfos[0][2].toString())

    // The effective session zone of a date_trunc-on-TIMESTAMPTZ partition is UTC (the partition key is a
    // UTC-aligned day boundary), so a UTC query rewrites to the MV while a creation-zone query must not.
    sql "SET time_zone = '${utcTz}'"
    mv_rewrite_success("""
        SELECT date_trunc(ts, 'day'), SUM(v)
        FROM ${tableName}
        GROUP BY date_trunc(ts, 'day')
    """, mvName)
    sql "SET time_zone = '${creationTz}'"
    mv_rewrite_fail("""
        SELECT date_trunc(ts, 'day'), SUM(v)
        FROM ${tableName}
        GROUP BY date_trunc(ts, 'day')
    """, mvName)

    // Read the MV in UTC: the stored partition boundary must be the UTC-aligned instant 2024-01-01 00:00Z
    // (= 2024-01-01 08:00+08), not the creation-zone boundary 2024-01-01 00:00+08, and it must equal what
    // the direct query computes in the same session.
    sql "SET time_zone = '${utcTz}'"
    def mvRes = sql "SELECT CAST(day_ts AS STRING), total FROM ${mvName} ORDER BY 1"
    Assert.assertEquals(1, mvRes.size())
    Assert.assertTrue("expected 2024-01-01 00:00:00.000000+00:00, got " + mvRes[0][0],
            mvRes[0][0].toString().contains("2024-01-01 00:00:00.000000+00:00"))
    Assert.assertEquals(10, mvRes[0][1])
    def directRes = sql """
        SELECT CAST(date_trunc(ts, 'day') AS STRING), SUM(v)
        FROM ${tableName}
        GROUP BY date_trunc(ts, 'day')
    """
    Assert.assertEquals("the materialized day boundary must equal the direct UTC computation",
            directRes, mvRes)
}
