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
 * Time-zone sensitive TIMESTAMPTZ expressions must never be rewritten across sessions with a different
 * time zone, otherwise the query returns stale materialized values computed in the MV creation zone.
 *
 * 1. A synchronous MV column that converts a TIMESTAMPTZ into a time-zone dependent value (date_trunc,
 *    cast, floor, ...) is allowed at creation: keeping the materialized value consistent across sessions
 *    is the user's responsibility, not enforced by Doris.
 * 2. Zone-invariant operations on TIMESTAMPTZ (MIN/MAX aggregates preserve the UTC instant) are allowed
 *    and still rewrite across zones.
 * 3. An asynchronous MTMV with a time-zone sensitive expression is allowed (it is refreshed in a
 *    consistent zone) but is NOT rewritten in a session with a different time zone; in the same zone it
 *    rewrites and returns the same result as the base query.
 */
suite("test_timestamptz_sync_mv_rewrite_timezone","mtmv") {
    def tableName = "timestamptz_sync_mv_rewrite_timezone_table"
    def syncMvName = "timestamptz_sync_mv_rewrite_timezone_sync"
    def asyncMvName = "timestamptz_sync_mv_rewrite_timezone_async"

    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"

    // A sync MV is an index on the base table: dropping the table drops the MV too. We must NOT run
    // `DROP MATERIALIZED VIEW ... ON <table>` here because that statement requires the table to exist,
    // which it may not in a fresh test database.
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "DROP MATERIALIZED VIEW IF EXISTS ${asyncMvName}"

    // Build the base table and the MVs in a UTC session.
    sql "SET time_zone = '+00:00'"
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            ts TIMESTAMPTZ(6),
            v INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num' = '1')
    """
    sql """
        INSERT INTO ${tableName} VALUES
        (1, '2024-01-01 00:30:00+00:00', 10)
    """
    sql "sync"

    // A synchronous MV that converts a TIMESTAMPTZ into a time-zone dependent value (date_trunc here) is
    // no longer rejected at creation: the materialized value is computed in the write/load session zone
    // and keeping it consistent across sessions is the user's responsibility. Each such MV is built on its
    // own DUPLICATE table (a DUPLICATE table cannot host several sync MVs whose columns collide).
    def tableNameTruncCol = "timestamptz_sync_mv_rewrite_timezone_trunc_col"
    def truncColMvName = "timestamptz_sync_mv_rewrite_timezone_trunc_col_mv"
    sql "DROP TABLE IF EXISTS ${tableNameTruncCol}"
    sql """
        CREATE TABLE ${tableNameTruncCol} (
            id INT,
            ts TIMESTAMPTZ(6),
            v INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num' = '1')
    """
    create_sync_mv(context.dbName, tableNameTruncCol, truncColMvName, """
        SELECT date_trunc(ts, 'day') AS day_ts, SUM(v) AS s
        FROM ${tableNameTruncCol}
        WHERE ts IS NOT NULL
        GROUP BY date_trunc(ts, 'day')
    """)

    // A synchronous MV whose SELECT columns are safe but whose WHERE compares a time-zone dependent
    // expression is likewise no longer rejected at creation: the WHERE is stored separately
    // (whereClauseItem) and its zone dependence is the user's responsibility.
    def tableNameTruncWhere = "timestamptz_sync_mv_rewrite_timezone_trunc_where"
    def truncWhereMvName = "timestamptz_sync_mv_rewrite_timezone_trunc_where_mv"
    sql "DROP TABLE IF EXISTS ${tableNameTruncWhere}"
    sql """
        CREATE TABLE ${tableNameTruncWhere} (
            id INT,
            ts TIMESTAMPTZ(6),
            v INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num' = '1')
    """
    create_sync_mv(context.dbName, tableNameTruncWhere, truncWhereMvName, """
        SELECT id AS mv_id, MIN(ts) AS mv_min
        FROM ${tableNameTruncWhere}
        WHERE date_trunc(ts, 'day') = '2024-01-01 00:00:00+00:00'
        GROUP BY id
    """)

    // A zone-invariant sync MV (MIN over a TIMESTAMPTZ preserves the UTC instant) is allowed.
    create_sync_mv(context.dbName, tableName, syncMvName, """
        SELECT id AS mv_id, MIN(ts) AS mv_min
        FROM ${tableName}
        GROUP BY id
    """)

    // Query in a different (+08:00) session: the zone-invariant MV rewrites and the results agree.
    sql "SET time_zone = '+08:00'"
    sql "SET enable_materialized_view_rewrite=false"
    def resRewriteOff = sql """
        SELECT id, MIN(ts) FROM ${tableName} GROUP BY id ORDER BY 1
    """
    sql "SET enable_materialized_view_rewrite=true"
    def resRewriteOn = sql """
        SELECT id, MIN(ts) FROM ${tableName} GROUP BY id ORDER BY 1
    """
    Assert.assertEquals(resRewriteOff, resRewriteOn)
    Assert.assertEquals(1, resRewriteOn.size())
    // the stored instant 2024-01-01 00:30:00 UTC renders as 08:30 in the +08:00 query session
    Assert.assertTrue("expected 2024-01-01 08:30:00.000000+08:00, got " + resRewriteOn[0][1],
            resRewriteOn[0][1].toString().contains("2024-01-01 08:30:00.000000+08:00"))
    // the zone-invariant MV is actually selected in the cross-zone session
    mv_rewrite_success("SELECT id, MIN(ts) FROM ${tableName} GROUP BY id", syncMvName)

    // An asynchronous MTMV with a time-zone sensitive expression is allowed but must NOT be rewritten
    // in a session with a different time zone (the UTC-materialized day boundary differs).
    sql "SET time_zone = '+00:00'"
    create_async_mv(context.dbName, asyncMvName, """
        SELECT date_trunc(ts, 'day') AS day_ts, SUM(v) AS s
        FROM ${tableName}
        WHERE ts IS NOT NULL
        GROUP BY date_trunc(ts, 'day')
    """)
    sql "SET time_zone = '+08:00'"
    mv_rewrite_fail("""
        SELECT date_trunc(ts, 'day'), SUM(v)
        FROM ${tableName}
        WHERE ts IS NOT NULL
        GROUP BY date_trunc(ts, 'day')
    """, asyncMvName)

    // In the SAME (+00:00) session the async MTMV rewrites and returns the same result as the base query.
    sql "SET time_zone = '+00:00'"
    def baseSameTz = sql """
        SELECT CAST(date_trunc(ts, 'day') AS STRING), SUM(v)
        FROM ${tableName}
        WHERE ts IS NOT NULL
        GROUP BY date_trunc(ts, 'day')
    """
    mv_rewrite_success("""
        SELECT date_trunc(ts, 'day'), SUM(v)
        FROM ${tableName}
        WHERE ts IS NOT NULL
        GROUP BY date_trunc(ts, 'day')
    """, asyncMvName)
    def mvSameTz = sql """
        SELECT CAST(date_trunc(ts, 'day') AS STRING), SUM(v)
        FROM ${tableName}
        WHERE ts IS NOT NULL
        GROUP BY date_trunc(ts, 'day')
    """
    Assert.assertEquals(baseSameTz, mvSameTz)
    Assert.assertEquals(1, mvSameTz.size())
    Assert.assertTrue("expected 2024-01-01 00:00:00.000000+00:00, got " + mvSameTz[0][0],
            mvSameTz[0][0].toString().contains("2024-01-01 00:00:00.000000+00:00"))

    // A synchronous MV built on a DUPLICATE table: a WHERE that compares two TIMESTAMPTZ instants is
    // zone-invariant and is created successfully.
    def tableName2 = "timestamptz_sync_mv_rewrite_timezone_table2"
    def tableName3 = "timestamptz_sync_mv_rewrite_timezone_table3"
    def cmpSyncMvName = "timestamptz_sync_mv_rewrite_timezone_cmp_sync"
    sql "DROP TABLE IF EXISTS ${tableName2}"
    sql """
        CREATE TABLE ${tableName2} (
            id INT,
            ts1 TIMESTAMPTZ(6),
            ts2 TIMESTAMPTZ(6),
            v INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num' = '1')
    """
    // positive: comparing two instants is zone-invariant and the MV is created successfully
    create_sync_mv(context.dbName, tableName2, cmpSyncMvName, """
        SELECT id AS mv_id, MIN(ts1) AS mv_min
        FROM ${tableName2}
        WHERE ts1 = ts2
        GROUP BY id
    """)
    // the materialized value is correct and identical whether or not the rewrite uses the MV
    sql "INSERT INTO ${tableName2} VALUES (1, '2024-01-01 00:30:00+00:00', '2024-01-01 00:30:00+00:00', 10)"
    sql "sync"
    sql "SET time_zone = '+08:00'"
    sql "SET enable_materialized_view_rewrite=false"
    def cmpOff = sql "SELECT id, MIN(ts1) FROM ${tableName2} WHERE ts1 = ts2 GROUP BY id ORDER BY 1"
    sql "SET enable_materialized_view_rewrite=true"
    def cmpOn = sql "SELECT id, MIN(ts1) FROM ${tableName2} WHERE ts1 = ts2 GROUP BY id ORDER BY 1"
    Assert.assertEquals(cmpOff, cmpOn)
    Assert.assertEquals(1, cmpOn.size())

    // casting an offset-free string into TIMESTAMPTZ interprets the string in the write session zone; it
    // is no longer rejected at creation either
    sql "DROP TABLE IF EXISTS ${tableName3}"
    sql """
        CREATE TABLE ${tableName3} (
            id INT,
            s STRING,
            v INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num' = '1')
    """
    create_sync_mv(context.dbName, tableName3, cmpSyncMvName + "_cast", """
        SELECT id AS mv_id, MIN(CAST(s AS TIMESTAMPTZ(6))) AS mv_min
        FROM ${tableName3}
        GROUP BY id
    """)
}
