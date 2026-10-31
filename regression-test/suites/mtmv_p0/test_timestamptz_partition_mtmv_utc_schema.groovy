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
 * A date_trunc-on-TIMESTAMPTZ partition makes the whole definition's effective session zone UTC, and the
 * stored schema has to be derived in that same zone: creation-session constant folding can leave a column
 * NOT NULL (e.g. IF(&lt;zone dependent comparison&gt;, 1, NULL) with a non-null creation-session result) while
 * the refresh, which reparses the SQL in UTC, folds it to NULL - the sink then rejects the rows.
 *
 * The definition must also not pin a time_zone of its own: a SET_VAR hint is re-applied every time the SQL
 * is parsed (refresh command and rewrite cache), so it would silently override the UTC semantics the
 * partition boundaries are derived in.
 */
suite("test_timestamptz_partition_mtmv_utc_schema","mtmv") {
    def dbName = "timestamptz_partition_mtmv_utc_schema"
    def tableName = "timestamptz_partition_mtmv_utc_schema_table"
    def mvName = "timestamptz_partition_mtmv_utc_schema_mv"

    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE ${dbName}"
    sql "USE ${dbName}"

    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"

    def creationTz = '+08:00'
    sql "SET time_zone = '${creationTz}'"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            ts TIMESTAMPTZ(6)
        )
        DUPLICATE KEY(id)
        PARTITION BY RANGE(ts) (
            PARTITION p0 VALUES [('2024-01-01 00:00:00+00:00'), ('2024-01-02 00:00:00+00:00'))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num' = '1')
    """
    sql "INSERT INTO ${tableName} VALUES (1, '2024-01-01 20:30:00+00:00')"
    sql "sync"

    // In +08:00 the offset-free literal '2024-01-01 00:00:00' denotes 2023-12-31T16:00Z, so the comparison
    // folds to TRUE (c is a non-null const) at creation; in UTC it folds to NULL, which is what the refresh
    // materializes. Deriving the stored schema under UTC keeps the column nullable and the refresh valid.
    sql "DROP MATERIALIZED VIEW IF EXISTS ${mvName}"
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        PARTITION BY(day_ts)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES('replication_num' = '1')
        AS
        SELECT date_trunc(ts, 'day') AS day_ts,
               IF(CAST('2024-01-01 00:00:00' AS TIMESTAMPTZ(6))
                  = CAST('2023-12-31 16:00:00+00:00' AS TIMESTAMPTZ(6)), 1, NULL) AS c
        FROM ${tableName}
        GROUP BY date_trunc(ts, 'day')
    """
    waitingMTMVTaskFinishedByMvName(mvName, dbName)

    def tasks = sql """
        SELECT Status, ErrorMsg
        FROM tasks('type' = 'mv')
        WHERE MvDatabaseName = '${dbName}' AND MvName = '${mvName}'
        ORDER BY CreateTime DESC
        LIMIT 1
    """
    Assert.assertEquals("the refresh must store the UTC-folded NULL instead of rejecting the row: "
            + tasks[0][1], "SUCCESS", tasks[0][0].toString())

    def mvInfos = sql """
        SELECT Name, State, RefreshState
        FROM mv_infos('database' = '${dbName}')
        WHERE Name = '${mvName}'
    """
    Assert.assertEquals("NORMAL", mvInfos[0][1].toString())
    Assert.assertEquals("SUCCESS", mvInfos[0][2].toString())

    // Read in UTC so the asserted strings are independent of the creation zone.
    sql "SET time_zone = '+00:00'"
    def mvRes = sql "SELECT CAST(day_ts AS STRING), c FROM ${mvName} ORDER BY 1"
    Assert.assertEquals(1, mvRes.size())
    Assert.assertTrue("expected the UTC day boundary 2024-01-01 00:00:00.000000+00:00, got " + mvRes[0][0],
            mvRes[0][0].toString().contains("2024-01-01 00:00:00.000000+00:00"))
    Assert.assertNull("the UTC-folded constant must be NULL", mvRes[0][1])

    // A time_zone pinned by the definition itself is re-applied whenever the SQL is parsed, so it cannot be
    // combined with the UTC the partition boundaries are derived in.
    sql "SET time_zone = '${creationTz}'"
    def hintedMvName = "timestamptz_partition_mtmv_utc_schema_hinted_mv"
    sql "DROP MATERIALIZED VIEW IF EXISTS ${hintedMvName}"
    test {
        sql """
            CREATE MATERIALIZED VIEW ${hintedMvName}
            BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
            PARTITION BY(day_ts)
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES('replication_num' = '1')
            AS
            SELECT /*+ SET_VAR(time_zone='+08:00') */ date_trunc(ts, 'day') AS day_ts, SUM(id) AS total
            FROM ${tableName}
            GROUP BY date_trunc(ts, 'day')
        """
        exception "SET_VAR"
    }

    // A hint that pins UTC agrees with the effective zone and stays allowed.
    sql """
        CREATE MATERIALIZED VIEW ${hintedMvName}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        PARTITION BY(day_ts)
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES('replication_num' = '1')
        AS
        SELECT /*+ SET_VAR(time_zone='UTC') */ date_trunc(ts, 'day') AS day_ts, SUM(id) AS total
        FROM ${tableName}
        GROUP BY date_trunc(ts, 'day')
    """
    waitingMTMVTaskFinishedByMvName(hintedMvName, dbName)

    // The allowed MV materializes the UTC day, so an unguarded UTC query may rewrite onto it while a query
    // in the creation zone may not: the cached definition carries the UTC semantics the query would not.
    def hintedQuery = "SELECT date_trunc(ts, 'day') AS day_ts, SUM(id) AS total" +
            " FROM ${tableName} GROUP BY date_trunc(ts, 'day')"
    sql "SET time_zone = '+00:00'"
    mv_rewrite_success(hintedQuery, hintedMvName)
    sql "SET time_zone = '${creationTz}'"
    mv_rewrite_fail(hintedQuery, hintedMvName)
}
