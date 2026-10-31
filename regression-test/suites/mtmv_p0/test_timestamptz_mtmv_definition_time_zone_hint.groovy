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
 * A materialized view definition that pins its own time_zone with a SET_VAR hint is materialized in that
 * zone on every refresh, because the hint is re-applied whenever the stored SQL is parsed. The persisted
 * session variables therefore have to record the hinted zone instead of the creating session's zone:
 * otherwise a query in the creating zone matches the persisted map, takes the unguarded rewrite cache and
 * reads values materialized in the hinted zone.
 */
suite("test_timestamptz_mtmv_definition_time_zone_hint","mtmv") {
    def dbName = "timestamptz_mtmv_definition_time_zone_hint"
    def tableName = "timestamptz_mtmv_definition_time_zone_hint_table"
    def mvName = "timestamptz_mtmv_definition_time_zone_hint_mv"

    sql "DROP DATABASE IF EXISTS ${dbName}"
    sql "CREATE DATABASE ${dbName}"
    sql "USE ${dbName}"

    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"

    def creationTz = '+00:00'
    def hintedTz = '+08:00'

    sql "SET time_zone = '${creationTz}'"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            ts TIMESTAMPTZ(6) NOT NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num' = '1')
    """
    // 2024-01-01 02:30Z is day 2024-01-01 in both zones, but the +08:00 day boundary is
    // 2023-12-31 16:00Z while the UTC one is 2024-01-01 00:00Z, so a rewrite across the two zones is
    // observable.
    sql "INSERT INTO ${tableName} VALUES (1, '2024-01-01 02:30:00+00:00')"
    sql "sync"

    // Created in UTC, but the definition pins +08:00, which is what the refresh applies.
    sql "DROP MATERIALIZED VIEW IF EXISTS ${mvName}"
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES('replication_num' = '1')
        AS
        SELECT /*+ SET_VAR(time_zone='${hintedTz}') */ id, date_trunc(ts, 'day') AS d
        FROM ${tableName}
    """
    waitingMTMVTaskFinishedByMvName(mvName, dbName)

    // The content is the +08:00 day boundary.
    sql "SET time_zone = '${hintedTz}'"
    def hintedRes = sql "SELECT CAST(d AS STRING) FROM ${mvName} ORDER BY 1"
    Assert.assertEquals(1, hintedRes.size())
    Assert.assertTrue("expected the +08:00 day boundary 2023-12-31T16:00Z, got " + hintedRes[0][0],
            hintedRes[0][0].toString().contains("2024-01-01 00:00:00"))

    def query = "SELECT id, date_trunc(ts, 'day') AS d FROM ${tableName}"

    // A query in the hinted zone agrees with the materialized content and may rewrite.
    sql "SET time_zone = '${hintedTz}'"
    mv_rewrite_success(query, mvName)

    // A query in the creating zone must not read it: its own day boundary is the UTC one.
    sql "SET time_zone = '${creationTz}'"
    mv_rewrite_fail(query, mvName)
    sql "SET enable_materialized_view_rewrite = false"
    def direct = sql "SELECT id, CAST(date_trunc(ts, 'day') AS STRING) FROM ${tableName} ORDER BY 1"
    sql "SET enable_materialized_view_rewrite = true"
    def guarded = sql "SELECT id, CAST(date_trunc(ts, 'day') AS STRING) FROM ${tableName} ORDER BY 1"
    Assert.assertEquals("the cross-zone query must return the query-zone day",
            direct[0][1].toString(), guarded[0][1].toString())
    Assert.assertTrue("expected the UTC day boundary 2024-01-01T00:00Z, got " + guarded[0][1],
            guarded[0][1].toString().contains("2024-01-01 00:00:00"));

    // Every hint scope is applied to the shared statement session while that scope is analyzed, so two
    // scopes pinning different zones would materialize different branches in different zones while the
    // definition can only persist one. Such a definition is rejected.
    def conflictingMvName = "timestamptz_mtmv_definition_time_zone_hint_conflict_mv"
    sql "DROP MATERIALIZED VIEW IF EXISTS ${conflictingMvName}"
    test {
        sql """
            CREATE MATERIALIZED VIEW ${conflictingMvName}
            BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES('replication_num' = '1')
            AS
            SELECT /*+ SET_VAR(time_zone='${hintedTz}') */ id, d
            FROM (
                SELECT /*+ SET_VAR(time_zone='${creationTz}') */ id, date_trunc(ts, 'day') AS d
                FROM ${tableName}
            ) t
        """
        exception "conflicting time zones"
    }

    // The same zone pinned by both scopes is consistent and stays allowed.
    def nestedMvName = "timestamptz_mtmv_definition_time_zone_hint_nested_mv"
    sql "DROP MATERIALIZED VIEW IF EXISTS ${nestedMvName}"
    sql """
        CREATE MATERIALIZED VIEW ${nestedMvName}
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES('replication_num' = '1')
        AS
        SELECT /*+ SET_VAR(time_zone='${hintedTz}') */ id, d
        FROM (
            SELECT /*+ SET_VAR(time_zone='${hintedTz}') */ id, date_trunc(ts, 'day') AS d
            FROM ${tableName}
        ) t
    """
    waitingMTMVTaskFinishedByMvName(nestedMvName, dbName)
}
