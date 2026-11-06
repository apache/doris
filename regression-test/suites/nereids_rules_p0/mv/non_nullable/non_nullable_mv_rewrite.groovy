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

import org.apache.doris.regression.util.Http

suite("non_nullable_mv_rewrite") {
    sql "SET enable_sql_cache = false"
    sql "SET enable_segment_limit_pushdown = true"
    sql "SET ann_index_candidate_rows_threshold = 0"
    sql "SET ann_index_candidate_rows_percent_threshold = 0"
    sql "DROP MATERIALIZED VIEW IF EXISTS non_nullable_mv"
    sql "DROP MATERIALIZED VIEW IF EXISTS non_nullable_part_mv"
    sql "DROP TABLE IF EXISTS non_nullable_base"
    sql "DROP TABLE IF EXISTS non_nullable_part_base"
    sql "DROP TABLE IF EXISTS non_nullable_keys"
    sql "DROP MATERIALIZED VIEW IF EXISTS non_nullable_helper_mv"
    sql "DROP TABLE IF EXISTS non_nullable_helper_base"
    sql """CREATE TABLE non_nullable_base (id INT, value INT NULL, vec ARRAY<FLOAT> NULL)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")"""
    sql """INSERT INTO non_nullable_base VALUES
        (0, 10, [0.0, 0.0, 0.0]), (1, 20, [1.0, 0.0, 0.0]),
        (2, 30, [2.0, 0.0, 0.0]), (3, 40, [3.0, 0.0, 0.0])"""
    sql """CREATE MATERIALIZED VIEW non_nullable_mv
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
        AS SELECT id, non_nullable(value) AS value, non_nullable(vec) AS vec FROM non_nullable_base"""
    sql """CREATE INDEX non_nullable_ann ON non_nullable_mv(vec) USING ANN
        PROPERTIES ("index_type" = "hnsw", "metric_type" = "l2_distance", "dim" = "3")"""
    String indexJobs = "SHOW ALTER TABLE COLUMN WHERE TableName = \"non_nullable_mv\" ORDER BY CreateTime DESC LIMIT 1"
    // Light index changes finish in the DDL; ordinary schema changes publish an alter job.
    if (!sql(indexJobs).isEmpty()) {
        waitForSchemaChangeDone {
            sql indexJobs
            time 600
        }
    }
    sql "REFRESH MATERIALIZED VIEW non_nullable_mv COMPLETE"
    waitingMTMVTaskFinishedByMvName("non_nullable_mv")
    sql "ANALYZE TABLE non_nullable_base WITH SYNC"
    sql "ANALYZE TABLE non_nullable_mv WITH SYNC"

    // Stabilize the cost choice for this tiny fixture, including the nullable output projection.
    sql """ALTER TABLE non_nullable_base MODIFY COLUMN id SET STATS ("row_count" = "1000")"""

    String plain = "SELECT /*+ use_mv(non_nullable_mv) */ id, value, vec FROM non_nullable_base ORDER BY id"
    mv_rewrite_success(plain, "non_nullable_mv")
    qt_plain plain
    sql "SET enable_materialized_view_rewrite = false"
    qt_plain_without_mv "SELECT id, value, vec FROM non_nullable_base ORDER BY id"
    sql "SET enable_materialized_view_rewrite = true"
    String wrapped = "SELECT /*+ use_mv(non_nullable_mv) */ id, non_nullable(vec) FROM non_nullable_base ORDER BY id"
    mv_rewrite_success(wrapped, "non_nullable_mv")
    qt_wrapped wrapped
    String filtered = "SELECT /*+ use_mv(non_nullable_mv) */ id, vec FROM non_nullable_base WHERE value > 20 ORDER BY id"
    mv_rewrite_success(filtered, "non_nullable_mv")
    qt_filtered filtered

    sql "SET enable_profile = true"
    sql "SET profile_level = 2"
    String ann = """SELECT /*+ use_mv(non_nullable_mv) */ id,
        l2_distance_approximate(vec, [0.0, 0.0, 0.0]) AS distance FROM non_nullable_base
        ORDER BY distance LIMIT 3"""
    mv_rewrite_success(ann, "non_nullable_mv")
    explain {
        sql ann
        contains "non_nullable_mv"
        contains "ANN SORT INFO"
    }
    qt_ann ann
    String queryId = sql("SELECT last_query_id()")[0][0].toString()
    boolean searchedIndex = false
    for (int attempt = 0; attempt < 30 && !searchedIndex; attempt++) {
        String profile = Http.GET("http://${context.config.feHttpAddress}/api/profile/text/?query_id=${queryId}",
                false, false, context.config.feHttpUser, context.config.feHttpPassword).toString()
        def counters = profile =~ /AnnIndexTopNSearchCnt:\s*([0-9]+)/
        searchedIndex = counters.any { it[1].toLong() > 0 }
        if (!searchedIndex) {
            sleep(500)
        }
    }
    assertTrue(searchedIndex, "The rewritten query must execute an ANN index search")
    sql "SET enable_profile = false"

    // A surviving cast must use scalar evaluation even when its underlying column has an ANN index.
    ["", "WHERE id > 0"].eachWithIndex { String filter, int i ->
        String castDistance = """SELECT id, l2_distance_approximate(
            nullable(CAST(CAST(vec AS ARRAY<DOUBLE>) AS ARRAY<FLOAT>)), [0.0, 0.0, 0.0]) AS distance
            FROM non_nullable_mv ${filter} ORDER BY distance LIMIT 3"""
        explain {
            sql castDistance
            notContains "ANN SORT INFO"
        }
        quickTest("ann_cast_${i}", castDistance,
            """SELECT /*+ SET_VAR(enable_materialized_view_rewrite=false) */ id,
                l2_distance_approximate(vec, [0.0, 0.0, 0.0]) AS distance
                FROM non_nullable_base ${filter} ORDER BY distance LIMIT 3""")
    }

    sql """CREATE TABLE non_nullable_keys (id INT)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")"""
    sql "INSERT INTO non_nullable_keys VALUES (0), (4)"
    // A join above the substituted scan must still generate a NULL vector for an unmatched row.
    String outerJoin = """SELECT /*+ use_mv(non_nullable_mv) */ k.id, b.vec
        FROM non_nullable_keys k LEFT JOIN non_nullable_base b ON k.id = b.id ORDER BY k.id"""
    mv_rewrite_success(outerJoin, "non_nullable_mv")
    qt_outer_join outerJoin
    sql "SET enable_materialized_view_rewrite = false"
    qt_outer_join_without_mv """SELECT k.id, b.vec FROM non_nullable_keys k
        LEFT JOIN non_nullable_base b ON k.id = b.id ORDER BY k.id"""
    sql "SET enable_materialized_view_rewrite = true"

    sql "INSERT INTO non_nullable_base VALUES (4, 50, NULL)"
    test {
        sql "SELECT non_nullable(vec) FROM non_nullable_base WHERE id = 4"
        exception "illegal for non_nullable"
    }
    sql "REFRESH MATERIALIZED VIEW non_nullable_mv COMPLETE"
    waitingMTMVTaskFinishedNotNeedSuccess(getJobName(context.dbName, "non_nullable_mv"))
    // Failed refreshes must preserve the last published data and retain source NULL rows on fallback.
    qt_failed_refresh """SELECT Status, ErrorMsg LIKE "%illegal for non_nullable%"
        FROM tasks("type" = "mv") WHERE MvDatabaseName = "${context.dbName}" AND MvName = "non_nullable_mv"
        ORDER BY CreateTime DESC LIMIT 1"""
    qt_previous_mv "SELECT id, vec FROM non_nullable_mv ORDER BY id"
    mv_not_part_in("SELECT id, vec FROM non_nullable_base WHERE vec IS NULL", "non_nullable_mv")
    qt_null_fallback "SELECT id, vec FROM non_nullable_base WHERE vec IS NULL ORDER BY id"

    sql """CREATE TABLE non_nullable_part_base (id INT, d DATE NOT NULL, vec ARRAY<FLOAT> NULL)
        DUPLICATE KEY(id, d)
        PARTITION BY RANGE(d) (
            PARTITION p1 VALUES [('2024-01-01'), ('2024-01-02')),
            PARTITION p2 VALUES [('2024-01-02'), ('2024-01-03'))
        ) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")"""
    sql """INSERT INTO non_nullable_part_base VALUES
        (1, '2024-01-01', [1.0, 0.0, 0.0]), (2, '2024-01-02', NULL)"""
    // Make a partial MV cheaper than scanning the whole source without overriding partition statistics.
    sql """INSERT INTO non_nullable_part_base SELECT number + 10, '2024-01-01', [1.0, 0.0, 0.0]
        FROM numbers("number" = "100")"""
    sql """CREATE MATERIALIZED VIEW non_nullable_part_mv
        BUILD DEFERRED REFRESH AUTO ON MANUAL PARTITION BY(d)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")
        AS SELECT id, d, non_nullable(vec) AS vec FROM non_nullable_part_base"""
    sql "REFRESH MATERIALIZED VIEW non_nullable_part_mv PARTITIONS(p_20240101_20240102)"
    waitingMTMVTaskFinishedByMvName("non_nullable_part_mv")
    sql "ANALYZE TABLE non_nullable_part_base WITH SYNC"
    // The unmaterialized partition still contains a NULL; only the MV branch is non-nullable.
    String partial = """SELECT /*+ use_mv(non_nullable_part_mv) */ id, d, vec
        FROM non_nullable_part_base WHERE id < 3 ORDER BY id"""
    mv_rewrite_success(partial, "non_nullable_part_mv")
    explain {
        sql partial
        contains "VUNION"
    }
    qt_partial partial
    sql "SET enable_materialized_view_rewrite = false"
    qt_partial_without_mv "SELECT id, d, vec FROM non_nullable_part_base WHERE id < 3 ORDER BY id"
    sql "SET enable_materialized_view_rewrite = true"

    // Exercise the shared array/ANN helper even when external fixtures are unavailable.
    sql """CREATE TABLE non_nullable_helper_base (id INT NULL, vec ARRAY<FLOAT> NULL)
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")"""
    sql "INSERT INTO non_nullable_helper_base VALUES (1, [1.0, 0.0]), (2, [2.0, 0.0])"
    sql "ANALYZE TABLE non_nullable_helper_base WITH SYNC"
    sql """ALTER TABLE non_nullable_helper_base MODIFY COLUMN id SET STATS ("row_count" = "1000")"""
    checkNonNullableMvRewrite("non_nullable_helper_base", ["id", "vec"], "non_nullable_helper_mv")

}
