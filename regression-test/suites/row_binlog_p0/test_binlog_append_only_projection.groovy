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

// Regression test for issue #66390: a ROW-binlog APPEND_ONLY scan whose SQL projection
// contains fewer columns than the tablet key count (or omits the leading key columns)
// used to force a key-ordered merge in the storage layer. The merge comparator then read
// key positions that do not exist in the projected blocks and crashed the BE with SIGSEGV
// inside VMergeIterator::init / std::push_heap.
//
// The table has TWO key columns and the queries below project one value column only
// (0 key columns read), or a non-leading key subset, over overlapping multi-rowset data,
// which is exactly the shape that used to crash.
suite("test_binlog_append_only_projection", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }
    sql "DROP DATABASE IF EXISTS test_binlog_append_only_projection_db"
    sql "CREATE DATABASE test_binlog_append_only_projection_db"
    sql "USE test_binlog_append_only_projection_db"
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    def dupTable = "append_only_proj_dup"
    def incrTimeFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    try {
        sql "DROP TABLE IF EXISTS ${dupTable}"

        // Two key columns so that any single-column projection is narrower than the
        // key prefix the merge comparator would use.
        sql """
            CREATE TABLE ${dupTable} (
                k1 BIGINT,
                k2 INT,
                v1 INT,
                v2 VARCHAR(16) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(k1, k2)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "binlog.enable" = "true",
                "binlog.format" = "ROW"
            )
        """

        sql "INSERT INTO ${dupTable} VALUES (1, 1, 10, 'seed1')"
        sql "INSERT INTO ${dupTable} VALUES (2, 2, 20, 'seed2')"
        sql "sync"
        sleep(1200)
        def t0 = incrTimeFormat.format(new Date())
        sleep(1200)

        // Each INSERT produces its own rowset; interleaved keys make the rowsets'
        // key ranges overlap, so a key-ordered read would need a real merge.
        sql "INSERT INTO ${dupTable} VALUES (1, 3, 30, 'w1'), (9, 1, 31, 'w1')"
        sql "INSERT INTO ${dupTable} VALUES (2, 4, 40, NULL), (8, 2, 41, 'w2')"
        sql "INSERT INTO ${dupTable} VALUES (1, 5, 50, 'w3'), (9, 3, 51, NULL)"
        sql "sync"
        sleep(1200)
        def t1 = incrTimeFormat.format(new Date())
        sleep(1200)
        sql "INSERT INTO ${dupTable} VALUES (7, 7, 70, 'late')"
        sql "sync"

        // 1. Project a single value column: 1 projected column < 2 key columns.
        //    This is the exact shape that used to crash the BE.
        assertEquals([[30], [31], [40], [41], [50], [51]],
                sql("""SELECT v1
                       FROM ${dupTable}@incr('startTimestamp' = '${t0}',
                           "endTimestamp" = "${t1}",
                           "incrementType" = "APPEND_ONLY")
                       ORDER BY v1"""))

        // 2. Project only the second key column: enough columns to compare, but the
        //    leading key k1 is absent, so a positional key comparison would have
        //    silently compared the wrong columns.
        assertEquals([[1], [2], [3], [3], [4], [5]],
                sql("""SELECT k2
                       FROM ${dupTable}@incr('startTimestamp' = '${t0}',
                           "endTimestamp" = "${t1}",
                           "incrementType" = "APPEND_ONLY")
                       ORDER BY k2"""))

        // 3. Projection in reversed column order relative to the schema.
        assertEquals([[30, 3L], [31, 1L], [40, 4L], [41, 2L], [50, 5L], [51, 3L]],
                sql("""SELECT v1, CAST(k2 AS BIGINT)
                       FROM ${dupTable}@incr('startTimestamp' = '${t0}',
                           "endTimestamp" = "${t1}",
                           "incrementType" = "APPEND_ONLY")
                       ORDER BY v1"""))

        // 4. Aggregate over a narrow projection.
        assertEquals([[6L, 243L]],
                sql("""SELECT count(*), sum(v1)
                       FROM ${dupTable}@incr('startTimestamp' = '${t0}',
                           "endTimestamp" = "${t1}",
                           "incrementType" = "APPEND_ONLY")"""))

        // 5. DETAIL / MIN_DELTA keep the forced key-ordered merge but widen the
        //    storage projection with the full key prefix internally; a narrow SQL
        //    projection must still work and return the same rows for a dup table.
        assertEquals([[30], [31], [40], [41], [50], [51]],
                sql("""SELECT v1
                       FROM ${dupTable}@incr('startTimestamp' = '${t0}',
                           "endTimestamp" = "${t1}",
                           "incrementType" = "DETAIL")
                       ORDER BY v1"""))
        assertEquals([[30], [31], [40], [41], [50], [51]],
                sql("""SELECT v1
                       FROM ${dupTable}@incr('startTimestamp' = '${t0}',
                           "endTimestamp" = "${t1}",
                           "incrementType" = "MIN_DELTA")
                       ORDER BY v1"""))
    } finally {
        sql "DROP DATABASE IF EXISTS test_binlog_append_only_projection_db"
    }
}
