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

suite("test_zonemap_always_true_count_on_index", "p0, nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_zonemap_always_true_count_on_index"
    sql "set enable_count_on_index_pushdown = true"
    sql "set enable_no_need_read_data_opt = true"
    sql "set experimental_enable_nereids_planner = true"
    sql "set enable_fallback_to_original_planner = false"
    sql "set inverted_index_skip_threshold = 0"

    sql """
        CREATE TABLE test_zonemap_always_true_count_on_index (
            k INT,
            app_id VARCHAR(32) NOT NULL,
            event_time DATETIMEV2 NOT NULL,
            v INT,
            INDEX idx_app_id (`app_id`) USING INVERTED COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(k, app_id)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        );
    """

    sql """
        INSERT INTO test_zonemap_always_true_count_on_index VALUES
            (1, 'app_a', '2026-03-28 00:10:00', 10),
            (2, 'app_a', '2026-03-28 12:20:00', 20),
            (3, 'app_b', '2026-03-28 23:30:00', 30);
    """
    sql "sync"

    def countSql = """
        SELECT COUNT(1) FROM test_zonemap_always_true_count_on_index
        WHERE event_time >= '2026-03-28 00:00:00'
          AND event_time < '2026-03-29 00:00:00'
          AND app_id = 'app_a'
    """

    explain {
        sql(countSql)
        contains "pushAggOp=COUNT_ON_INDEX"
    }

    def countWithKeyRangeSql = """
        SELECT COUNT(1) FROM test_zonemap_always_true_count_on_index
        WHERE k >= 1
          AND k < 4
          AND app_id = 'app_a'
    """

    explain {
        sql(countWithKeyRangeSql)
        contains "pushAggOp=COUNT_ON_INDEX"
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("segment_iterator._read_columns_by_index",
                [column_name: "event_time"])
        qt_count countSql
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("segment_iterator._read_columns_by_index")
    }

    qt_count_with_key_range countWithKeyRangeSql

    // A zone map can prove IN and IS NOT NULL always true as well, not only a comparison.
    // Both columns below are non-key and are not selected, so once their predicates are proved
    // redundant their data must never be read.
    def moreTable = "test_zonemap_always_true_more_predicates"
    sql "DROP TABLE IF EXISTS ${moreTable}"
    sql """
        CREATE TABLE ${moreTable} (
            k INT,
            app_id VARCHAR(32) NOT NULL,
            tag INT NOT NULL,
            v INT,
            INDEX idx_app_id_more (`app_id`) USING INVERTED COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(k, app_id)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        );
    """

    // Every row carries the same `tag`, so a list holding that one value covers the whole zone.
    // No row has a NULL `v`.
    sql """
        INSERT INTO ${moreTable} VALUES
            (1, 'app_a', 5, 10),
            (2, 'app_a', 5, 20),
            (3, 'app_b', 5, 30);
    """
    sql "sync"

    def alwaysTrueSql = """
        SELECT COUNT(1) FROM ${moreTable}
        WHERE tag IN (5) AND v IS NOT NULL AND app_id = 'app_a'
    """

    try {
        GetDebugPoint().enableDebugPointForAllBEs("segment_iterator._read_columns_by_index",
                [column_name: "tag,v"])
        qt_always_true_in_and_is_not_null alwaysTrueSql
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("segment_iterator._read_columns_by_index")
    }

    // A delete condition can also drop whole pages. A page holds 16384 rows, so runs of 50000
    // equal values leave whole pages sitting inside a single run. Reading the profile needs a
    // suite that does not race other queries for the FE profile list, which is why this lives
    // here rather than in delete_p0.
    def pageSkipTable = "test_zonemap_delete_page_skip"
    sql """ DROP TABLE IF EXISTS ${pageSkipTable} """
    sql """
    CREATE TABLE ${pageSkipTable} (
        `k1` int NOT NULL,
        `v1` int NOT NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`k1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 1
    PROPERTIES (
    "replication_num" = "1",
    "disable_auto_compaction" = "true"
    );
    """
    sql """insert into ${pageSkipTable} select number, number div 50000 from numbers("number" = "150000");"""

    // Delete one whole run. The pages inside it hold that value only, so the delete condition
    // covers them completely and they can be dropped without being read.
    sql """delete from ${pageSkipTable} where v1 in (1);"""
    qt_whole_page_delete """select v1, count(*) from ${pageSkipTable} group by v1 order by v1;"""

    // The page skip only runs for columns that also carry a query predicate, so query on v1.
    // `v1 >= 1` is false for some rows, which keeps it from being dropped as always true.
    // 50000 rows are deleted; anything less reaching the row level filter means whole pages
    // were dropped by the zone map first.
    sql """ set enable_profile = true; """
    def pageSkipQueryId = "test_zone_map_delete_page_skip_" + System.currentTimeMillis()
    profile(pageSkipQueryId) {
        run {
            sql "/* ${pageSkipQueryId} */ select count(*) from ${pageSkipTable} where v1 >= 1"
        }
        check { profileString, exception ->
            def matcher = java.util.regex.Pattern
                    .compile("RowsDelFiltered:\\s*(?:[\\d.]+[KMB]?\\s*\\()?(\\d+)\\)?")
                    .matcher(profileString)
            assertTrue(matcher.find(), "RowsDelFiltered is missing from the profile")
            def rowsDelFiltered = Integer.parseInt(matcher.group(1))
            log.info("rows the delete condition filtered one by one: {}", rowsDelFiltered)
            assertTrue(rowsDelFiltered < 50000,
                       "expected whole pages to be skipped, RowsDelFiltered=" + rowsDelFiltered)
        }
    }
    sql """ set enable_profile = false; """

    sql """ DROP TABLE IF EXISTS ${pageSkipTable} """
}
