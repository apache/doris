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

    try {
        GetDebugPoint().enableDebugPointForAllBEs("segment_iterator._read_columns_by_index",
                [column_name: "event_time"])
        qt_count countSql
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("segment_iterator._read_columns_by_index")
    }
}
