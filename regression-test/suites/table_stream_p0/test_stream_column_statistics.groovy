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

suite("test_stream_column_statistics") {
    sql "DROP STREAM IF EXISTS stream_col_stats_dup_s"
    sql "DROP STREAM IF EXISTS stream_col_stats_unique_s"
    sql "DROP TABLE IF EXISTS stream_col_stats_dup"
    sql "DROP TABLE IF EXISTS stream_col_stats_unique"
    sql """
        CREATE TABLE stream_col_stats_dup (id INT, v INT)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num"="1", "changelog.enable"="true")
    """
    sql """
        CREATE TABLE stream_col_stats_unique (id INT, v INT)
        UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="true",
                    "changelog.enable"="true", "changelog.need_historical_value"="true")
    """
    sql "INSERT INTO stream_col_stats_dup VALUES (1, NULL), (2, 20), (3, 20)"
    sql "INSERT INTO stream_col_stats_unique VALUES (1, NULL), (2, 20), (3, 30)"
    sql """CREATE STREAM stream_col_stats_dup_s ON TABLE stream_col_stats_dup
           PROPERTIES ("type"="append_only", "show_initial_rows"="false")"""
    sql """CREATE STREAM stream_col_stats_unique_s ON TABLE stream_col_stats_unique
           PROPERTIES ("type"="min_delta", "show_initial_rows"="false")"""
    sql "INSERT INTO stream_col_stats_dup VALUES (4, NULL), (5, 50)"
    sql "INSERT INTO stream_col_stats_unique VALUES (1, 10), (4, NULL)"
    sql "DELETE FROM stream_col_stats_unique WHERE id = 2"
    sql "SYNC"

    // The per-column population may predate the current scan estimate. Set v first and
    // id last: this deliberately keeps v's count/NULL pair at 1000/900, while the table's
    // injected scan row count is 100. A historical scan estimates 90 NULLs, not 900 or 9.
    sql """ALTER TABLE stream_col_stats_dup MODIFY COLUMN v SET STATS
           ('row_count'='1000', 'ndv'='100', 'num_nulls'='900', 'avg_size'='4',
            'min_value'='1', 'max_value'='100')"""
    sql """ALTER TABLE stream_col_stats_dup MODIFY COLUMN id SET STATS
           ('row_count'='100', 'ndv'='100', 'num_nulls'='0', 'avg_size'='4',
            'min_value'='1', 'max_value'='100')"""
    sql """ALTER TABLE stream_col_stats_unique MODIFY COLUMN v SET STATS
           ('row_count'='1000', 'ndv'='100', 'num_nulls'='900', 'avg_size'='4',
            'min_value'='1', 'max_value'='100')"""
    sql """ALTER TABLE stream_col_stats_unique MODIFY COLUMN id SET STATS
           ('row_count'='100', 'ndv'='100', 'num_nulls'='0', 'avg_size'='4',
            'min_value'='1', 'max_value'='100')"""

    delegate.quickRunTest("snapshot_null_estimate",
            "EXPLAIN PHYSICAL PLAN SELECT v FROM stream_col_stats_dup_s@snapshot() WHERE v IS NULL",
            false, { row -> row.collect { value -> value.toString()
                    .replaceAll(/cost = [^\n]+/, "cost = <cost>")
                    .replaceAll(/#\d+/, "#")
                    .replaceAll(/@\d+/, "@")
                    .replaceAll(/\[\d+\]/, "[]")
                    .replaceAll(/selectedIndexId=\d+/, "selectedIndexId=<id>") } })

    order_qt_dup_snapshot "SELECT id, v FROM stream_col_stats_dup_s@snapshot()"
    order_qt_dup_delta "SELECT id, v FROM stream_col_stats_dup_s"
    order_qt_dup_reset "SELECT id, v FROM stream_col_stats_dup_s@reset()"
    order_qt_unique_snapshot "SELECT id, v FROM stream_col_stats_unique_s@snapshot()"
    order_qt_unique_delta "SELECT id, v, __DORIS_STREAM_CHANGE_TYPE_COL__ FROM stream_col_stats_unique_s"
    order_qt_unique_reset "SELECT id, v FROM stream_col_stats_unique_s@reset()"
    sql "SET enable_runtime_filter_prune=true"
    order_qt_snapshot_join """
        SELECT d.id, d.v, u.v
        FROM stream_col_stats_dup_s@snapshot() d
        LEFT JOIN stream_col_stats_unique_s@snapshot() u ON d.id = u.id
    """
}
