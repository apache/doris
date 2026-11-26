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

    // Keep v's source population at 1000 rows with 900 NULLs. The later injection for id
    // sets the table-level row-count fallback. A snapshot can use its partition baseline instead.
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

    def plan = sql_return_maparray """
        EXPLAIN PHYSICAL PLAN SELECT v FROM stream_col_stats_dup_s@snapshot() WHERE v IS NULL
    """
    def rowEstimate = { String operator ->
        def node = plan.find { it["Explain String(Nereids Planner)"].contains(operator) }
        assertTrue(node != null, "Missing ${operator} in ${plan}")
        def stats = node["Explain String(Nereids Planner)"] =~ /stats=(\p{Nd}+(?:[.,'’\p{Zs}\u066b\u066c]\p{Nd}+)*)/
        assertTrue(stats.find(), "Missing row estimate for ${operator} in ${node}")
        // EXPLAIN uses localized groups of three digits and up to two fractional digits.
        def parts = stats.group(1).split(/[^\p{Nd}]+/).toList()
        if (parts.size() > 1 && parts.last().length() <= 2) {
            return (parts[0..-2].join("") + "." + parts.last()) as BigDecimal
        }
        return parts.join("") as BigDecimal
    }
    // Check the column distribution against the estimate chosen for this snapshot.
    def scanRows = rowEstimate("PhysicalOlapScan")
    def filterRows = rowEstimate("PhysicalFilter")
    assertTrue(scanRows > 0, "Expected a positive snapshot scan estimate in ${plan}")
    qt_snapshot_null_selectivity "SELECT CAST(${filterRows} / ${scanRows} AS DECIMAL(3, 2))"

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
