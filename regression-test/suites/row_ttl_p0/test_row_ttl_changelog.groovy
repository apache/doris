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

suite("test_row_ttl_changelog", "nonConcurrent") {
    def compactBaseAndChangelog = {
        def tablets = sql_return_maparray("SHOW TABLETS FROM row_ttl_changelog")
        def baseTablets = tablets.findAll { it.IsChangelog == 'false' }.collect { it.TabletId }
        def changelogTablets = tablets.findAll { it.IsChangelog == 'true' }.collect { it.TabletId }
        // Changelog tablets only support their dedicated cumulative compaction path.
        trigger_and_wait_compaction("row_ttl_changelog", "full", 300, [] as String[], baseTablets)
        trigger_and_wait_compaction("row_ttl_changelog", "cumulative", 300, [] as String[], changelogTablets)
    }
    for (boolean vertical : [false, true]) {
        setBeConfigTemporary([
            enable_vertical_compaction: vertical,
            vertical_compaction_num_columns_per_group: 1,
            changelog_compaction_goal_size_mbytes: 0,
            changelog_compaction_file_count_threshold: 2,
            changelog_compaction_wait_timesec_after_visible: 0,
            changelog_compaction_time_threshold_seconds: 86400
        ]) {
            for (boolean sourceColumn : [false, true]) {
                for (boolean mow : [false, true]) {
                    sql "DROP TABLE IF EXISTS row_ttl_changelog"
                    def keys = mow ? "UNIQUE" : "DUPLICATE"
                    def mowProperty = mow ? '''
                        , "enable_unique_key_merge_on_write"="true",
                        "enable_unique_key_skip_bitmap_column"="true"
                    ''' : ''
                    def ttlProperties = sourceColumn ? '''
                        , "function_column.ttl_col"="event_time", "function_column.ttl"="0",
                        "function_column.ttl_time_zone"="+00:00"
                    ''' : ''
                    sql """
                        CREATE TABLE row_ttl_changelog (k INT, event_time DATETIMEV2(6), v INT)
                        ${keys} KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
                        PROPERTIES (
                            "replication_num"="1", "disable_auto_compaction"="true",
                            "changelog.enable"="true", "function_column.enable_row_ttl"="true"
                            ${mowProperty} ${ttlProperties}
                        )
                    """
                    sql "INSERT INTO row_ttl_changelog VALUES (1, '9999-01-01', 10), (2, '9999-01-01', 20)"
                    if (sourceColumn) {
                        sql "INSERT INTO row_ttl_changelog VALUES (1, '1970-01-01', 11), (3, '1970-01-01', 30)"
                    } else {
                        sql """INSERT INTO row_ttl_changelog(k, event_time, v, __DORIS_TTL_COL__)
                            VALUES (1, '1970-01-01', 11, 0), (3, '1970-01-01', 30, 0)"""
                    }
                    sql "INSERT INTO row_ttl_changelog VALUES (4, NULL, 40)"

                    "order_qt_base_before_${vertical}_${sourceColumn}_${mow}"(
                            "SELECT k, v FROM row_ttl_changelog ORDER BY k, v")
                    // Raw changelog retains all five writes, including the expired successor.
                    // Row TTL must not remove log entries when reclaiming the base table.
                    "order_qt_log_before_${vertical}_${sourceColumn}_${mow}"(
                            "SELECT k, v FROM changelog('table'='row_ttl_changelog') ORDER BY k, v")
                    compactBaseAndChangelog()
                    "order_qt_base_after_${vertical}_${sourceColumn}_${mow}"(
                            "SELECT k, v FROM row_ttl_changelog ORDER BY k, v")
                    "order_qt_log_after_${vertical}_${sourceColumn}_${mow}"(
                            "SELECT k, v FROM changelog('table'='row_ttl_changelog') ORDER BY k, v")

                    test {
                        sql "SELECT k FROM row_ttl_changelog FOR VERSION AS OF 1"
                        exception "FOR VERSION/TIME AS OF is not supported on tables with row TTL"
                    }
                    test {
                        sql "SELECT k FROM row_ttl_changelog@incr()"
                        exception "INCR query is not supported on tables with row TTL"
                    }
                    test {
                        sql "CREATE STREAM row_ttl_changelog_stream ON TABLE row_ttl_changelog"
                        exception "CREATE STREAM is not supported on tables with row TTL"
                    }
                    if (mow) {
                        sql "UPDATE row_ttl_changelog SET v=21 WHERE k=2"
                        sql "UPDATE row_ttl_changelog SET v=22 WHERE k=2"
                        def expiration = sourceColumn ? "event_time='1970-01-01'" : "__DORIS_TTL_COL__=0"
                        sql "UPDATE row_ttl_changelog SET ${expiration} WHERE k=4"
                        sql "INSERT INTO row_ttl_changelog(k,v) VALUES (5,50)"
                        "order_qt_update_${vertical}_${sourceColumn}"(
                                "SELECT k, event_time, v FROM row_ttl_changelog ORDER BY k")
                        "order_qt_update_log_${vertical}_${sourceColumn}"(
                                "SELECT k, v FROM changelog('table'='row_ttl_changelog') ORDER BY k, v")
                        compactBaseAndChangelog()
                        "order_qt_update_compacted_${vertical}_${sourceColumn}"(
                                "SELECT k, event_time, v FROM row_ttl_changelog ORDER BY k")
                        "order_qt_update_log_compacted_${vertical}_${sourceColumn}"(
                                "SELECT k, v FROM changelog('table'='row_ttl_changelog') ORDER BY k, v")
                    }
                }
            }
        }
    }
}
