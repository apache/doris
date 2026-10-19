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

suite("test_binlog_rowset_tso_pruning", "nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_binlog_rowset_tso_pruning FORCE"
    sql """
        CREATE TABLE test_binlog_rowset_tso_pruning (k INT, v INT)
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """

    // Read timestamps from FE so that the window uses the same clock and timezone
    // as SQL parsing. Leave a whole second after each group of committed writes.
    def nextBoundary = {
        sleep(1200)
        sql("SELECT DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s')")[0][0].toString()
    }
    sql "INSERT INTO test_binlog_rowset_tso_pruning VALUES (1, 10), (2, 20), (3, 30)"
    def start = nextBoundary()
    sql "INSERT INTO test_binlog_rowset_tso_pruning VALUES (1, 11)"
    sql "INSERT INTO test_binlog_rowset_tso_pruning VALUES (4, 40)"
    sql "DELETE FROM test_binlog_rowset_tso_pruning WHERE k = 2"
    sql "INSERT INTO test_binlog_rowset_tso_pruning VALUES (1, 12)"
    def end = nextBoundary()
    sql "INSERT INTO test_binlog_rowset_tso_pruning VALUES (5, 50)"
    def emptyStart = nextBoundary()
    def emptyEnd = nextBoundary()

    def detail = """
        SELECT k, v, __DORIS_BINLOG_OP__
        FROM test_binlog_rowset_tso_pruning@incr(
            'startTimestamp' = '${start}', 'endTimestamp' = '${end}',
            'incrementType' = 'DETAIL')
    """
    def minDelta = detail.replace("'DETAIL'", "'MIN_DELTA'")

    // The before-images must survive pruning of the seed rowset. Repeated updates
    // must still fold correctly in MIN_DELTA, and deletes must remain visible.
    order_qt_detail detail
    order_qt_min_delta minDelta
    order_qt_start_only """
        SELECT k, v, __DORIS_BINLOG_OP__
        FROM test_binlog_rowset_tso_pruning@incr(
            'startTimestamp' = '${start}', 'incrementType' = 'DETAIL')
    """
    order_qt_end_only """
        SELECT k, v, __DORIS_BINLOG_OP__
        FROM test_binlog_rowset_tso_pruning@incr(
            'endTimestamp' = '${start}', 'incrementType' = 'DETAIL')
    """
    order_qt_empty_detail """
        SELECT k, v FROM test_binlog_rowset_tso_pruning@incr(
            'startTimestamp' = '${emptyStart}', 'endTimestamp' = '${emptyEnd}',
            'incrementType' = 'DETAIL')
    """
    order_qt_empty_min_delta """
        SELECT k, v FROM test_binlog_rowset_tso_pruning@incr(
            'startTimestamp' = '${emptyStart}', 'endTimestamp' = '${emptyEnd}',
            'incrementType' = 'MIN_DELTA')
    """
    order_qt_snapshot "SELECT k, v FROM test_binlog_rowset_tso_pruning"

    // The existing helper triggers local compaction. Cloud mode still exercises
    // the common pruning path above, without relying on local compaction APIs.
    if (!isCloudMode()) {
        setBeConfigTemporary([
            binlog_compaction_goal_size_mbytes: 0,
            binlog_compaction_file_count_threshold: 2,
            binlog_compaction_wait_timesec_after_visible: 0,
            binlog_compaction_time_threshold_seconds: 86400
        ]) {
            trigger_and_wait_compaction("test_binlog_rowset_tso_pruning", "cumulative")
            // A compacted rowset can span both sides of the query window. Its
            // remaining segment/row predicates must enforce the original bounds.
            order_qt_compacted_detail detail
            order_qt_compacted_min_delta minDelta
            order_qt_compacted_snapshot "SELECT k, v FROM test_binlog_rowset_tso_pruning"
        }
    }
}
