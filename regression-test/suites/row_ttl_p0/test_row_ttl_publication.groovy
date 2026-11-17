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

suite("test_row_ttl_publication", "nonConcurrent") {
    sql "DROP TABLE IF EXISTS row_ttl_publication"
    sql """CREATE TABLE row_ttl_publication (k INT, sub_k INT, event_time DATETIMEV2(6), v INT)
        DUPLICATE KEY(k, sub_k) PARTITION BY RANGE(k) (
            PARTITION p1 VALUES LESS THAN (2), PARTITION p2 VALUES LESS THAN (4))
        DISTRIBUTED BY HASH(k) BUCKETS 1
        ROLLUP (ttl_rollup(sub_k, k, v))
        PROPERTIES ("replication_num"="1", "disable_auto_compaction"="true",
            "function_column.enable_row_ttl"="true", "function_column.ttl_col"="event_time",
            "function_column.ttl"="36500 day", "function_column.ttl_time_zone"="+00:00")"""
    sql "INSERT INTO row_ttl_publication VALUES (1, 1, '2000-01-01', 10), (2, 2, '2000-01-01', 20), (3, 3, NULL, 30)"
    def failPoint = "SchemaChangeHandler.updateRowTtl.fail_after_partitions"
    try {
        GetDebugPoint().enableDebugPointForAllFEs(failPoint, [value: "1"])
        test {
            sql """ALTER TABLE row_ttl_publication SET ("function_column.ttl"="0")"""
            exception "row TTL update failed after 1 partitions"
        }
        order_qt_shorter_published "SELECT k, v FROM row_ttl_publication ORDER BY k"
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(failPoint)
    }
    sql """ALTER TABLE row_ttl_publication SET ("function_column.ttl"="0")"""
    test {
        sql """ALTER TABLE row_ttl_publication SET ("function_column.ttl"="36500 day")"""
        exception "Increasing function_column.ttl is not supported"
    }
    order_qt_retry_published "SELECT k, event_time, v FROM row_ttl_publication ORDER BY k"
    explain {
        sql "SELECT /*+ USE_MV(row_ttl_publication.ttl_rollup) */ k, v FROM row_ttl_publication"
        contains "row_ttl_publication(ttl_rollup)"
    }
    order_qt_rollup_policy "SELECT /*+ USE_MV(row_ttl_publication.ttl_rollup) */ k, v FROM row_ttl_publication ORDER BY k"
    trigger_and_wait_compaction("row_ttl_publication", "full")
    order_qt_after_gc "SELECT /*+ USE_MV(row_ttl_publication.ttl_rollup) */ k, v FROM row_ttl_publication ORDER BY k"
    if (!isCloudMode()) {
        def tablets = sql_return_maparray("SHOW TABLETS FROM row_ttl_publication")
        def counts = tablets.collect { tablet ->
            def (code, out, err) = curl("GET", tablet.MetaUrl)
            assertEquals(0, code)
            def meta = parseJson(out.trim())
            [meta.row_ttl_config.duration_us, meta.row_ttl_config.config_version,
             meta.rs_metas.sum { it.num_rows as long }]
        }.sort { a, b -> a[2] <=> b[2] }
        counts.each { row -> qt_base_rollup_policy "SELECT ${row[0]}, ${row[1]}, ${row[2]}" }
    }
}
