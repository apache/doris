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

import org.awaitility.Awaitility
import java.util.concurrent.TimeUnit

suite("test_row_binlog_ttl_publication", "nonConcurrent") {
    sql "DROP TABLE IF EXISTS row_binlog_ttl_publication"
    sql """
        CREATE TABLE row_binlog_ttl_publication (k INT, v INT)
        DUPLICATE KEY(k)
        PARTITION BY RANGE(k) (
            PARTITION p1 VALUES LESS THAN (2),
            PARTITION p2 VALUES LESS THAN (3)
        )
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num"="1", "binlog.enable"="true", "binlog.format"="ROW",
                    "binlog.ttl_seconds"="86400", "disable_auto_compaction"="true")
    """
    sql "INSERT INTO row_binlog_ttl_publication VALUES (1, 10), (2, 20)"
    def ttl = {
        def ddl = sql("SHOW CREATE TABLE row_binlog_ttl_publication")[0][1]
        return (ddl =~ /"binlog\.ttl_seconds" = "(-?\d+)"/)[0][1]
    }
    def failPoint = "SchemaChangeHandler.updateRowBinlogConfig.fail_after_partitions"
    try {
        GetDebugPoint().enableDebugPointForAllFEs(failPoint, [value: "1"])
        test {
            sql """ALTER TABLE row_binlog_ttl_publication SET ("binlog.ttl_seconds"="1")"""
            exception "ROW binlog configuration update failed after 1 partitions"
        }
        // Cleanup may now use the shorter TTL: FE must already advertise it durably.
        qt_shorter_published "SELECT ${ttl()}"
        qt_paused_retained "SELECT count(*) FROM binlog('table'='row_binlog_ttl_publication')"
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(failPoint)
    }
    // Identical retry must update the partition missed by the failed attempt.
    sql """ALTER TABLE row_binlog_ttl_publication SET ("binlog.ttl_seconds"="1")"""
    try {
        GetDebugPoint().enableDebugPointForAllFEs(failPoint, [value: "1"])
        test {
            sql """ALTER TABLE row_binlog_ttl_publication SET ("binlog.ttl_seconds"="86400")"""
            exception "ROW binlog configuration update failed after 1 partitions"
        }
        qt_extension_not_published "SELECT ${ttl()}"
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(failPoint)
    }
    sql """ALTER TABLE row_binlog_ttl_publication SET ("binlog.ttl_seconds"="86400")"""
    qt_extension_published "SELECT ${ttl()}"
    qt_extension_retained "SELECT count(*) FROM row_binlog_ttl_publication@incr('incrementType'='DETAIL')"

    try {
        GetDebugPoint().enableDebugPointForAllFEs(failPoint, [value: "1"])
        test {
            sql """ALTER TABLE row_binlog_ttl_publication SET ("binlog.ttl_seconds"="1")"""
            exception "ROW binlog configuration update failed after 1 partitions"
        }
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(failPoint)
    }
    sql """ALTER TABLE row_binlog_ttl_publication SET ("disable_auto_compaction"="false")"""
    def waitForCleanup = { long expectedCount ->
        Awaitility.await().atMost(120, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until {
            (sql("SELECT count(*) FROM binlog('table'='row_binlog_ttl_publication')")[0][0] as long) == expectedCount
        }
        if (isCloudMode()) {
            // Compaction preserves the version; refresh other BEs that still cache old rowsets.
            for (def backend : sql_return_maparray("SHOW BACKENDS")) {
                def url = "http://${backend.Host}:${backend.HttpPort}/api/compaction_score?sync_meta=true&top_n=0"
                def (code, out, err) = curl("GET", url)
                assertEquals(0, code)
                assertTrue(parseJson(out) instanceof List)
            }
        }
    }
    waitForCleanup(1L)
    qt_partial_cleanup "SELECT count(*) FROM binlog('table'='row_binlog_ttl_publication')"
    qt_partial_cleanup_policy "SELECT ${ttl()}"
    sql """ALTER TABLE row_binlog_ttl_publication SET ("binlog.ttl_seconds"="1")"""
    waitForCleanup(0L)
    qt_retry_cleanup "SELECT count(*) FROM binlog('table'='row_binlog_ttl_publication')"
    order_qt_base_preserved "SELECT * FROM row_binlog_ttl_publication ORDER BY k"
}
