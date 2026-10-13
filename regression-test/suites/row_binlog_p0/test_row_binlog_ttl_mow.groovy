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

suite("test_row_binlog_ttl_mow", "nonConcurrent") {
    sql "SET enable_sql_cache = true"
    sql "SET enable_query_cache = true"
    sql "DROP TABLE IF EXISTS row_binlog_ttl_mow"
    sql """
        CREATE TABLE row_binlog_ttl_mow (k INT, v INT)
        UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true",
            "binlog.ttl_seconds" = "9223372036854775807",
            "disable_auto_compaction" = "true"
        )
    """
    sql "INSERT INTO row_binlog_ttl_mow VALUES (1, 10), (2, 20)"
    sql "SET show_hidden_columns = true"
    def snapshotTso = sql("SELECT MAX(__DORIS_COMMIT_TSO_COL__) FROM row_binlog_ttl_mow")[0][0]
    sql "SET show_hidden_columns = false"
    // Use the server clock and separate commits from the second-resolution TIME boundary.
    sleep(1100)
    def snapshotTime = sql("SELECT CAST(NOW() AS STRING)")[0][0]
    sleep(1100)
    sql "UPDATE row_binlog_ttl_mow SET v = 11 WHERE k = 1"
    sql "DELETE FROM row_binlog_ttl_mow WHERE k = 2"
    order_qt_base_before "SELECT * FROM row_binlog_ttl_mow ORDER BY k"
    order_qt_version_before "SELECT k, v FROM row_binlog_ttl_mow FOR VERSION AS OF ${snapshotTso} ORDER BY k"
    order_qt_time_before "SELECT k, v FROM row_binlog_ttl_mow FOR TIME AS OF '${snapshotTime}' ORDER BY k"
    qt_retained "SELECT count(*) FROM row_binlog_ttl_mow@incr('incrementType' = 'DETAIL')"
    qt_raw_before "SELECT count(*) FROM binlog('table' = 'row_binlog_ttl_mow')"

    sql """ALTER TABLE row_binlog_ttl_mow SET ("binlog.ttl_seconds" = "1")"""
    Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until {
        (sql("SELECT count(*) FROM row_binlog_ttl_mow@incr('incrementType' = 'DETAIL')")[0][0] as long) == 0L
    }
    qt_logically_expired "SELECT count(*) FROM row_binlog_ttl_mow@incr('incrementType' = 'DETAIL')"
    def checkExpiredSnapshots = {
        test {
            sql "SELECT k, v FROM row_binlog_ttl_mow FOR VERSION AS OF ${snapshotTso} ORDER BY k"
            exception "Row binlog offset has expired according to binlog.ttl_seconds"
        }
        test {
            sql "SELECT k, v FROM row_binlog_ttl_mow FOR TIME AS OF '${snapshotTime}' ORDER BY k"
            exception "Row binlog offset has expired according to binlog.ttl_seconds"
        }
    }
    checkExpiredSnapshots()
    qt_paused_retained "SELECT count(*) FROM binlog('table' = 'row_binlog_ttl_mow')"

    // Extending retention can expose logs still present while physical cleanup is paused.
    sql """ALTER TABLE row_binlog_ttl_mow SET ("binlog.ttl_seconds" = "9223372036854775807")"""
    order_qt_version_extended "SELECT k, v FROM row_binlog_ttl_mow FOR VERSION AS OF ${snapshotTso} ORDER BY k"
    qt_retention_extended "SELECT count(*) FROM row_binlog_ttl_mow@incr('incrementType' = 'DETAIL')"
    sql """ALTER TABLE row_binlog_ttl_mow SET ("binlog.ttl_seconds" = "1")"""
    sql """ALTER TABLE row_binlog_ttl_mow SET ("disable_auto_compaction" = "false")"""
    def waitForCleanup = {
        Awaitility.await().atMost(120, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until {
            (sql("SELECT count(*) FROM binlog('table' = 'row_binlog_ttl_mow')")[0][0] as long) == 0L
        }
        if (isCloudMode()) {
            // Compaction preserves the version; other BEs may still cache the original rowsets.
            for (def backend : sql_return_maparray("SHOW BACKENDS")) {
                def url = "http://${backend.Host}:${backend.HttpPort}/api/compaction_score?sync_meta=true&top_n=0"
                def (code, out, err) = curl("GET", url)
                assertEquals(0, code)
                assertTrue(parseJson(out) instanceof List)
            }
        }
    }
    waitForCleanup()
    checkExpiredSnapshots()
    qt_physically_reclaimed "SELECT count(*) FROM binlog('table' = 'row_binlog_ttl_mow')"
    order_qt_base_after "SELECT * FROM row_binlog_ttl_mow ORDER BY k"

    // Later rounds merge new logs with the empty version carrier from previous cleanup.
    for (int value : [12, 13]) {
        sql "UPDATE row_binlog_ttl_mow SET v = ${value} WHERE k = 1"
        waitForCleanup()
    }
    qt_repeatedly_reclaimed "SELECT count(*) FROM binlog('table' = 'row_binlog_ttl_mow')"
    order_qt_base_after_repeated_cleanup "SELECT * FROM row_binlog_ttl_mow ORDER BY k"

    // Reuse the same snapshot SQL across expiry with no intervening writes or DDL.
    sql """ALTER TABLE row_binlog_ttl_mow SET ("binlog.ttl_seconds" = "30")"""
    sql "INSERT INTO row_binlog_ttl_mow VALUES (3, 30)"
    sql "SET show_hidden_columns = true"
    def cachedTso = sql("SELECT MAX(__DORIS_COMMIT_TSO_COL__) FROM row_binlog_ttl_mow")[0][0]
    sql "SET show_hidden_columns = false"
    sql "UPDATE row_binlog_ttl_mow SET v = 14 WHERE k = 1"
    def snapshot = "SELECT k, v FROM row_binlog_ttl_mow FOR VERSION AS OF ${cachedTso} ORDER BY k"
    qt_snapshot_before_expiry snapshot
    qt_snapshot_repeated snapshot
    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until {
        (sql("SELECT count(*) FROM row_binlog_ttl_mow@incr('incrementType' = 'DETAIL')")[0][0] as long) == 0L
    }
    test {
        sql snapshot
        exception "Row binlog offset has expired according to binlog.ttl_seconds"
    }

}
