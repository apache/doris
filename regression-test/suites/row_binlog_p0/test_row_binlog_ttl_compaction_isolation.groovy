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

suite("test_row_binlog_ttl_compaction_isolation", "nonConcurrent") {
    sql "DROP TABLE IF EXISTS row_binlog_ttl_isolation_paused"
    sql "DROP TABLE IF EXISTS row_binlog_ttl_isolation_active"
    sql """
        CREATE TABLE row_binlog_ttl_isolation_paused (isolation_key INT, payload INT)
        DUPLICATE KEY(isolation_key) DISTRIBUTED BY HASH(isolation_key) BUCKETS 1
        PROPERTIES ("replication_num"="1", "binlog.enable"="true", "binlog.format"="ROW",
                    "binlog.ttl_seconds"="2", "disable_auto_compaction"="true")
    """
    sql "CREATE TABLE row_binlog_ttl_isolation_active LIKE row_binlog_ttl_isolation_paused"
    sql "INSERT INTO row_binlog_ttl_isolation_paused VALUES (1,10)"
    sql "INSERT INTO row_binlog_ttl_isolation_active VALUES (1,10)"

    // Cloud tablets can be assigned to different BEs. Load both schemas on every BE so this
    // exercises shared-schema isolation regardless of the placement chosen by the FE.
    def tablets = sql_return_maparray("SHOW TABLETS FROM row_binlog_ttl_isolation_paused")
    tablets.addAll(sql_return_maparray("SHOW TABLETS FROM row_binlog_ttl_isolation_active"))
    if (isCloudMode()) {
        for (def backend : sql_return_maparray("SHOW BACKENDS")) {
            for (def tablet : tablets) {
                def url = "http://${backend.Host}:${backend.HttpPort}/api/meta/header/${tablet.TabletId}?byte_to_base64=true"
                def (code, out, err) = curl("GET", url)
                assertEquals(0, code)
                assertEquals(tablet.TabletId as long, parseJson(out).tablet_id as long)
            }
        }
    }

    def refreshCachedCloudRowsets = {
        if (isCloudMode()) {
            // Compaction keeps the same visible version. A query on another BE may reuse
            // its old Rowsets; refresh those caches before asserting physical reclamation.
            for (def backend : sql_return_maparray("SHOW BACKENDS")) {
                def url = "http://${backend.Host}:${backend.HttpPort}/api/compaction_score?sync_meta=true&top_n=0"
                def (code, out, err) = curl("GET", url)
                assertEquals(0, code)
                assertTrue(parseJson(out) instanceof List)
            }
        }
    }

    sql """ALTER TABLE row_binlog_ttl_isolation_active SET ("disable_auto_compaction"="false")"""
    Awaitility.await().atMost(120, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until {
        (sql("SELECT count(*) FROM binlog('table'='row_binlog_ttl_isolation_active')")[0][0] as long) == 0L
    }
    // Observe several scanner rounds after the first table has actually reclaimed its log.
    sleep(5000)
    refreshCachedCloudRowsets()
    qt_active_reclaimed "SELECT count(*) FROM binlog('table'='row_binlog_ttl_isolation_active')"
    qt_paused_retained "SELECT count(*) FROM binlog('table'='row_binlog_ttl_isolation_paused')"
    order_qt_paused_base "SELECT * FROM row_binlog_ttl_isolation_paused ORDER BY isolation_key"
    order_qt_active_base "SELECT * FROM row_binlog_ttl_isolation_active ORDER BY isolation_key"

    sql """ALTER TABLE row_binlog_ttl_isolation_paused SET ("disable_auto_compaction"="false")"""
    Awaitility.await().atMost(120, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until {
        (sql("SELECT count(*) FROM binlog('table'='row_binlog_ttl_isolation_paused')")[0][0] as long) == 0L
    }
    refreshCachedCloudRowsets()
    qt_paused_reclaimed "SELECT count(*) FROM binlog('table'='row_binlog_ttl_isolation_paused')"
}
