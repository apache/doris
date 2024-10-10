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

import org.junit.Assert
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite("test_delete_publish_skip_read", "nonConcurrent") {
    def table1 = "test_delete_publish_skip_read"
    sql "DROP TABLE IF EXISTS ${table1} FORCE;"
    sql """ CREATE TABLE IF NOT EXISTS ${table1} (
            `k1` int NOT NULL,
            `c1` int,
            `c2` int,
            `c3` int,
            `c4` int
            )UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "enable_mow_light_delete" = "false",
            "disable_auto_compaction" = "true",
            "enable_unique_key_skip_bitmap_column" = "true",
            "replication_num" = "1"); """

    sql "insert into ${table1} values(1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3);"
    sql "sync;"
    order_qt_sql "select * from ${table1};"

    def enable_publish_spin_wait = {
        if (isCloudMode()) {
            GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
        } else {
            GetDebugPoint().enableDebugPointForAllBEs("EnginePublishVersionTask::execute.enable_spin_wait")
        }
    }

    def disable_publish_spin_wait = {
        if (isCloudMode()) {
            GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
        } else {
            GetDebugPoint().disableDebugPointForAllBEs("EnginePublishVersionTask::execute.enable_spin_wait")
        }
    }

    def enable_block_in_publish = {
        if (isCloudMode()) {
            GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")
        } else {
            GetDebugPoint().enableDebugPointForAllBEs("EnginePublishVersionTask::execute.block")
        }
    }

    def disable_block_in_publish = {
        if (isCloudMode()) {
            GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")
        } else {
            GetDebugPoint().disableDebugPointForAllBEs("EnginePublishVersionTask::execute.block")
        }
    }

    try {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()

        // block the partial update in publish phase
        enable_publish_spin_wait()
        enable_block_in_publish()
        def t1 = Thread.start {
            streamLoad {
                table "${table1}"
                set 'format', 'json'
                set 'read_json_by_line', 'true'
                set 'strict_mode', 'false'
                set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
                file 'skip_delete.json'
                time 10000
            }
        }

        Thread.sleep(500)

        def t2 = Thread.start {
            sql "insert into ${table1}(k1,__DORIS_DELETE_SIGN__) values(2,1);"
        }

        // let the partial update load publish
        disable_block_in_publish()
        t1.join()
        t2.join()

        order_qt_sql "select k1,c1,c2,c3,c4,__DORIS_DELETE_SIGN__,__DORIS_VERSION_COL__ from ${table1};"

        sql "set skip_delete_sign=true;"
        sql "set skip_storage_engine_merge=true;"
        sql "set skip_delete_bitmap=true;"
        sql "sync;"

        order_qt_sql "select k1,c1,c2,c3,c4,__DORIS_DELETE_SIGN__,__DORIS_VERSION_COL__ from ${table1};"
    } catch(Exception e) {
        logger.info(e.getMessage())
        throw e
    } finally {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
    sql "DROP TABLE IF EXISTS ${table1};"
}
