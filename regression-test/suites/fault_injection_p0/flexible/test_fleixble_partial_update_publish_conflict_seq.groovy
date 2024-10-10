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

suite("test_flexible_partial_update_publish_conflict_seq", "nonConcurrent") {

    def tableName = "test_flexible_partial_update_publish_conflict_seq"
    sql """ DROP TABLE IF EXISTS ${tableName} force;"""
    sql """ CREATE TABLE ${tableName} (
        `k` int(11) NULL, 
        `v1` BIGINT NULL,
        `v2` BIGINT NULL,
        `v3` BIGINT NULL,
        `v4` BIGINT NULL,
        ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "enable_unique_key_skip_bitmap_column" = "true",
        "function_column.sequence_col" = "v1",
        "store_row_column" = "false"); """
    def show_res = sql "show create table ${tableName}"
    assertTrue(show_res.toString().contains('"enable_unique_key_skip_bitmap_column" = "true"'))
    sql """insert into ${tableName} values(1,1,1,1,1),(2,2,2,2,2),(4,4,4,4,4),(5,5,5,5,5),(6,6,6,6,6);"""
    order_qt_sql "select k,v1,v2,v3,v4,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"

    def beNodes = sql_return_maparray("show backends;")
    def tabletStat = sql_return_maparray("show tablets from ${tableName};").get(0)
    def tabletBackendId = tabletStat.BackendId
    def tabletId = tabletStat.TabletId
    def tabletBackend;
    for (def be : beNodes) {
        if (be.BackendId == tabletBackendId) {
            tabletBackend = be
            break;
        }
    }
    logger.info("tablet ${tabletId} on backend ${tabletBackend.Host} with backendId=${tabletBackend.BackendId}");


    def enable_publish_spin_wait = {
        if (isCloudMode()) {
            GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
        } else {
            GetDebugPoint().enableDebugPointForAllBEs("EnginePublishVersionTask::execute.enable_spin_wait")
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

    def inspectRows = { sqlStr ->
        sql "set skip_delete_sign=true;"
        sql "set skip_delete_bitmap=true;"
        sql "sync"
        qt_inspect sqlStr
        sql "set skip_delete_sign=false;"
        sql "set skip_delete_bitmap=false;"
        sql "sync"
    }

    try {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()

        // block the flexible partial update in publish phase
        enable_publish_spin_wait()
        enable_block_in_publish()
        def t1 = Thread.start {
            streamLoad {
                table "${tableName}"
                set 'format', 'json'
                set 'read_json_by_line', 'true'
                set 'strict_mode', 'false'
                set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
                file "test3.json"
                time 20000
            }
        }

        Thread.sleep(500)

        def t2 = Thread.start {
            streamLoad {
                table "${tableName}"
                set 'format', 'json'
                set 'read_json_by_line', 'true'
                set 'strict_mode', 'false'
                set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
                file "test4.json"
                time 20000
            }
        }

        Thread.sleep(500)

        disable_block_in_publish()
        t1.join()
        t2.join()

        qt_sql "select k,v1,v2,v3,v4,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName} order by k;"
        inspectRows "select k,v1,v2,v3,v4,__DORIS_SEQUENCE_COL__,__DORIS_VERSION_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName} order by k,__DORIS_VERSION_COL__,__DORIS_SEQUENCE_COL__;" 
    } catch(Exception e) {
        logger.info(e.getMessage())
        throw e
    } finally {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
    }

    // sql "DROP TABLE IF EXISTS ${tableName};"
}
