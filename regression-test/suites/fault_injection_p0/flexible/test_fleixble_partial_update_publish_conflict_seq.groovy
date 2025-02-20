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

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

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
    sql """insert into ${tableName} values(1,1,1,1,1),(2,1,2,2,2),(3,1,3,3,3),(4,1,4,4,4),(14,1,14,14,14),(15,1,15,15,15),(16,1,16,16,16),(17,1,17,17,17),(27,1,27,27,27),(28,1,28,28,28),(29,1,29,29,29);"""
    qt_sql "select k,v1,v2,v3,v4,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName} order by k;"

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


    def enable_publish_spin_wait = { tokenName -> 
        if (isCloudMode()) {
            GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait", [token: "${tokenName}"])
        } else {
            GetDebugPoint().enableDebugPointForAllBEs("EnginePublishVersionTask::execute.enable_spin_wait", [token: "${tokenName}"])
        }
    }

    def disable_publish_spin_wait = {
        if (isCloudMode()) {
            GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
        } else {
            GetDebugPoint().disableDebugPointForAllBEs("EnginePublishVersionTask::execute.enable_spin_wait")
        }
    }

    def enable_block_in_publish = { passToken -> 
        if (isCloudMode()) {
            GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block", [pass_token: "${passToken}"])
        } else {
            GetDebugPoint().enableDebugPointForAllBEs("EnginePublishVersionTask::execute.block", [pass_token: "${passToken}"])
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
        enable_publish_spin_wait("t1")
        enable_block_in_publish("-1")

        // load 1
        def t1 = Thread.start {
            streamLoad {
                table "${tableName}"
                set 'format', 'json'
                set 'read_json_by_line', 'true'
                set 'strict_mode', 'false'
                set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
                file "test3.json"
                time 60000
            }
        }

        Thread.sleep(500)

        // load 2
        def t2 = Thread.start {
            streamLoad {
                table "${tableName}"
                set 'format', 'json'
                set 'read_json_by_line', 'true'
                set 'strict_mode', 'false'
                set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
                file "test4.json"
                time 60000
            }
        }

        // row(1)'s seq value: origin < load 2 < load 1
        // row(2)'s seq value: origin < load 1 < load 2
        // row(3)'s seq value: origin < load 1, load 2 without seq val
        // row(4)'s seq value: origin < load 2, load 1 without seq val

        // row(14)'s seq value: origin < load 2(with delete sign) < load 1
        // row(15)'s seq value: origin < load 1 < load 2(with delete sign)
        // row(16)'s seq value: origin < load 1, load 2 without seq val, with delete sign
        // row(17)'s seq value: origin < load 2(with delete sign), load 1 without seq val
        
        // row(27)'s seq value: origin < load 2 < load 1(with delete sign)
        // row(28)'s seq value: origin < load 1(with delete sign) < load 2
        // row(29)'s seq value: origin < load 1(with delete sign), load 2 without seq val
        // row(30)'s seq value: origin < load 2, load 1 without seq val, with delete sign

        Thread.sleep(500)

        disable_block_in_publish()
        t1.join()
        t2.join()

        qt_sql "select k,v1,v2,v3,v4,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName} order by k;"
        inspectRows "select k,v1,v2,v3,v4,__DORIS_SEQUENCE_COL__,__DORIS_VERSION_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__),__DORIS_DELETE_SIGN__ from ${tableName} order by k,__DORIS_VERSION_COL__,__DORIS_SEQUENCE_COL__;" 


        // ===========================================================================================
        // publish alignment read from rowsets which have multi-segments
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
        sql "truncate table ${tableName}"
        enable_publish_spin_wait("token1")
        enable_block_in_publish("-1")
        def t3 = Thread.start {
            sql "set insert_visible_timeout_ms=60000;"
            sql "sync;"
            sql "insert into ${tableName} values(1,10,1,1,1),(2,20,2,2,2),(3,30,3,3,3),(4,40,4,4,4);"
        }
        Thread.sleep(700)
        def t4 = Thread.start {
            sql "set enable_unique_key_partial_update=true;"
            sql "set insert_visible_timeout_ms=60000;"
            sql "set enable_insert_strict=false;"
            sql "sync;"
            sql "insert into ${tableName}(k,v1,v2,v3) values(1,99,99,99),(3,88,88,88);"
        }
        Thread.sleep(700)
        enable_publish_spin_wait("token2")
        def t5 = Thread.start {
            streamLoad {
                table "${tableName}"
                set 'format', 'json'
                set 'read_json_by_line', 'true'
                set 'strict_mode', 'false'
                set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
                file "test6.json"
                time 40000
            }
        }
        Thread.sleep(700)
        // let t3 and t4 publish
        enable_block_in_publish("token1")
        t3.join()
        t4.join()
        Thread.sleep(3000)
        qt_sql1 "select k,v1,v2,v3,v4 from ${tableName} order by k;"

        // let t5 publish
        // in publish phase, t5 will read from t4 which has multi segments
        enable_block_in_publish("token2")
        t5.join()
        qt_sql2 "select k,v1,v2,v3,v4 from ${tableName} order by k;"
        inspectRows "select k,v1,v2,v3,v4,__DORIS_SEQUENCE_COL__,__DORIS_VERSION_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__),__DORIS_DELETE_SIGN__ from ${tableName} order by k,__DORIS_VERSION_COL__,__DORIS_SEQUENCE_COL__;" 

    } catch(Exception e) {
        logger.info(e.getMessage())
        throw e
    } finally {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
    }

    // sql "DROP TABLE IF EXISTS ${tableName};"
}
