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

suite("test_flexible_partial_update_publish_conflict", "nonConcurrent") {

    def tableName = "test_flexible_partial_update_publish_conflict"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
        `k` int(11) NULL, 
        `v1` BIGINT NULL,
        `v2` BIGINT NULL DEFAULT "9876",
        `v3` BIGINT NOT NULL,
        `v4` BIGINT NOT NULL DEFAULT "1234",
        `v5` BIGINT NULL
        ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "enable_unique_key_skip_bitmap_column" = "true",
        "store_row_column" = "false"); """
    def show_res = sql "show create table ${tableName}"
    assertTrue(show_res.toString().contains('"enable_unique_key_skip_bitmap_column" = "true"'))
    sql """insert into ${tableName} select number, number, number, number, number, number from numbers("number" = "6"); """
    order_qt_sql "select k,v1,v2,v3,v4,v5,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"

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

    try {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()

        // block the partial update in publish phase
        enable_publish_spin_wait()
        enable_block_in_publish()
        def t1 = Thread.start {
            streamLoad {
                table "${tableName}"
                set 'format', 'json'
                set 'read_json_by_line', 'true'
                set 'strict_mode', 'false'
                set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
                file "test1.json"
                time 40000
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
                file "test2.json"
                time 40000
            }
        }

        Thread.sleep(500)

        disable_publish_spin_wait()
        disable_block_in_publish()
        t1.join()
        t2.join()

        order_qt_sql "select k,v1,v2,v3,v4,v5,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"


        // ==================================================================================================
        sql "truncate table ${tableName}"
        enable_publish_spin_wait("token1")
        enable_block_in_publish("-1")
        def t3 = Thread.start {
            sql "set insert_visible_timeout_ms=60000;"
            sql "sync;"
            sql "insert into ${tableName} values(1,1,1,1,1,1),(2,2,2,2,2,2);"
        }
        Thread.sleep(700)
        def t4 = Thread.start {
            sql "set enable_unique_key_partial_update=true;"
            sql "set insert_visible_timeout_ms=60000;"
            sql "set enable_insert_strict=false;"
            sql "sync;"
            sql "insert into ${tableName}(k,v1,v2,v3) values(1,99,99,99);"
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
                file "test5.json"
                time 40000
            }
        }
        Thread.sleep(700)
        // let t3 and t4 publish
        enable_block_in_publish("token1")
        t3.join()
        t4.join()
        Thread.sleep(1000)
        qt_sql1 "select k,v1,v2,v3,v4,v5 from ${tableName} order by k;"
        // let t5 publish
        enable_block_in_publish("token2")
        t5.join()
        qt_sql2 "select k,v1,v2,v3,v4,v5 from ${tableName} order by k;"

    } catch(Exception e) {
        logger.info(e.getMessage())
        throw e
    } finally {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
}
