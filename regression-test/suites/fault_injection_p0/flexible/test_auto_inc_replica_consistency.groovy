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

suite("test_auto_inc_replica_consistency", "nonConcurrent") {

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    def tableName = "test_auto_inc_replica_consistency"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
        `k` int(11) NULL, 
        `v1` BIGINT NULL,
        `v2` BIGINT NULL,
        `id` BIGINT NOT NULL AUTO_INCREMENT(20)
        ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "enable_unique_key_skip_bitmap_column" = "true",
        "store_row_column" = "false"); """

    def beNodes = sql_return_maparray("show backends;")
    if (beNodes.size() > 1) {
        logger.info("skip to run the case when there are more than 1 BE.")
        return
    }

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


        enable_publish_spin_wait("t1")
        enable_block_in_publish("-1")


        Thread.sleep(1000)
        def t1 = Thread.start {
            String load1 = 
                    """{"k":1,"v1":100}"""
            streamLoad {
                table "${tableName}"
                set 'format', 'json'
                set 'read_json_by_line', 'true'
                set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
                inputStream new ByteArrayInputStream(load1.getBytes())
                time 60000
            }
        }
        Thread.sleep(1000)


        def t2 = Thread.start {
            String load2 = 
                    """{"k":1,"v2":200}"""
            streamLoad {
                table "${tableName}"
                set 'format', 'json'
                set 'read_json_by_line', 'true'
                set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
                inputStream new ByteArrayInputStream(load2.getBytes())
                time 60000
            }
        }
        Thread.sleep(1000)


        disable_publish_spin_wait()
        disable_block_in_publish()
        t1.join()
        t2.join()
        Thread.sleep(1000)

        sql "insert into ${tableName}(k,v1,v2) values(2,2,2);"

        qt_sql "select k,v1,v2,id from ${tableName} order by k;"
        sql "set skip_delete_bitmap=true;"
        sql "sync;"
        qt_sql "select k,v1,v2,id,__DORIS_VERSION_COL__ from ${tableName} order by k,__DORIS_VERSION_COL__;"

    } catch(Exception e) {
        logger.info(e.getMessage())
        throw e
    } finally {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
}
