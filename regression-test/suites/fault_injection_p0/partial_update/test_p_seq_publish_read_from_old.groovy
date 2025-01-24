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

suite("test_p_seq_publish_read_from_old", "nonConcurrent") {

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    def tableName = "test_p_seq_publish_read_from_old"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
        `k` int(11) NULL, 
        `v1` BIGINT NULL,
        `v2` BIGINT NULL,
        `v3` BIGINT NOT NULL DEFAULT "9876",
        `v4` BIGINT NOT NULL DEFAULT "1234",
        `v5` BIGINT NULL
        ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "function_column.sequence_col" = "v1",
        "store_row_column" = "false"); """
    sql """insert into ${tableName} values(1,100,1,1,1,1),(2,100,2,2,2,2),(3,100,3,3,3,3),(4,100,4,4,4,4);"""
    qt_sql "select k,v1,v2,v3,v4,v5 from ${tableName} order by k;"

    def inspectRows = { sqlStr ->
        sql "set skip_delete_sign=true;"
        sql "set skip_delete_bitmap=true;"
        sql "sync"
        qt_inspect sqlStr
        sql "set skip_delete_sign=false;"
        sql "set skip_delete_bitmap=false;"
        sql "sync"
    }

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
        enable_publish_spin_wait()
        enable_block_in_publish()

        def t1 = Thread.start {
            sql "set enable_unique_key_partial_update=true;"
            sql "insert into ${tableName}(k,v1,__DORIS_DELETE_SIGN__) values(1,100,1),(2,200,1),(3,50,1);"
        }

        Thread.sleep(1000)
        def t2 = Thread.start {
            sql "set enable_unique_key_partial_update=true;"
            sql "insert into ${tableName}(k,__DORIS_DELETE_SIGN__) values(4,1);" 
        }

        Thread.sleep(1000)
        def t3 = Thread.start {
            sql "set enable_unique_key_partial_update=true;"
            sql "insert into ${tableName}(k,v2,v3) values(1,987,77777),(2,987,77777),(3,987,77777),(4,987,77777);" 
        }

        disable_block_in_publish()
        t1.join()
        t2.join()
        t3.join()
        Thread.sleep(1500)
        sql "sync;"
        qt_sql "select k,v1,v2,v3,v4,v5 from ${tableName} order by k;"
        inspectRows "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,__DORIS_VERSION_COL__,__DORIS_DELETE_SIGN__ from ${tableName} order by k,__DORIS_VERSION_COL__,__DORIS_SEQUENCE_COL__;"

    } catch(Exception e) {
        logger.info(e.getMessage())
        throw e
    } finally {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
}
