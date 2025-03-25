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

suite("test_partial_update_publish_seq", "nonConcurrent") {

    def enable_block_in_publish = {
        if (isCloudMode()) {
            GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
            GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")
        } else {
            GetDebugPoint().enableDebugPointForAllBEs("EnginePublishVersionTask::execute.enable_spin_wait")
            GetDebugPoint().enableDebugPointForAllBEs("EnginePublishVersionTask::execute.block")
        }
    }

    def disable_block_in_publish = {
        if (isCloudMode()) {
            GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
            GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")
        } else {
            GetDebugPoint().disableDebugPointForAllBEs("EnginePublishVersionTask::execute.enable_spin_wait")
            GetDebugPoint().disableDebugPointForAllBEs("EnginePublishVersionTask::execute.block")
        }
    }

    def inspect_rows = { sqlStr ->
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

        def table1 = "test_partial_update_publish_seq_map"
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
                "function_column.sequence_col" = "c1",
                "replication_num" = "1"); """

        sql "insert into ${table1} values(1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3);"
        sql "sync;"
        qt_seq_map_0 "select * from ${table1} order by k1;"


        // with seq map val, >/=/< conflicting seq val
        enable_block_in_publish()
        def t1 = Thread.start {
            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict=false;"
            sql "sync;"
            sql "insert into ${table1}(k1,c1,c2) values(1,10,99),(2,10,99),(3,10,99);"
        }
        Thread.sleep(500)
        def t2 = Thread.start {
            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict=false;"
            sql "sync;"
            sql "insert into ${table1}(k1,c1,c3) values(1,20,88),(2,10,88),(3,5,88);"
        }
        Thread.sleep(1000)
        disable_block_in_publish()
        t1.join()
        t2.join()
        qt_seq_map_1 "select * from ${table1} order by k1;"
        inspect_rows "select *,__DORIS_DELETE_SIGN__,__DORIS_SEQUENCE_COL__,__DORIS_VERSION_COL__ from ${table1} order by k1,__DORIS_VERSION_COL__;"

        // without seq map val, the filled seq val >/=/< conflicting seq val
        enable_block_in_publish()
        t1 = Thread.start {
            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict=false;"
            sql "sync;"
            sql "insert into ${table1}(k1,c1,c2) values(1,9,77),(2,10,77),(3,50,77);"
        }
        Thread.sleep(500)
        t2 = Thread.start {
            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict=false;"
            sql "sync;"
            sql "insert into ${table1}(k1,c4) values(1,33),(2,33),(3,33);"
        }
        Thread.sleep(1000)
        disable_block_in_publish()
        t1.join()
        t2.join()
        qt_seq_map_2 "select * from ${table1} order by k1;"
        inspect_rows "select *,__DORIS_DELETE_SIGN__,__DORIS_SEQUENCE_COL__,__DORIS_VERSION_COL__ from ${table1} order by k1,__DORIS_VERSION_COL__;"

        // with delete sign and seq col val, >/=/< conflicting seq val
        enable_block_in_publish()
        t1 = Thread.start {
            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict=false;"
            sql "sync;"
            sql "insert into ${table1}(k1,c1,c2) values(1,80,66),(2,100,66),(3,120,66);"
        }
        Thread.sleep(500)
        t2 = Thread.start {
            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict=false;"
            sql "sync;"
            sql "insert into ${table1}(k1,c1,__DORIS_DELETE_SIGN__) values(1,100,1),(2,100,1),(3,100,1);"
        }
        Thread.sleep(1000)
        disable_block_in_publish()
        t1.join()
        t2.join()
        qt_seq_map_3 "select * from ${table1} order by k1;"
        inspect_rows "select *,__DORIS_DELETE_SIGN__,__DORIS_SEQUENCE_COL__,__DORIS_VERSION_COL__ from ${table1} order by k1,__DORIS_VERSION_COL__;"


        sql "truncate table ${table1};"
        sql "insert into ${table1} values(1,10,1,1,1),(2,10,2,2,2),(3,10,3,3,3);"
        sql "sync;"
        // with delete sign and without seq col val, >/=/< conflicting seq val
        enable_block_in_publish()
        t1 = Thread.start {
            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict=false;"
            sql "sync;"
            sql "insert into ${table1}(k1,c1,c2) values(1,20,55),(2,100,55),(3,120,55);"
        }
        Thread.sleep(500)
        t2 = Thread.start {
            sql "set enable_unique_key_partial_update=true;"
            sql "set enable_insert_strict=false;"
            sql "sync;"
            sql "insert into ${table1}(k1,c4,__DORIS_DELETE_SIGN__) values(1,100,1),(2,100,1),(3,100,1);"
        }
        Thread.sleep(1000)
        disable_block_in_publish()
        t1.join()
        t2.join()
        qt_seq_map_4 "select * from ${table1} order by k1;"
        inspect_rows "select *,__DORIS_DELETE_SIGN__,__DORIS_SEQUENCE_COL__,__DORIS_VERSION_COL__ from ${table1} order by k1,__DORIS_VERSION_COL__;"


    } catch(Exception e) {
        logger.info(e.getMessage())
        throw e
    } finally {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
}
