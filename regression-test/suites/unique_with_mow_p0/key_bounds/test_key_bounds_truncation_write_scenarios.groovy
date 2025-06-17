
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

import com.google.common.collect.Maps
import org.apache.commons.lang.RandomStringUtils
import org.apache.doris.regression.util.Http
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite("test_key_bounds_truncation_write_scenarios", "nonConcurrent") {

    def tableName = "test_key_bounds_truncation_write_scenarios"
    sql """ DROP TABLE IF EXISTS ${tableName} force;"""
    sql """ CREATE TABLE ${tableName} (
        `k` varchar(65533) NOT NULL,
        `v1` int,
        v2 int,
        v3 int not null default '99')
        UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES("replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true"); """

    def printCompactionStatus = { tblName ->
        def tablets = sql_return_maparray("show tablets from ${tblName};")
        for (def tabletStat : tablets) {
            def compactionStatusUrl = tabletStat.CompactionStatus
            def jsonMeta = Http.GET(compactionStatusUrl, true, false)
            logger.info("${jsonMeta.rowsets}")
        }
    }

    def checkKeyBounds = { int length, int version = -1 ->
        def tablets = sql_return_maparray("show tablets from ${tableName};")
        for (def tabletStat : tablets) {
            def metaUrl = tabletStat.MetaUrl
            def tabletId = tabletStat.TabletId
            logger.info("begin curl ${metaUrl}")
            def jsonMeta = Http.GET(metaUrl, true, false)
            for (def meta : jsonMeta.rs_metas) {
                int end_version = meta.end_version
                if (version != -1 && version != end_version) { 
                    continue
                }
                logger.info("version=[${meta.start_version}-${meta.end_version}], meta.segments_key_bounds_truncated=${meta.segments_key_bounds_truncated}")
                if (end_version >= 2 && meta.num_rows > 0) {
                    assert meta.segments_key_bounds_truncated
                }
                for (def bounds : meta.segments_key_bounds) {
                    String min_key = bounds.min_key
                    String max_key = bounds.max_key
                    // only check length here
                    logger.info("tablet_id=${tabletId}, version=[${meta.start_version}-${meta.end_version}]\nmin_key=${min_key}, size=${min_key.size()}\nmax_key=${max_key}, size=${max_key.size()}")
                    assert min_key.size() <= length
                    assert max_key.size() <= length
                }
            }
        }
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


    Random random = new Random()
    def randomString = { -> 
        int count = random.nextInt(200) + 20
        return RandomStringUtils.randomAlphabetic(count);
    }

    def customBeConfig = [
        segments_key_bounds_truncation_threshold : 20
    ]

    setBeConfigTemporary(customBeConfig) {

        // 1. load
        logger.info("============= load ==============")
        int m = 10, n = 20
        for (int i = 0; i < m; i++) {
            String sqlStr = "insert into ${tableName} values"
            for (int j = 1; j <= n; j++) {
                sqlStr += """("${randomString()}", 1, 1, 1)"""
                if (j < n) {
                    sqlStr += ","
                }
            }
            sqlStr += ";"
            sql sqlStr
        }
        printCompactionStatus(tableName)
        checkKeyBounds(20)


        // 2. partial update with publish conflict, will generate new segment and update rowset in publish phase
        logger.info("============= partial update ==============")
        set_be_param("segments_key_bounds_truncation_threshold", 16)
        Thread.sleep(2000)
        try {
            GetDebugPoint().clearDebugPointsForAllFEs()
            GetDebugPoint().clearDebugPointsForAllBEs()

            enable_publish_spin_wait()
            enable_block_in_publish()

            String values = ""
            for (int i = 1; i <= m; i++) {
                values += """("${randomString()}", 2)"""
                if (i < m) {
                    values += ","
                }
            }

            Thread.sleep(200)

            def t1 = Thread.start {
                sql "set enable_insert_strict = false;"
                sql "set enable_unique_key_partial_update = true;"
                sql "sync;"
                sql """ insert into ${tableName}(k,v1) values ${values};"""
            }

            def t2 = Thread.start {
                sql "set enable_insert_strict = false;"
                sql "set enable_unique_key_partial_update = true;"
                sql "sync;"
                sql """ insert into ${tableName}(k,v2) values ${values};"""
            }

            Thread.sleep(1500)
            disable_publish_spin_wait()
            disable_block_in_publish()

            t1.join()
            t2.join()

            sql "set enable_unique_key_partial_update = false;"
            sql "set enable_insert_strict = true;"
            sql "sync;"

            Thread.sleep(1000)
            printCompactionStatus(tableName)
            checkKeyBounds(16, 12)
            checkKeyBounds(16, 13)

        } finally {
            disable_publish_spin_wait()
            disable_block_in_publish()
        }


        // 3. schema change
        def doSchemaChange = { cmd ->
            sql cmd
            waitForSchemaChangeDone {
                sql """SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY createtime DESC LIMIT 1"""
                time 20000
            }
        }

        // direct schema change
        logger.info("============= schema change 1 ==============")
        set_be_param("segments_key_bounds_truncation_threshold", 12)
        Thread.sleep(1000)
        doSchemaChange " ALTER table ${tableName} modify column v2 varchar(100)"
        printCompactionStatus(tableName)
        checkKeyBounds(12)
        sql "insert into ${tableName} select * from ${tableName};"
        def res1 = sql "select k from ${tableName} group by k having count(*)>1;"
        assert res1.size() == 0

        // linked schema change
        logger.info("============= schema change 2 ==============")
        set_be_param("segments_key_bounds_truncation_threshold", 20)
        Thread.sleep(1000)
        doSchemaChange " ALTER table ${tableName} modify column v3 int null default '99'"
        printCompactionStatus(tableName)
        // will use previous rowsets' segment key bounds
        // so the length is still 12
        checkKeyBounds(12)
        sql "insert into ${tableName} select * from ${tableName};"
        def res2 = sql "select k from ${tableName} group by k having count(*)>1;"
        assert res2.size() == 0

        // sort schema change
        logger.info("============= schema change 3 ==============")
        set_be_param("segments_key_bounds_truncation_threshold", 15)
        Thread.sleep(2000)
        doSchemaChange " ALTER table ${tableName} add column k2 int key after k;"
        doSchemaChange " ALTER table ${tableName} order by (k2,k,v1,v2,v3);"
        printCompactionStatus(tableName)
        checkKeyBounds(15)
        sql "insert into ${tableName} select * from ${tableName};"
        def res3 = sql "select k from ${tableName} group by k having count(*)>1;"
        assert res3.size() == 0

        // 4. compaction
        logger.info("============= compaction ==============")
        set_be_param("segments_key_bounds_truncation_threshold", 8)
        Thread.sleep(2000)
        trigger_and_wait_compaction(tableName, "full")
        checkKeyBounds(8)

        qt_sql "select count(*) from ${tableName};"
    }
}
