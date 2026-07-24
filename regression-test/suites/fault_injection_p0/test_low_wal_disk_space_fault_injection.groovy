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

suite("test_low_wal_disk_space_fault_injection","nonConcurrent") {
    def tableName = "wal_test"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """    
        CREATE TABLE IF NOT EXISTS ${tableName} (    
            `k` int ,    
            `v` int ,    
        ) engine=olap    
        UNIQUE KEY(k)    
        DISTRIBUTED BY HASH(`k`)     
        BUCKETS 32     
        properties("replication_num" = "1")    
        """
    GetDebugPoint().clearDebugPointsForAllBEs()
    sql """ set group_commit = async_mode; """

    // case 1: Simulate WAL space shortage at Debug Point to test downgrade mechanism
    logger.info("=== case 1: Simulate WAL space shortage at Debug Point to test downgrade mechanism ===")
    try {
        GetDebugPoint().enableDebugPointForAllBEs("LoadBlockQueue.has_enough_wal_disk_space.low_space")
        def results = Collections.synchronizedList([])
        def threads = []
        for (int i = 0; i < 5; i++) {
            threads.add(Thread.startDaemon("stream-load-$i") {
                streamLoad {
                    table "${tableName}"
                    set 'column_separator', ','
                    set 'compress_type', 'GZ'
                    set 'format', 'csv'
                    set 'group_commit', 'async_mode'
                    unset 'label'
                    file 'test_low_wal_disk_space_fault_injection.csv.gz'
                    time 60000
                    check { result, exception, startTime, endTime ->
                        if (exception != null) {
                            throw exception
                        }
                        def json = parseJson(result)
                        results.add(json)
                    }
                }
            })
        }
        // wait for all stream load threads done
        threads.each { it.join(60000) }
        assertTrue(results.size() == 5, "there should be 5 stream load results")
        results.each { json ->
            assertEquals("Success", json.Status, "Stream load show success")
            assertTrue(json.GroupCommit, "GroupCommit should be true")
        }
        def txnIds = results.collect { it.TxnId }
        assertTrue(txnIds.every { it == txnIds[0] }, "all txnId show be the same")
        logger.info("case1 verification successful: correctly downgraded to sync mode when WAL space is insufficient, stream load successful")
    } catch (Exception e) {
        logger.error("case1 failure: " + e.getMessage())
        throw e
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("LoadBlockQueue.has_enough_wal_disk_space.low_space")
    }

    // case 2: Set a minimal WAL limit to test HTTP layer admission checks
    logger.info("=== case 2: Set a minimal WAL limit to test HTTP layer admission checks ===")
    try {
        GetDebugPoint().enableDebugPointForAllBEs("StreamLoad.load_size_smaller_than_wal_limit.always_false")
        try {
            streamLoad {
                table "${tableName}"
                set 'column_separator', ','
                set 'compress_type', 'GZ'
                set 'format', 'csv'
                set 'group_commit', 'async_mode'
                unset 'label'
                file 'test_low_wal_disk_space_fault_injection.csv.gz'
                time 60000
            }
            assertTrue(false, "An exception for insufficient WAL space should be thrown")
        } catch (Exception e) {
            logger.info("catch exception message: " + e.getMessage())
            assertTrue(e.getMessage().contains("no space for group commit") || e.getMessage().contains("WAL"), "abnormal information should include content related to WAL space")
        }
        logger.info("case2 verification successful: when WAL restriction is too small, requests are correctly rejected at the HTTP layer")
    } catch (Exception e) {
        logger.error("failure in case2: " + e.getMessage())
        throw e
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("StreamLoad.load_size_smaller_than_wal_limit.always_false")
    }
}
