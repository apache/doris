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

suite("test_variant_external_meta_concurrent", "nonConcurrent") {
    def set_be_config = { key, value ->
        String backend_id;
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        backend_id = backendId_to_backendIP.keySet()[0]
        def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), 
                                                backendId_to_backendHttpPort.get(backend_id), 
                                                key, value)
        logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
    }

    // Test 1: Concurrent reads of external meta
    sql "DROP TABLE IF EXISTS test_concurrent_read"
    sql """
        CREATE TABLE test_concurrent_read (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 4
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3");
    """
    
    // Insert data with many subcolumns to stress test external meta loading
    for (int i = 0; i < 20; i++) {
        def fields = []
        for (int j = 0; j < 50; j++) {
            fields.add("\"field_${j}\": ${i * 100 + j}")
        }
        def json = "{" + fields.join(", ") + "}"
        sql """insert into test_concurrent_read values (${i}, '${json}')"""
    }
    
    // Run concurrent queries that will trigger external meta loading
    def threads = []
    def results = [].asSynchronized()
    
    for (int t = 0; t < 5; t++) {
        def threadId = t
        threads.add(Thread.start {
            try {
                // Each thread queries different fields
                def fieldIdx = threadId * 10
                def result = sql """
                    select count(*) from test_concurrent_read 
                    where cast(v['field_${fieldIdx}'] as int) is not null
                """
                results.add([threadId: threadId, success: true, result: result])
            } catch (Exception e) {
                results.add([threadId: threadId, success: false, error: e.message])
                logger.error("Thread ${threadId} failed: ${e.message}")
            }
        })
    }
    
    threads.each { it.join() }
    
    // Verify all threads succeeded
    def failures = results.findAll { !it.success }
    if (!failures.isEmpty()) {
        logger.error("Concurrent read test failed: ${failures}")
        throw new Exception("Concurrent read test had failures: ${failures}")
    }
    
    qt_concurrent_1 "select count(*) from test_concurrent_read"
    qt_concurrent_2 "select count(distinct k) from test_concurrent_read"

    // Test 2: Multiple tables with external meta
    for (int tableIdx = 0; tableIdx < 5; tableIdx++) {
        def tableName = "test_multi_table_${tableIdx}"
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (
                k bigint,
                v variant
            )
            DUPLICATE KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3");
        """
        
        sql """insert into ${tableName} values (1, '{"table_${tableIdx}": ${tableIdx}}')"""
        sql """insert into ${tableName} values (2, '{"table_${tableIdx}": ${tableIdx * 10}}')"""
    }
    
    // Query all tables concurrently
    threads = []
    results = [].asSynchronized()
    
    for (int t = 0; t < 5; t++) {
        def tableIdx = t
        def tableName = "test_multi_table_${tableIdx}"
        threads.add(Thread.start {
            try {
                def result = sql """
                    select k, v['table_${tableIdx}'] from ${tableName} order by k
                """
                results.add([tableIdx: tableIdx, success: true, rowCount: result.size()])
            } catch (Exception e) {
                results.add([tableIdx: tableIdx, success: false, error: e.message])
                logger.error("Table ${tableIdx} query failed: ${e.message}")
            }
        })
    }
    
    threads.each { it.join() }
    
    failures = results.findAll { !it.success }
    if (!failures.isEmpty()) {
        throw new Exception("Multi-table concurrent test had failures: ${failures}")
    }

    // Test 3: Concurrent writes and reads
    sql "DROP TABLE IF EXISTS test_concurrent_write_read"
    sql """
        CREATE TABLE test_concurrent_write_read (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 4
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3");
    """
    
    // Initial data
    for (int i = 0; i < 10; i++) {
        sql """insert into test_concurrent_write_read values (${i}, '{"initial": ${i}}')"""
    }
    
    threads = []
    results = [].asSynchronized()
    
    // Start reader threads
    for (int t = 0; t < 3; t++) {
        def threadId = t
        threads.add(Thread.start {
            try {
                for (int i = 0; i < 5; i++) {
                    def result = sql """select count(*) from test_concurrent_write_read"""
                    Thread.sleep(100)  // Small delay between reads
                }
                results.add([type: 'reader', threadId: threadId, success: true])
            } catch (Exception e) {
                results.add([type: 'reader', threadId: threadId, success: false, error: e.message])
            }
        })
    }
    
    // Start writer threads
    for (int t = 0; t < 2; t++) {
        def threadId = t
        threads.add(Thread.start {
            try {
                for (int i = 0; i < 3; i++) {
                    def k = 100 + threadId * 10 + i
                    sql """insert into test_concurrent_write_read values (${k}, '{"writer": ${threadId}}')"""
                    Thread.sleep(150)
                }
                results.add([type: 'writer', threadId: threadId, success: true])
            } catch (Exception e) {
                results.add([type: 'writer', threadId: threadId, success: false, error: e.message])
            }
        })
    }
    
    threads.each { it.join() }
    
    failures = results.findAll { !it.success }
    if (!failures.isEmpty()) {
        throw new Exception("Concurrent write-read test had failures: ${failures}")
    }
    
    qt_write_read_1 "select count(*) from test_concurrent_write_read"
    qt_write_read_2 "select count(*) from test_concurrent_write_read where cast(v['initial'] as int) is not null"
    qt_write_read_3 "select count(*) from test_concurrent_write_read where cast(v['writer'] as int) is not null"

    // Test 4: Stress test with frequent compaction triggers
    sql "DROP TABLE IF EXISTS test_compaction_stress"
    sql """
        CREATE TABLE test_compaction_stress (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 2
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3");
    """
    
    // Insert multiple small segments
    for (int i = 0; i < 15; i++) {
        sql """insert into test_compaction_stress values (${i}, '{"segment": ${i}, "value": ${i * 100}}')"""
    }
    
    // Trigger compaction while querying
    threads = []
    results = [].asSynchronized()
    
    // Query thread
    threads.add(Thread.start {
        try {
            for (int i = 0; i < 10; i++) {
                def result = sql """select count(*) from test_compaction_stress"""
                Thread.sleep(200)
            }
            results.add([type: 'query', success: true])
        } catch (Exception e) {
            results.add([type: 'query', success: false, error: e.message])
        }
    })
    
    // Give query thread a head start
    Thread.sleep(100)
    
    // Compaction thread
    threads.add(Thread.start {
        try {
            trigger_and_wait_compaction("test_compaction_stress", "full")
            results.add([type: 'compaction', success: true])
        } catch (Exception e) {
            results.add([type: 'compaction', success: false, error: e.message])
        }
    })
    
    threads.each { it.join() }
    
    failures = results.findAll { !it.success }
    if (!failures.isEmpty()) {
        throw new Exception("Compaction stress test had failures: ${failures}")
    }
    
    qt_stress_1 "select count(*) from test_compaction_stress"
    qt_stress_2 "select k, v['segment'] from test_compaction_stress order by k limit 5"

    // Test 5: Cache behavior - repeated queries should hit cache
    sql "DROP TABLE IF EXISTS test_cache_behavior"
    sql """
        CREATE TABLE test_cache_behavior (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true", "storage_format" = "V3");
    """
    
    // Insert data with many subcolumns
    def fields = []
    for (int i = 0; i < 100; i++) {
        fields.add("\"cache_field_${i}\": ${i}")
    }
    def json = "{" + fields.join(", ") + "}"
    
    for (int i = 0; i < 5; i++) {
        sql """insert into test_cache_behavior values (${i}, '${json}')"""
    }
    
    // First query - should load external meta
    def startTime = System.currentTimeMillis()
    sql """select k, v['cache_field_0'], v['cache_field_50'], v['cache_field_99'] 
           from test_cache_behavior order by k"""
    def firstQueryTime = System.currentTimeMillis() - startTime
    
    // Subsequent queries - should use cached meta (should be faster)
    startTime = System.currentTimeMillis()
    for (int i = 0; i < 5; i++) {
        sql """select k, v['cache_field_0'], v['cache_field_50'], v['cache_field_99'] 
               from test_cache_behavior order by k"""
    }
    def cachedQueriesTime = System.currentTimeMillis() - startTime
    def avgCachedTime = cachedQueriesTime / 5
    
    logger.info("First query time: ${firstQueryTime}ms, Avg cached query time: ${avgCachedTime}ms")
    
    qt_cache_1 "select count(*) from test_cache_behavior"

    // Cleanup
    sql "DROP TABLE IF EXISTS test_concurrent_read"
    for (int i = 0; i < 5; i++) {
        sql "DROP TABLE IF EXISTS test_multi_table_${i}"
    }
    sql "DROP TABLE IF EXISTS test_concurrent_write_read"
    sql "DROP TABLE IF EXISTS test_compaction_stress"
    sql "DROP TABLE IF EXISTS test_cache_behavior"
    
}


