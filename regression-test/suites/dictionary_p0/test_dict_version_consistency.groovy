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

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

suite("test_dict_version_consistency", "nonConcurrent") {
    sql "drop database if exists test_dict_version_consistency"
    sql "create database test_dict_version_consistency"
    sql "use test_dict_version_consistency"

    sql """
        create table src_dict(
            k varchar(100) not null,
            v varchar(100) not null
        )
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        properties("replication_num" = "1");
    """
    sql "insert into src_dict values ('1', 'value1'), ('2', 'value2'), ('3', 'value3')"
    sql """
        create dictionary dict1 using src_dict
        (k KEY, v VALUE) LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """
    waitAllDictionariesReady()

    def queryDict = { String key ->
        sql "select dict_get('test_dict_version_consistency.dict1', 'v', '${key}')"
    }

    // get current dict version via show dictionaries
    // columns: DictionaryId(0) DictionaryName(1) BaseTableName(2) Version(3) Status(4) ...
    def getDictVersion = {
        def rows = sql "show dictionaries"
        for (def row : rows) {
            if (row[1] == "dict1") return row[3] as int
        }
        return -1
    }

    // wait until dict version advances past baseline (FE increaseVersion done, commit still pending)
    def waitVersionAdvanced = { int baseline, long timeoutMs ->
        long deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            def v = getDictVersion()
            if (v > baseline) return v
            sleep(200)
        }
        throw new AssertionError("dict version did not advance past ${baseline} within ${timeoutMs}ms")
    }

    // baseline
    def baselineVersion = getDictVersion()
    def result = queryDict("1")
    assertEquals("value1", result[0][0])

    // ============ Test 1: staging fallback ============
    // Scenario: FE increaseVersion() done (version N+1 visible) but commit RPC to BE not yet sent
    //           (blocked at afterIncJournal debug point).
    //           Query with version N+1 should succeed via _refreshing_dict_map fallback.
    logger.info("=== Test 1: staging fallback ===")
    GetDebugPoint().enableDebugPointForAllFEs("DictionaryManager.afterIncJournal")
    try {
        def executor = Executors.newSingleThreadExecutor()
        def refreshFuture = executor.submit({
            sql "use test_dict_version_consistency"
            sql "refresh dictionary dict1"
        })

        // wait until FE has increased version (blocked at afterIncJournal, commit not done yet)
        def advancedVersion = waitVersionAdvanced(baselineVersion, 10000)
        logger.info("FE version advanced to ${advancedVersion}, commit still pending")

        // query should succeed via staging fallback
        result = queryDict("1")
        assertEquals("value1", result[0][0])
        logger.info("query succeeded via staging fallback")

        GetDebugPoint().disableDebugPointForAllFEs("DictionaryManager.afterIncJournal")
        refreshFuture.get(30, TimeUnit.SECONDS)
        executor.shutdown()
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs("DictionaryManager.afterIncJournal")
    }
    logger.info("=== Test 1 passed ===")

    // ============ Test 2: multi-version retention ============
    // Scenario: query holds old version N (dict_get_delay sleeps 5s at BE get()),
    //           refresh commits new version N+1 during the sleep.
    //           Verifies _dict_id_to_versioned_map retains old version N (max_versions=2).
    logger.info("=== Test 2: multi-version retention ===")
    update_all_be_config("dictionary_max_versions", "2")
    GetDebugPoint().enableDebugPointForAllBEs("dict_get_delay", [sleep_sec: 5])
    try {
        def baselineVersion2 = getDictVersion()
        // submit query in background: FE plan uses baselineVersion2, BE sleeps 5s at get()
        def executor = Executors.newSingleThreadExecutor()
        def queryResult = new java.util.concurrent.atomic.AtomicReference<List>()
        def queryFuture = executor.submit({
            sql "use test_dict_version_consistency"
            queryResult.set(sql "select dict_get('test_dict_version_consistency.dict1', 'v', '1')")
        })

        // wait for BE to enter dict_get_delay (query is now sleeping with old version)
        sleep(2000)

        // refresh commits new version while query holds old version
        sql "refresh dictionary dict1"
        def newVersion = waitVersionAdvanced(baselineVersion2, 10000)
        logger.info("refresh committed version ${newVersion}, query still holding ${baselineVersion2}")

        // query should complete successfully using old version (multi-version retention)
        queryFuture.get(30, TimeUnit.SECONDS)
        executor.shutdown()
        result = queryResult.get()
        assertEquals("value1", result[0][0])
        logger.info("query succeeded with old version (multi-version retention)")
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("dict_get_delay")
        update_all_be_config("dictionary_max_versions", "1")
    }
    logger.info("=== Test 2 passed ===")
}
