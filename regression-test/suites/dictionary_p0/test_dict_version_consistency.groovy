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
        UNIQUE KEY(`k`)
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
    // Scenario: refresh commits version N+1 with new data, then query forced to use
    //           old version N via FE debug point. Verifies _dict_id_to_versioned_map
    //           retains old version N (max_versions=2) and returns old data.
    logger.info("=== Test 2: multi-version retention ===")
    def prevMaxVersions = "2"
    update_all_be_config("dictionary_max_versions", "2")
    try {
        def baselineVersion2 = getDictVersion()

        // update src data so N+1 returns a distinguishable value (insert overwrites unique key)
        sql "insert into src_dict values ('1', 'value1_v2')"
        sql "sync"
        sql "refresh dictionary dict1"
        def newVersion = waitVersionAdvanced(baselineVersion2, 10000)
        logger.info("refresh committed version ${newVersion}, baseline was ${baselineVersion2}")

        // query with forced old version N should return old data (multi-version retention)
        GetDebugPoint().enableDebugPointForAllFEs("ExpressionTranslator.dict_get_version",
                [version_id: baselineVersion2.toString()])
        try {
            result = queryDict("1")
            assertEquals("value1", result[0][0])
            logger.info("query with forced version ${baselineVersion2} returned old data")
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs("ExpressionTranslator.dict_get_version")
        }

        // query with current version N+1 should return new data
        result = queryDict("1")
        assertEquals("value1_v2", result[0][0])
        logger.info("query with current version ${newVersion} returned new data")
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs("ExpressionTranslator.dict_get_version")
        update_all_be_config("dictionary_max_versions", prevMaxVersions)
    }
    logger.info("=== Test 2 passed ===")
}
