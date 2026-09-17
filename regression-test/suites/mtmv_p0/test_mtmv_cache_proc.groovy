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

suite("test_mtmv_cache_proc", "mtmv") {
    def dbName = "regression_test_mtmv_p0"
    def tableName = "t_test_mtmv_cache_proc_user"
    def mvName = "mtmv_cache_proc_mv"

    sql """drop materialized view if exists ${mvName}"""
    sql """drop table if exists ${tableName}"""

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            event_day DATE,
            id BIGINT,
            username VARCHAR(20)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES ('replication_num' = '1');
        """

    // SHOW PROC '/mtmv_cache' should list two children: stat and hot.
    def dirRows = sql """SHOW PROC '/mtmv_cache'"""
    assertEquals(2, dirRows.size())
    def dirNames = dirRows.collect { it[0] }
    assertTrue(dirNames.contains("stat"))
    assertTrue(dirNames.contains("hot"))

    // SHOW PROC '/mtmv_cache/stat' returns 6 KV rows.
    def statRows = sql """SHOW PROC '/mtmv_cache/stat'"""
    def statKeys = statRows.collect { it[0] }
    ["size", "hitCount", "missCount", "evictionCount", "loadFailureCount", "hitRate"].each {
        assertTrue(statKeys.contains(it), "stat missing key: ${it}")
    }

    // Create an MV and trigger cache fill via a rewrite-eligible query.
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT event_day, id, username FROM ${tableName};
    """
    def jobName = getJobName(dbName, mvName)
    sql """REFRESH MATERIALIZED VIEW ${mvName} AUTO"""
    waitingMTMVTaskFinished(jobName)
    // Query the base table so nereids checks the MV — fills the cache.
    sql """SELECT event_day, id, username FROM ${tableName}"""

    // hot proc: 5 columns, our MV should appear with its real DbName/MvName.
    def hotRows = sql """SHOW PROC '/mtmv_cache/hot'"""
    if (!hotRows.isEmpty()) {
        assertEquals(5, hotRows[0].size())
        def mvRow = hotRows.find { it[2] == mvName }
        if (mvRow != null) {
            assertEquals(dbName, mvRow[1])
            assertTrue(mvRow[3] == "Yes" || mvRow[3] == "No")
            assertTrue((mvRow[4] as Long) >= 0L)
        }
    }

    // mtmv_cache_hot_show_num caps the row count.
    def originalCap = sql """ADMIN SHOW FRONTEND CONFIG LIKE 'mtmv_cache_hot_show_num'"""
    def originalCapVal = originalCap.isEmpty() ? "500" : originalCap[0][1]
    try {
        sql """ADMIN SET FRONTEND CONFIG ('mtmv_cache_hot_show_num' = '1')"""
        def capped = sql """SHOW PROC '/mtmv_cache/hot'"""
        assertTrue(capped.size() <= 1, "hot row count should be <= 1 after capping, got ${capped.size()}")
    } finally {
        sql """ADMIN SET FRONTEND CONFIG ('mtmv_cache_hot_show_num' = '${originalCapVal}')"""
    }

    sql """drop materialized view if exists ${mvName}"""
    sql """drop table if exists ${tableName}"""
}
