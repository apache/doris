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

import org.apache.doris.regression.suite.ClusterOptions

// In cloud mode, schema change with sorting writes intermediate rowsets to the root of the file
// cache path. They must be removed whether the schema change succeeds or fails, and the ones left
// by a crashed BE process must be removed when BE restarts.
suite("test_cloud_sc_sorting_tmp_rowset_cleanup", "docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)
    options.enableDebugPoints()

    def tableName = "test_cloud_sc_sorting_tmp_rowset_cleanup"
    // <rowset_id>_<segment_id>.dat or .idx, rowset_id is a 48 hex string with version "02"
    def rowsetFilePattern = ~/^02[0-9a-f]{46}_.*\.(dat|idx)$/

    def rowsetFilesInCacheRoot = { String cacheRoot ->
        def files = new File(cacheRoot).listFiles()
                .findAll { it.isFile() && it.name ==~ rowsetFilePattern }
                .collect { it.name }
        logger.info("rowset files in ${cacheRoot}: ${files}")
        return files
    }

    def waitRollupJob = { String expectedState ->
        def state = null
        awaitUntil(300) {
            def jobs = sql_return_maparray """
                SHOW ALTER TABLE ROLLUP WHERE TableName = "${tableName}" ORDER BY CreateTime DESC LIMIT 1
            """
            state = jobs[0].State
            logger.info("rollup job: ${jobs[0]}")
            return state == "FINISHED" || state == "CANCELLED"
        }
        assertEquals(expectedState, state)
    }

    def countLogLines = { String logFile, String keyword ->
        return new File(logFile).readLines().count { it.contains(keyword) }
    }

    docker(options) {
        def be = cluster.getBeByIndex(1)
        def cacheRoot = be.getBasePath() + "/storage/file_cache"

        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE ${tableName} (
                k1 INT NOT NULL,
                k2 INT NOT NULL,
                v INT NOT NULL
            )
            DUPLICATE KEY(k1, k2)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ("disable_auto_compaction" = "true")
        """
        for (int i = 0; i < 5; i++) {
            sql """
                INSERT INTO ${tableName}
                SELECT number % 100, 100 - number % 100, number FROM numbers("number" = "1000")
            """
        }
        assertEquals(0, rowsetFilesInCacheRoot(cacheRoot).size())

        // 1. The rollup reorders the keys, so the schema change sorts the data and generates
        // intermediate rowsets, which must be removed after the job finishes.
        def sortingLogsBefore = countLogLines(be.getLogFilePath(), "sc_sorting: 1")
        sql """ ALTER TABLE ${tableName} ADD ROLLUP r_sorted(k2, k1, v) """
        waitRollupJob("FINISHED")
        assertTrue(countLogLines(be.getLogFilePath(), "sc_sorting: 1") > sortingLogsBefore)
        assertEquals(0, rowsetFilesInCacheRoot(cacheRoot).size())
        def rows = sql """ SELECT count(*) FROM ${tableName} INDEX r_sorted """
        assertEquals(5000L, rows[0][0] as long)

        // 2. Intermediate rowsets must also be removed when the schema change fails.
        try {
            GetDebugPoint().enableDebugPointForAllBEs(
                    "VBaseSchemaChangeWithSorting._inner_process.before_external_sorting")
            sql """ ALTER TABLE ${tableName} ADD ROLLUP r_failed(k2, v) """
            waitRollupJob("CANCELLED")
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(
                    "VBaseSchemaChangeWithSorting._inner_process.before_external_sorting")
        }
        assertEquals(0, rowsetFilesInCacheRoot(cacheRoot).size())

        // 3. Rowset files left by a previous BE process, e.g. BE crashed during schema change,
        // are removed when BE starts. Other files in the cache root are kept.
        cluster.stopBackends(1)
        def random = new Random()
        def randomHex = { int len -> (1..len).collect { Integer.toHexString(random.nextInt(16)) }.join() }
        def staleRowsetId = "02" + randomHex(46)
        def staleFiles = ["${staleRowsetId}_0.dat", "${staleRowsetId}_0.idx"]
        def keptFile = "not_a_rowset_file.dat"
        (staleFiles + [keptFile]).each { new File("${cacheRoot}/${it}").text = "data" }
        assertEquals(staleFiles.size(), rowsetFilesInCacheRoot(cacheRoot).size())

        cluster.startBackends(1)
        awaitUntil(60) {
            def backends = sql_return_maparray "SHOW BACKENDS"
            return backends[0].Alive.toString() == "true"
        }
        assertEquals(0, rowsetFilesInCacheRoot(cacheRoot).size())
        assertTrue(new File("${cacheRoot}/${keptFile}").exists())
        rows = sql """ SELECT count(*) FROM ${tableName} INDEX r_sorted """
        assertEquals(5000L, rows[0][0] as long)
    }
}
