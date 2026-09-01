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

suite("test_mow_cumulative_compaction_multi_output_segments", "nonConcurrent") {
    if (!isCloudMode()) {
        logger.info("skip test_mow_cumulative_compaction_multi_output_segments in non-cloud mode")
        return
    }

    def backendIdToHost = [:]
    def backendIdToHttpPort = [:]
    getBackendIpHttpPort(backendIdToHost, backendIdToHttpPort)

    def configNames = [
            "compaction_batch_size",
            "doris_scanner_row_bytes",
            "enable_rowid_conversion_correctness_check",
            "enable_vertical_compaction",
            "inverted_index_compaction_enable",
            "vertical_compaction_max_segment_size"
    ]
    def originalConfigs = [:]

    def readBeConfig = { String backendId, String configName ->
        def host = backendIdToHost[backendId]
        def port = backendIdToHttpPort[backendId]
        def (code, out, err) = curl(
                "GET", "http://${host}:${port}/api/show_config?conf_item=${configName}")
        assertEquals(0, code)
        def configs = parseJson(out)
        assertEquals(1, configs.size())
        assertEquals(configName, configs[0][0])
        return configs[0][2].toString()
    }

    def updateBeConfig = { String configName, String value ->
        backendIdToHost.keySet().each { String backendId ->
            def host = backendIdToHost[backendId]
            def port = backendIdToHttpPort[backendId]
            def (code, out, err) = curl(
                    "POST", "http://${host}:${port}/api/update_config?${configName}=${value}")
            assertEquals(0, code)
            assertTrue(out.contains("OK"), "failed to set ${configName}=${value}: ${out}, ${err}")
        }
    }

    backendIdToHost.keySet().each { String backendId ->
        originalConfigs[backendId] = [:]
        configNames.each { String configName ->
            originalConfigs[backendId][configName] = readBeConfig(backendId, configName)
        }
    }

    def resetBeConfigs = {
        originalConfigs.each { String backendId, Map configs ->
            def host = backendIdToHost[backendId]
            def port = backendIdToHttpPort[backendId]
            configs.each { String configName, String value ->
                def (code, out, err) = curl(
                        "POST", "http://${host}:${port}/api/update_config?${configName}=${value}")
                assertEquals(0, code)
                assertTrue(out.contains("OK"),
                        "failed to reset ${configName}=${value}: ${out}, ${err}")
            }
        }
    }

    def showTablet = { def backend, String tabletId ->
        def (code, out, err) =
                be_show_tablet_status(backend.Host, backend.HttpPort, tabletId)
        logger.info("Show tablet status: code=${code}, out=${out}, err=${err}")
        assertEquals(0, code)
        return parseJson(out.trim())
    }

    def findRowset = { def tabletStatus, int startVersion, int endVersion ->
        def rowset =
                tabletStatus.rowsets.find { it.startsWith("[${startVersion}-${endVersion}] ") }
        assertNotNull(rowset,
                "cannot find rowset [${startVersion}-${endVersion}]: ${tabletStatus.rowsets}")
        return rowset
    }

    def parseRowset = { String rowset ->
        def matcher = rowset =~
                /\[[0-9]+-[0-9]+\]\s+([0-9]+)\s+DATA\s+([A-Z_]+)\s+([0-9a-f]+)/
        assertTrue(matcher.find(), "unexpected rowset format: ${rowset}")
        def segmentIds = []
        def segmentIdsMatcher = rowset =~ /\s\[([0-9]+(?:,[0-9]+)*)\]$/
        if (segmentIdsMatcher.find()) {
            segmentIds = segmentIdsMatcher.group(1).split(",").collect { it.toInteger() }
        }
        return [
                segmentNum: matcher.group(1).toInteger(),
                overlap: matcher.group(2),
                rowsetId: matcher.group(3),
                segmentIds: segmentIds
        ]
    }

    def waitForCompaction = { def backend, String tabletId ->
        long timeoutMs = 120000
        long deadline = System.currentTimeMillis() + timeoutMs
        def lastStatus = null
        while (System.currentTimeMillis() < deadline) {
            def (code, out, err) =
                    be_get_compaction_status(backend.Host, backend.HttpPort, tabletId)
            logger.info("Get compaction status: code=${code}, out=${out}, err=${err}")
            assertEquals(0, code)
            lastStatus = parseJson(out.trim())
            assertEquals("success", lastStatus.status.toLowerCase())
            if (!lastStatus.run_status) {
                return
            }
            Thread.sleep(1000)
        }
        assertTrue(false,
                "compaction timeout: tablet=${tabletId}, timeoutMs=${timeoutMs}, last=${lastStatus}")
    }

    def waitForRowset = { backend, tabletId, startVersion, endVersion ->
        long timeoutMs = 30000
        long deadline = System.currentTimeMillis() + timeoutMs
        def lastStatus = null
        while (System.currentTimeMillis() < deadline) {
            lastStatus = showTablet(backend, tabletId)
            if (lastStatus.rowsets.any {
                it.startsWith("[${startVersion}-${endVersion}] ")
            }) {
                return lastStatus
            }
            Thread.sleep(200)
        }
        assertTrue(false,
                "rowset [${startVersion}-${endVersion}] is not visible: " +
                        "tablet=${tabletId}, timeoutMs=${timeoutMs}, last=${lastStatus}")
    }

    def loadRows = { String tableName, int startKey, int endKey, int valueBase ->
        def batchTags = [
                100000: "batchone",
                200000: "batchtwo",
                300000: "batchthree",
                400000: "batchfour",
                500000: "batchfive"
        ]
        def batchTag = batchTags[valueBase]
        assertNotNull(batchTag)
        StringBuilder content = new StringBuilder()
        (startKey..endKey).each { int key ->
            String suffix = key.toString().padLeft(32, "0")
            String payload =
                    "${batchTag} payload ${suffix} ${(key * 17L).toString().padLeft(32, "0")}"
            content.append("${key},${valueBase + key},${payload}\n")
        }
        streamLoad {
            table tableName
            set "column_separator", ","
            inputStream new ByteArrayInputStream(content.toString().getBytes())
            time 120000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(endKey - startKey + 1, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }
        sql "sync"
    }

    def readRows = { String tableName ->
        return sql("""
            SELECT k, v
            FROM ${tableName}
            WHERE k IN (1, 16384, 16385, 32768, 32769, 49152, 49153, 65536, 65537, 81920)
            ORDER BY k
        """)
    }

    def readRowsByIndex = { String tableName ->
        def batchTags = ["batchone", "batchtwo", "batchthree", "batchfour", "batchfive"]
        return batchTags.collect { String batchTag ->
            def result = sql """
                SELECT /*+ SET_VAR(enable_match_without_inverted_index = false) */ COUNT(*)
                FROM ${tableName}
                WHERE payload MATCH '${batchTag}'
            """
            return result[0][0]
        }
    }

    def getLocalDeleteBitmap = { def backend, String tabletId ->
        def (code, out, err) = curl(
                "GET",
                "http://${backend.Host}:${backend.HttpPort}" +
                        "/api/delete_bitmap/count_local?verbose=true&tablet_id=${tabletId}")
        logger.info("Get local delete bitmap: code=${code}, out=${out}, err=${err}")
        assertEquals(0, code)
        return parseJson(out.trim())
    }

    def getDeleteBitmapAtVersion = { def deleteBitmap, String rowsetId, int version ->
        def segmentIds = []
        long cardinality = 0
        deleteBitmap.delete_bitmap.each { String key, bitmapVersions ->
            if (!key.contains("rowset: ${rowsetId},")) {
                return
            }
            def segmentMatcher = key =~ /segment:\s+([0-9]+)/
            assertTrue(segmentMatcher.find(), "unexpected delete bitmap key: ${key}")
            bitmapVersions.each { String bitmapVersion ->
                def versionMatcher = bitmapVersion =~ /v:\s+([0-9]+),\s+c:\s+([0-9]+)/
                assertTrue(versionMatcher.find(),
                        "unexpected delete bitmap value: ${bitmapVersion}")
                if (versionMatcher.group(1).toInteger() == version) {
                    segmentIds.add(segmentMatcher.group(1).toInteger())
                    cardinality += versionMatcher.group(2).toLong()
                }
            }
        }
        return [segmentIds: segmentIds.unique(), cardinality: cardinality]
    }

    GetDebugPoint().clearDebugPointsForAllBEs()
    try {
        updateBeConfig("compaction_batch_size", "512")
        updateBeConfig("doris_scanner_row_bytes", "1")
        updateBeConfig("enable_rowid_conversion_correctness_check", "true")
        updateBeConfig("inverted_index_compaction_enable", "true")
        updateBeConfig("vertical_compaction_max_segment_size", "8192")
        updateBeConfig("enable_cloud_random_segment_id", "true")

        GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")
        GetDebugPoint().enableDebugPointForAllBEs(
                "CloudCompactionMixin.construct_output_rowset_writer.random_start_segment_id")
        GetDebugPoint().enableDebugPointForAllBEs(
                "CloudCompactionMixin.construct_output_rowset_writer.max_rows_per_segment",
                [max_rows_per_segment: "1024"])

        def runCompaction = {
                String tableName, boolean withInvertedIndex, boolean enableVerticalCompaction ->
            updateBeConfig("enable_vertical_compaction", enableVerticalCompaction.toString())
            def indexDefinition = withInvertedIndex
                    ? """,
                        INDEX idx_payload (payload) USING INVERTED
                        PROPERTIES("parser" = "english")
                    """
                    : ""
            sql "DROP TABLE IF EXISTS ${tableName}"
            sql """
                CREATE TABLE ${tableName} (
                    k INT NOT NULL,
                    v BIGINT NOT NULL,
                    payload VARCHAR(128) NOT NULL
                    ${indexDefinition}
                )
                UNIQUE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "disable_auto_compaction" = "true",
                    "enable_unique_key_merge_on_write" = "true",
                    "inverted_index_storage_format" = "V2"
                )
            """

            loadRows(tableName, 1, 32768, 100000)
            loadRows(tableName, 16385, 49152, 200000)
            loadRows(tableName, 32769, 65536, 300000)
            // Keep this rowset outside the compaction range. Its updates create delete bitmap
            // entries on the [2-4] output rowset, exercising row-id conversion to physical
            // segment ids.
            loadRows(tableName, 49153, 81920, 400000)

            def countBefore = sql "SELECT COUNT(*) FROM ${tableName}"
            assertEquals(81920, countBefore[0][0])
            def rowsBefore = readRows(tableName)
            def indexRowsBefore = withInvertedIndex ? readRowsByIndex(tableName) : null

            def tablets = sql_return_maparray "SHOW TABLETS FROM ${tableName}"
            assertEquals(1, tablets.size())
            def tabletId = tablets[0].TabletId.toString()
            def backendId = tablets[0].BackendId.toString()
            def backends = sql_return_maparray "SHOW BACKENDS"
            def backend = backends.find { it.BackendId.toString() == backendId }
            assertNotNull(backend)

            def before = showTablet(backend, tabletId)
            [2, 3, 4].each { int version ->
                def inputRowset = findRowset(before, version, version)
                def inputInfo = parseRowset(inputRowset)
                assertTrue(inputInfo.segmentNum > 1, inputRowset)
            }
            def untouchedRowset = findRowset(before, 5, 5)
            def untouchedInfo = parseRowset(untouchedRowset)
            def untouchedSegmentIds = untouchedInfo.segmentIds.isEmpty()
                    ? (0..<untouchedInfo.segmentNum).toList()
                    : untouchedInfo.segmentIds

            GetDebugPoint().enableDebugPointForAllBEs(
                    "CloudSizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets",
                    [tablet_id: tabletId, start_version: "2", end_version: "4"])
            def (code, out, err) =
                    be_run_cumulative_compaction(backend.Host, backend.HttpPort, tabletId)
            logger.info("Run compaction: code=${code}, out=${out}, err=${err}")
            assertEquals(0, code)
            def compactResult = parseJson(out.trim())
            assertEquals("success", compactResult.status.toLowerCase())
            waitForCompaction(backend, tabletId)

            def after = showTablet(backend, tabletId)
            def outputRowset = findRowset(after, 2, 4)
            def outputInfo = parseRowset(outputRowset)
            assertEquals("NONOVERLAPPING", outputInfo.overlap)
            assertTrue(outputInfo.segmentNum > 1, outputRowset)
            assertEquals(outputInfo.segmentNum, outputInfo.segmentIds.size())
            assertTrue(outputInfo.segmentIds[0] > 0, outputRowset)
            for (int i = 1; i < outputInfo.segmentIds.size(); ++i) {
                assertEquals(outputInfo.segmentIds[i - 1] + 1, outputInfo.segmentIds[i])
            }
            assertEquals(untouchedRowset, findRowset(after, 5, 5))

            def countAfter = sql "SELECT COUNT(*) FROM ${tableName}"
            assertEquals(countBefore, countAfter)
            assertEquals(rowsBefore, readRows(tableName))
            if (withInvertedIndex) {
                assertEquals(indexRowsBefore, readRowsByIndex(tableName))
            }

            def deleteBitmap = getLocalDeleteBitmap(backend, tabletId)
            assertNotNull(deleteBitmap.delete_bitmap)
            def outputDeleteBitmapKeys = deleteBitmap.delete_bitmap.keySet().findAll {
                it.contains("rowset: ${outputInfo.rowsetId},")
            }
            assertFalse(outputDeleteBitmapKeys.isEmpty(),
                    "missing delete bitmap for output rowset ${outputInfo.rowsetId}: ${deleteBitmap}")
            outputDeleteBitmapKeys.each { String key ->
                def matcher = key =~ /segment:\s+([0-9]+)/
                assertTrue(matcher.find(), "unexpected delete bitmap key: ${key}")
                assertTrue(outputInfo.segmentIds.contains(matcher.group(1).toInteger()),
                        "delete bitmap references a segment outside " +
                                "${outputInfo.segmentIds}: ${key}")
            }

            // Version 6 overlaps live rows in both the compacted [2-4] rowset and the untouched
            // version 5 rowset. Verify publish calculates new delete bitmaps against both.
            loadRows(tableName, 32769, 65536, 500000)

            // Read version 6 first so the query path synchronizes the newly visible rowset from MS
            // into the BE tablet cache used by the compaction status endpoint.
            def countAfterUpdate = sql "SELECT COUNT(*) FROM ${tableName}"
            assertEquals(countBefore, countAfterUpdate)

            def afterUpdate = waitForRowset(backend, tabletId, 6, 6)
            assertEquals(outputRowset, findRowset(afterUpdate, 2, 4))
            assertEquals(untouchedRowset, findRowset(afterUpdate, 5, 5))
            findRowset(afterUpdate, 6, 6)

            def expectedRowsAfterUpdate = rowsBefore.collect { row ->
                int key = (row[0] as Number).intValue()
                if (key >= 32769 && key <= 65536) {
                    return [row[0], 500000L + key]
                }
                return row
            }
            assertEquals(expectedRowsAfterUpdate, readRows(tableName))
            if (withInvertedIndex) {
                assertEquals([16384L, 16384L, 0L, 16384L, 32768L],
                        readRowsByIndex(tableName))
            }

            def deleteBitmapAfterUpdate = getLocalDeleteBitmap(backend, tabletId)
            def outputBitmapAtVersion6 =
                    getDeleteBitmapAtVersion(deleteBitmapAfterUpdate, outputInfo.rowsetId, 6)
            assertEquals(16384L, outputBitmapAtVersion6.cardinality)
            assertFalse(outputBitmapAtVersion6.segmentIds.isEmpty(),
                    "missing version 6 delete bitmap for compacted rowset " +
                            "${outputInfo.rowsetId}: ${deleteBitmapAfterUpdate}")
            outputBitmapAtVersion6.segmentIds.each { int segmentId ->
                assertTrue(outputInfo.segmentIds.contains(segmentId),
                        "version 6 delete bitmap references a segment outside " +
                                "${outputInfo.segmentIds}: ${segmentId}")
            }

            def untouchedBitmapAtVersion6 =
                    getDeleteBitmapAtVersion(deleteBitmapAfterUpdate, untouchedInfo.rowsetId, 6)
            assertEquals(16384L, untouchedBitmapAtVersion6.cardinality)
            assertFalse(untouchedBitmapAtVersion6.segmentIds.isEmpty(),
                    "missing version 6 delete bitmap for rowset " +
                            "${untouchedInfo.rowsetId}: ${deleteBitmapAfterUpdate}")
            untouchedBitmapAtVersion6.segmentIds.each { int segmentId ->
                assertTrue(untouchedSegmentIds.contains(segmentId),
                        "version 6 delete bitmap references a segment outside " +
                                "${untouchedSegmentIds}: ${segmentId}")
            }
        }

        // Cover the full cross-product of compaction writer and inverted index modes.
        runCompaction("test_mow_cumulative_compaction_multi_output_segments", false, true)
        runCompaction("test_mow_cumu_compact_multi_segments_non_vertical", false, false)
        runCompaction("test_mow_cumulative_compaction_multi_output_segments_index", true, true)
        // runCompaction("test_mow_cumu_compact_multi_segments_index_non_vertical", true, false)
    } finally {
        GetDebugPoint().clearDebugPointsForAllBEs()
        resetBeConfigs()
    }
}
