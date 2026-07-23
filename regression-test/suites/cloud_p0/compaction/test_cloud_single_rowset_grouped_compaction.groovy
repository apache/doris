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

import java.util.Base64

suite("test_cloud_single_rowset_grouped_compaction", "docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)
    options.enableDebugPoints()
    int initialInputSegmentsPerGroup = 2
    long compactionTimeoutMs = 90000L
    options.beConfigs += [
        "doris_scanner_row_bytes=1",
        "enable_cloud_single_rowset_compaction=true",
        "cloud_single_rowset_compaction_min_segments=2",
        "cloud_single_rowset_compaction_segment_group_size=${initialInputSegmentsPerGroup}",
        "cumulative_compaction_min_deltas=2",
        "enable_aggregate_non_mow_key_bounds=false",
        "disable_auto_compaction=true",
        "enable_java_support=false"
    ]

    docker(options) {
        def metaService = cluster.getAllMetaservices().get(0)
        def metaServiceEndpoint = "${metaService.host}:${metaService.httpPort}"

        def getRowsetMeta = { tabletId, int version ->
            def rowsetMeta = null
            getSegmentFilesFromMs(metaServiceEndpoint, tabletId, version) { responseCode, body ->
                assertEquals(200, responseCode)
                rowsetMeta = parseJson(body)
            }
            assertNotNull(rowsetMeta)
            return rowsetMeta
        }

        def compareEncodedKeys = { String leftBase64, String rightBase64 ->
            byte[] left = Base64.getDecoder().decode(leftBase64)
            byte[] right = Base64.getDecoder().decode(rightBase64)
            int commonLength = Math.min(left.length, right.length)
            for (int i = 0; i < commonLength; ++i) {
                int leftByte = Byte.toUnsignedInt(left[i])
                int rightByte = Byte.toUnsignedInt(right[i])
                if (leftByte != rightByte) {
                    return leftByte <=> rightByte
                }
            }
            return left.length <=> right.length
        }

        def logCaseSection = { String description ->
            logger.info("========================================================================")
            logger.info(description)
            logger.info("========================================================================")
        }

        def showTablet = { tableName, beHost, bePort, tabletId ->
            sql "SELECT COUNT(*) FROM ${tableName}"
            def (code, out, err) = be_show_tablet_status(beHost, bePort, tabletId)
            logger.info("Show tablet status: code=${code}, out=${out}, err=${err}")
            assertEquals(0, code)
            return parseJson(out.trim())
        }

        def rowsetByVersion = { tabletJson, int version ->
            def rowset = tabletJson.rowsets.find { it.startsWith("[${version}-${version}] ") }
            assertNotNull(rowset)
            return rowset
        }

        def parseRowsetInfo = { rowset ->
            def matcher = rowset =~ /\[[0-9]+-[0-9]+\]\s+([0-9]+)\s+DATA\s+([A-Z_]+)/
            assertTrue(matcher.find(), "unexpected rowset format: ${rowset}")
            return [segments: matcher.group(1).toInteger(), overlap: matcher.group(2)]
        }

        def waitForCompaction = { beHost, bePort, tabletId, timeoutMs ->
            long deadline = System.currentTimeMillis() + timeoutMs
            def lastStatus = null
            while (System.currentTimeMillis() < deadline) {
                def (code, out, err) = be_get_compaction_status(beHost, bePort, tabletId)
                logger.info("Get compaction status: code=${code}, out=${out}, err=${err}")
                assertEquals(0, code)
                lastStatus = parseJson(out.trim())
                assertEquals("success", lastStatus.status.toLowerCase())
                if (!lastStatus.run_status) {
                    return
                }
                Thread.sleep(1000)
            }
            assertTrue(false, "compaction did not finish on ${beHost}:${bePort}, " +
                    "tablet=${tabletId}, timeoutMs=${timeoutMs}, last=${lastStatus}")
        }

        def runCumulativeCompaction = { beHost, bePort, tabletId ->
            def (code, out, err) = be_run_cumulative_compaction(beHost, bePort, tabletId)
            logger.info("Run compaction: code=${code}, out=${out}, err=${err}")
            assertEquals(0, code)
            def compactJson = parseJson(out.trim())
            assertEquals("success", compactJson.status.toLowerCase())
            waitForCompaction(beHost, bePort, tabletId, compactionTimeoutMs)
        }

        def readPointRows = { String tableName ->
            def pointResult = sql "SELECT k, v FROM ${tableName} WHERE k = 100 ORDER BY v"
            return pointResult.collect { row -> row.collect { column -> column.toString() } }
        }

        def checkPointRows = { String tableName, List<List<String>> expectedPointRows ->
            def pointRows = readPointRows(tableName)
            if (expectedPointRows != null) {
                assertEquals(expectedPointRows, pointRows)
            }
            return pointRows
        }

        def checkSingleRowsetGroupedCompaction = {
                String tableName, String keyType, String valueColumn, String extraProperties,
                int inputSegmentsPerGroup, int rowsPerLoadRound, int expectedRows,
                List<List<String>> expectedPointRows,
                boolean expectMultipleOutputSegmentsPerGroup ->
            sql "DROP TABLE IF EXISTS ${tableName}"
            sql """
                CREATE TABLE ${tableName} (
                    k INT,
                    ${valueColumn}
                )
                ${keyType}(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "disable_auto_compaction" = "true"${extraProperties}
                )
            """

            StringBuilder content = new StringBuilder()
            for (int i = 0; i < 2; i++) {
                (1..rowsPerLoadRound).each {
                    content.append("${it},${it + i}\n")
                }
            }
            streamLoad {
                table "${tableName}"
                set "column_separator", ","
                inputStream new ByteArrayInputStream(content.toString().getBytes())
                time 30000
                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(rowsPerLoadRound * 2, json.NumberTotalRows)
                    assertEquals(0, json.NumberFilteredRows)
                }
            }
            sql "sync"

            def pointRowsBeforeCompaction = checkPointRows(tableName, expectedPointRows)
            if (expectedPointRows == null) {
                assertEquals(1, pointRowsBeforeCompaction.size())
            }

            def tablets = sql_return_maparray "SHOW TABLETS FROM ${tableName}"
            assertEquals(1, tablets.size())
            def tabletId = tablets[0].TabletId
            def backendId = tablets[0].BackendId
            def backends = sql_return_maparray "SHOW BACKENDS"
            def backend = backends.find { it.BackendId == backendId }
            assertNotNull(backend)

            def before = showTablet(tableName, backend.Host, backend.HttpPort, tabletId)
            def inputRowset = rowsetByVersion(before, 2)
            def inputInfo = parseRowsetInfo(inputRowset)
            assertEquals("OVERLAPPING", inputInfo.overlap)
            assertTrue(inputInfo.segments > inputSegmentsPerGroup, inputRowset)

            set_be_param("cloud_single_rowset_compaction_segment_group_size",
                    inputSegmentsPerGroup.toString())
            runCumulativeCompaction(backend.Host, backend.HttpPort, tabletId)

            def after = showTablet(tableName, backend.Host, backend.HttpPort, tabletId)
            def outputRowset = rowsetByVersion(after, 2)
            def outputInfo = parseRowsetInfo(outputRowset)
            assertEquals("NONOVERLAPPING_WITHIN_GROUP", outputInfo.overlap)
            def expectedOutputSegments =
                    (inputInfo.segments + inputSegmentsPerGroup - 1)
                            .intdiv(inputSegmentsPerGroup)
            if (expectMultipleOutputSegmentsPerGroup) {
                assertTrue(outputInfo.segments > expectedOutputSegments, outputRowset)
            } else {
                assertEquals(expectedOutputSegments, outputInfo.segments)
            }
            def countResult = sql "SELECT COUNT(*) FROM ${tableName}"
            assertEquals(expectedRows, countResult[0][0])
            def pointRowsAfterCompaction = checkPointRows(tableName, expectedPointRows)
            if (expectedPointRows == null && pointRowsAfterCompaction != pointRowsBeforeCompaction) {
                logger.warn("Point query result changed after single rowset grouped compaction" +
                        ", table=${tableName}, before=${pointRowsBeforeCompaction}" +
                        ", after=${pointRowsAfterCompaction}")
            }

            // Compact the grouped rowset as one range. This exercises VerticalBlockReader's
            // NONOVERLAPPING_WITHIN_GROUP iterator initialization and must produce a fully
            // non-overlapping rowset.
            set_be_param("cloud_single_rowset_compaction_segment_group_size",
                    outputInfo.segments.toString())
            runCumulativeCompaction(backend.Host, backend.HttpPort, tabletId)

            def afterSecondCompaction =
                    showTablet(tableName, backend.Host, backend.HttpPort, tabletId)
            def finalRowset = rowsetByVersion(afterSecondCompaction, 2)
            def finalInfo = parseRowsetInfo(finalRowset)
            assertEquals("NONOVERLAPPING", finalInfo.overlap)
            if (expectMultipleOutputSegmentsPerGroup) {
                assertTrue(finalInfo.segments > 1, finalRowset)
                def finalRowsetMeta = getRowsetMeta(tabletId, 2)
                assertEquals(finalInfo.segments, finalRowsetMeta.num_segments.toString().toInteger())
                assertFalse(finalRowsetMeta.segments_key_bounds_aggregated ?: false)
                assertFalse(finalRowsetMeta.segments_key_bounds_truncated ?: false)
                def segmentKeyBounds = finalRowsetMeta.segments_key_bounds
                assertEquals(finalInfo.segments, segmentKeyBounds.size())
                segmentKeyBounds.eachWithIndex { keyBounds, int segmentId ->
                    assertTrue(compareEncodedKeys(keyBounds.min_key, keyBounds.max_key) <= 0,
                            "invalid key range at segment ${segmentId}")
                    if (segmentId > 0) {
                        def previousKeyBounds = segmentKeyBounds[segmentId - 1]
                        assertTrue(compareEncodedKeys(
                                        previousKeyBounds.max_key, keyBounds.min_key) < 0,
                                "overlapping key ranges at segments ${segmentId - 1} and " +
                                        "${segmentId}")
                    }
                }
            }
            def finalCountResult = sql "SELECT COUNT(*) FROM ${tableName}"
            assertEquals(expectedRows, finalCountResult[0][0])
            def finalPointRows = checkPointRows(tableName, expectedPointRows)
            if (expectedPointRows == null && finalPointRows != pointRowsAfterCompaction) {
                logger.warn("Point query result changed after repeated single rowset compaction" +
                        ", table=${tableName}, before=${pointRowsAfterCompaction}" +
                        ", after=${finalPointRows}")
            }
        }

        GetDebugPoint().clearDebugPointsForAllBEs()

        try {
            GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")

            // ====================================================
            logCaseSection("Standard cases: compact DUP, AGG, MOW, and MOR rowsets twice " +
                    "with 2 or 4 input segments per group")
            [2, 4].each { int inputSegmentsPerGroup ->
                checkSingleRowsetGroupedCompaction(
                        "test_cloud_single_rowset_grouped_compaction_g${inputSegmentsPerGroup}_dup",
                        "DUPLICATE KEY", "v INT", "", inputSegmentsPerGroup, 8192, 8192 * 2,
                        [["100", "100"], ["100", "101"]], false)
                checkSingleRowsetGroupedCompaction(
                        "test_cloud_single_rowset_grouped_compaction_g${inputSegmentsPerGroup}_agg",
                        "AGGREGATE KEY", "v INT SUM", "", inputSegmentsPerGroup, 8192, 8192,
                        [["100", "201"]], false)
                checkSingleRowsetGroupedCompaction(
                        "test_cloud_single_rowset_grouped_compaction_g${inputSegmentsPerGroup}_mow",
                        "UNIQUE KEY", "v INT",
                        ", \"enable_unique_key_merge_on_write\" = \"true\"",
                        inputSegmentsPerGroup, 8192, 8192, [["100", "101"]], false)
                checkSingleRowsetGroupedCompaction(
                        "test_cloud_single_rowset_grouped_compaction_g${inputSegmentsPerGroup}_mor",
                        "UNIQUE KEY", "v INT",
                        ", \"enable_unique_key_merge_on_write\" = \"false\"",
                        inputSegmentsPerGroup, 8192, 8192, null, false)
            }

            // ====================================================
            logCaseSection("MOW delete-bitmap case: compact each of three overlapping rowsets " +
                    "twice and verify cross-rowset delete bitmap conversion")
            sql "DROP TABLE IF EXISTS test_cloud_single_rowset_compaction_mow_delete_bitmap"
            sql """
                CREATE TABLE test_cloud_single_rowset_compaction_mow_delete_bitmap (
                    k INT,
                    v INT
                )
                UNIQUE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "disable_auto_compaction" = "true",
                    "enable_unique_key_merge_on_write" = "true"
                )
            """

            def loadMowRows = {
                    int startKey, int endKey, int valueBase, int duplicateRounds ->
                StringBuilder content = new StringBuilder()
                (startKey..endKey).each { int key ->
                    content.append("${key},${valueBase + key}\n")
                }
                // Append an inner key range to create duplicates within this rowset. Keep the
                // offsets deterministic so a failure can be reproduced exactly.
                int duplicateStartKey = startKey + 2048
                int duplicateEndKey = endKey - 3072
                for (int round = 0; round < duplicateRounds; ++round) {
                    (duplicateStartKey..duplicateEndKey).each { int key ->
                        content.append("${key},${valueBase + key + 1}\n")
                    }
                }
                streamLoad {
                    table "test_cloud_single_rowset_compaction_mow_delete_bitmap"
                    set "column_separator", ","
                    inputStream new ByteArrayInputStream(content.toString().getBytes())
                    time 30000
                    check { result, exception, startTime, endTime ->
                        if (exception != null) {
                            throw exception
                        }
                        def json = parseJson(result)
                        assertEquals("success", json.Status.toLowerCase())
                        int originalRows = endKey - startKey + 1
                        int duplicateRows = duplicateEndKey - duplicateStartKey + 1
                        assertEquals(originalRows + duplicateRows * duplicateRounds,
                                json.NumberTotalRows)
                        assertEquals(0, json.NumberFilteredRows)
                    }
                }
                sql "sync"
            }

            loadMowRows(1, 16384, 100000, 1)
            loadMowRows(8193, 24576, 200000, 2)
            loadMowRows(12289, 28672, 300000, 3)

            def readMowRows = {
                def rows = sql """
                    SELECT k, v
                    FROM test_cloud_single_rowset_compaction_mow_delete_bitmap
                    WHERE k IN (1, 8192, 8193, 12288, 12289, 16384, 16385, 24576, 24577, 28672)
                    ORDER BY k
                """
                return rows.collect { row -> row.collect { column -> column.toString() } }
            }
            def expectedMowRows = [
                ["1", "100001"],
                ["8192", "108193"],
                ["8193", "208193"],
                ["12288", "212289"],
                ["12289", "312289"],
                ["16384", "316385"],
                ["16385", "316386"],
                ["24576", "324577"],
                ["24577", "324578"],
                ["28672", "328672"]
            ]
            assertEquals(expectedMowRows, readMowRows())
            def mowCountBeforeCompaction =
                    sql "SELECT COUNT(*) FROM test_cloud_single_rowset_compaction_mow_delete_bitmap"
            assertEquals(28672, mowCountBeforeCompaction[0][0])

            def mowTablets = sql_return_maparray(
                    "SHOW TABLETS FROM test_cloud_single_rowset_compaction_mow_delete_bitmap")
            assertEquals(1, mowTablets.size())
            def mowTabletId = mowTablets[0].TabletId
            def mowBackendId = mowTablets[0].BackendId
            def mowBackends = sql_return_maparray "SHOW BACKENDS"
            def mowBackend = mowBackends.find { it.BackendId == mowBackendId }
            assertNotNull(mowBackend)

            def checkMowQueryResult = {
                def countResult =
                        sql "SELECT COUNT(*) FROM test_cloud_single_rowset_compaction_mow_delete_bitmap"
                assertEquals(mowCountBeforeCompaction, countResult)
                assertEquals(expectedMowRows, readMowRows())
            }

            def mowRowsetVersions = [2, 3, 4]
            def mowMaxSegmentSizeByVersion = [2: 32768, 3: 8192, 4: 2048]
            def finalMowSegmentCounts = [:]
            def compactMowRowsetTwice = { int targetVersion ->
                logger.info("Compact MOW rowset version ${targetVersion} twice")
                def beforeFirstCompaction = showTablet(
                        "test_cloud_single_rowset_compaction_mow_delete_bitmap",
                        mowBackend.Host, mowBackend.HttpPort, mowTabletId)
                def inputRowset = rowsetByVersion(beforeFirstCompaction, targetVersion)
                def inputInfo = parseRowsetInfo(inputRowset)
                assertEquals("OVERLAPPING", inputInfo.overlap)
                assertTrue(inputInfo.segments > initialInputSegmentsPerGroup, inputRowset)
                def untouchedVersions = mowRowsetVersions.findAll { it != targetVersion }
                def untouchedRowsets = untouchedVersions.collectEntries { int version ->
                    [(version): rowsetByVersion(beforeFirstCompaction, version)]
                }

                set_be_param("vertical_compaction_max_segment_size",
                        mowMaxSegmentSizeByVersion[targetVersion].toString())
                set_be_param("cloud_single_rowset_compaction_segment_group_size",
                        initialInputSegmentsPerGroup.toString())
                runCumulativeCompaction(mowBackend.Host, mowBackend.HttpPort, mowTabletId)

                def afterFirstCompaction = showTablet(
                        "test_cloud_single_rowset_compaction_mow_delete_bitmap",
                        mowBackend.Host, mowBackend.HttpPort, mowTabletId)
                def groupedRowset = rowsetByVersion(afterFirstCompaction, targetVersion)
                def groupedInfo = parseRowsetInfo(groupedRowset)
                assertEquals("NONOVERLAPPING_WITHIN_GROUP", groupedInfo.overlap)
                assertNotEquals(inputRowset, groupedRowset)
                untouchedRowsets.each { int version, String rowset ->
                    assertEquals(rowset, rowsetByVersion(afterFirstCompaction, version))
                }
                checkMowQueryResult()

                set_be_param("cloud_single_rowset_compaction_segment_group_size",
                        groupedInfo.segments.toString())
                runCumulativeCompaction(mowBackend.Host, mowBackend.HttpPort, mowTabletId)

                def afterSecondCompaction = showTablet(
                        "test_cloud_single_rowset_compaction_mow_delete_bitmap",
                        mowBackend.Host, mowBackend.HttpPort, mowTabletId)
                def nonoverlappingRowset = rowsetByVersion(afterSecondCompaction, targetVersion)
                def nonoverlappingInfo = parseRowsetInfo(nonoverlappingRowset)
                assertEquals("NONOVERLAPPING", nonoverlappingInfo.overlap)
                assertNotEquals(groupedRowset, nonoverlappingRowset)
                finalMowSegmentCounts[targetVersion] = nonoverlappingInfo.segments
                untouchedRowsets.each { int version, String rowset ->
                    assertEquals(rowset, rowsetByVersion(afterSecondCompaction, version))
                }
                checkMowQueryResult()
            }

            set_be_param("enable_rowid_conversion_correctness_check", "true")
            set_be_param("compaction_batch_size", "512")
            mowRowsetVersions.each { int version -> compactMowRowsetTwice(version) }
            assertEquals(mowRowsetVersions.size(), finalMowSegmentCounts.values().toSet().size(),
                    "expected different final segment counts: ${finalMowSegmentCounts}")

            // ====================================================
            logCaseSection("Multi-segment case: verify disjoint key ranges after the second " +
                    "cumulative compaction")
            set_be_param("cloud_single_rowset_compaction_segment_group_size",
                    initialInputSegmentsPerGroup.toString())
            set_be_param("vertical_compaction_max_segment_size", "2048")
            set_be_param("compaction_batch_size", "512")
            checkSingleRowsetGroupedCompaction(
                    "test_cloud_single_rowset_grouped_compact_multi_seg_dup",
                    "DUPLICATE KEY", "v INT", "", initialInputSegmentsPerGroup, 32768, 32768 * 2,
                    [["100", "100"], ["100", "101"]], true)

            // ====================================================
            logCaseSection("Schema-change case: verify CREATE INDEX clears the grouped rowset " +
                    "layout")
            sql "DROP TABLE IF EXISTS test_cloud_grouped_compaction_schema_change"
            sql """
                CREATE TABLE test_cloud_grouped_compaction_schema_change (
                    k INT,
                    v INT
                )
                DUPLICATE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "disable_auto_compaction" = "true"
                )
            """

            StringBuilder schemaChangeContent = new StringBuilder()
            for (int i = 0; i < 2; i++) {
                (1..8192).each {
                    schemaChangeContent.append("${it},${it + i}\n")
                }
            }
            streamLoad {
                table "test_cloud_grouped_compaction_schema_change"
                set "column_separator", ","
                inputStream new ByteArrayInputStream(schemaChangeContent.toString().getBytes())
                time 30000
                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(8192 * 2, json.NumberTotalRows)
                    assertEquals(0, json.NumberFilteredRows)
                }
            }
            sql "sync"

            def schemaChangeTablets =
                    sql_return_maparray "SHOW TABLETS FROM test_cloud_grouped_compaction_schema_change"
            assertEquals(1, schemaChangeTablets.size())
            def oldTabletId = schemaChangeTablets[0].TabletId
            def oldBackendId = schemaChangeTablets[0].BackendId
            def schemaChangeBackends = sql_return_maparray "SHOW BACKENDS"
            def oldBackend = schemaChangeBackends.find { it.BackendId == oldBackendId }
            assertNotNull(oldBackend)

            set_be_param("cloud_single_rowset_compaction_segment_group_size",
                    initialInputSegmentsPerGroup.toString())
            runCumulativeCompaction(oldBackend.Host, oldBackend.HttpPort, oldTabletId)

            def groupedTablet = showTablet("test_cloud_grouped_compaction_schema_change",
                    oldBackend.Host, oldBackend.HttpPort, oldTabletId)
            def groupedRowset = rowsetByVersion(groupedTablet, 2)
            def groupedInfo = parseRowsetInfo(groupedRowset)
            assertEquals("NONOVERLAPPING_WITHIN_GROUP", groupedInfo.overlap)

            sql """
                CREATE INDEX idx_v ON test_cloud_grouped_compaction_schema_change(v)
                USING INVERTED
            """
            waitForSchemaChangeDone {
                sql """
                    SHOW ALTER TABLE COLUMN
                    WHERE TableName='test_cloud_grouped_compaction_schema_change'
                    ORDER BY CreateTime DESC LIMIT 1
                """
                time 90
            }

            schemaChangeTablets =
                    sql_return_maparray "SHOW TABLETS FROM test_cloud_grouped_compaction_schema_change"
            assertEquals(1, schemaChangeTablets.size())
            def newTabletId = schemaChangeTablets[0].TabletId
            assertNotEquals(oldTabletId, newTabletId)
            def newBackendId = schemaChangeTablets[0].BackendId
            schemaChangeBackends = sql_return_maparray "SHOW BACKENDS"
            def newBackend = schemaChangeBackends.find { it.BackendId == newBackendId }
            assertNotNull(newBackend)

            def rewrittenTablet = showTablet("test_cloud_grouped_compaction_schema_change",
                    newBackend.Host, newBackend.HttpPort, newTabletId)
            def rewrittenRowset = rowsetByVersion(rewrittenTablet, 2)
            def rewrittenInfo = parseRowsetInfo(rewrittenRowset)
            assertNotEquals("NONOVERLAPPING_WITHIN_GROUP", rewrittenInfo.overlap)
            def rewrittenRowsetMeta = getRowsetMeta(newTabletId, 2)
            assertTrue((rewrittenRowsetMeta.segment_group_sizes ?: []).isEmpty())
            def rewrittenCount =
                    sql "SELECT COUNT(*) FROM test_cloud_grouped_compaction_schema_change"
            assertEquals(8192 * 2, rewrittenCount[0][0])
            def rewrittenPointRows =
                    readPointRows("test_cloud_grouped_compaction_schema_change")
            assertEquals([["100", "100"], ["100", "101"]], rewrittenPointRows)
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
        }
    }
}
