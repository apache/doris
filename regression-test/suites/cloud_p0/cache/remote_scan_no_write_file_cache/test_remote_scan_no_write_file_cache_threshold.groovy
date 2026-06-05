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
import org.apache.doris.regression.util.Http

import java.util.Random

suite("test_remote_scan_no_write_file_cache_threshold", "docker") {
    def cacheBlockSize = 262144L
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)
    options.msNum = 1
    options.beConfigs += [
        "enable_file_cache=true",
        "disable_storage_page_cache=true",
        "enable_java_support=false",
        "enable_evict_file_cache_in_advance=false",
        "enable_read_cache_file_directly=false",
        "inverted_index_query_cache_limit=67108864",
        "inverted_index_searcher_cache_limit=67108864",
        "file_cache_enter_disk_resource_limit_mode_percent=99",
        "file_cache_each_block_size=${cacheBlockSize}",
        "file_cache_path=[{\"path\":\"/opt/apache-doris/be/storage/file_cache\",\"total_size\":134217728,\"query_limit\":134217728}]"
    ]

    docker(options) {
        def clusters = sql "SHOW CLUSTERS"
        assert !clusters.isEmpty()
        def computeGroup = clusters[0][0]
        sql "use @${computeGroup}"

        def backends = sql "SHOW BACKENDS"
        assert backends.size() == 1
        def beHost = backends[0][1]
        def beHttpPort = backends[0][4]
        def supportMaxScannersConcurrency =
                !(sql "show variables like 'max_scanners_concurrency'").isEmpty()

        def rowsPerTable = 4096
        def batchRows = 512
        def invertedIndexBatchRows = 128
        def halfCacheBlockSize = (cacheBlockSize / 2).toLong()
        def chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

        def clearFileCache = {
            def result = Http.GET("http://${beHost}:${beHttpPort}/api/file_cache?op=clear&sync=true", true)
            logger.info("clear file cache result: ${result}")
            assert result.status.toString().equalsIgnoreCase("OK") : result
        }

        def randomText = { Random random, int length ->
            def builder = new StringBuilder(length)
            for (int i = 0; i < length; i++) {
                builder.append(chars.charAt(random.nextInt(chars.length())))
            }
            return builder.toString()
        }

        def insertRows = { String tableName ->
            for (int batchStart = 0; batchStart < rowsPerTable; batchStart += batchRows) {
                def random = new Random(((long) tableName.hashCode()) * 131 + batchStart)
                def data = new StringBuilder(batchRows * 3500)
                for (int i = 0; i < batchRows; i++) {
                    int id = batchStart + i
                    data.append(id).append('\t')
                    data.append(id % 128).append('\t')
                    data.append(randomText(random, 2048)).append('\t')
                    data.append(randomText(random, 1024)).append('\n')
                }
                streamLoad {
                    table tableName
                    set 'column_separator', '\t'
                    set 'columns', 'id,group_id,payload,pad'
                    inputText data.toString()
                    time 60000
                    check { result, exception, startTime, endTime ->
                        if (exception != null) {
                            throw exception
                        }
                        logger.info("stream load ${tableName} result: ${result}")
                        def json = parseJson(result)
                        assert json.Status.toString().equalsIgnoreCase("success") : result
                        assert json.NumberLoadedRows.toString().toInteger() == batchRows : result
                    }
                }
            }
            sql "SYNC"
        }

        def insertInvertedIndexRows = { String tableName ->
            for (int batchStart = 0; batchStart < rowsPerTable; batchStart += invertedIndexBatchRows) {
                def random = new Random(((long) tableName.hashCode()) * 257 + batchStart)
                def data = new StringBuilder(invertedIndexBatchRows * 3500)
                for (int i = 0; i < invertedIndexBatchRows; i++) {
                    int id = batchStart + i
                    data.append(id).append('\t')
                    data.append("needle group").append(id % 16).append(' ')
                    data.append(randomText(random, 2048)).append('\t')
                    data.append(randomText(random, 1024)).append('\n')
                }
                streamLoad {
                    table tableName
                    set 'column_separator', '\t'
                    set 'columns', 'id,body,pad'
                    inputText data.toString()
                    time 60000
                    check { result, exception, startTime, endTime ->
                        if (exception != null) {
                            throw exception
                        }
                        logger.info("stream load ${tableName} result: ${result}")
                        def json = parseJson(result)
                        assert json.Status.toString().equalsIgnoreCase("success") : result
                        assert json.NumberLoadedRows.toString().toInteger() == invertedIndexBatchRows :
                                result
                    }
                }
            }
            sql "SYNC"
        }

        def parseProfileCounterValue = { String valueText ->
            def exact = (valueText =~ /\((\d+)\)/)
            if (exact.find()) {
                return exact.group(1).toLong()
            }
            def number = (valueText =~ /([0-9]+(?:\.[0-9]+)?)\s*(B|KB|MB|GB)?/)
            if (!number.find()) {
                return 0L
            }
            BigDecimal value = new BigDecimal(number.group(1))
            long multiplier = 1L
            if (number.group(2) == "KB") {
                multiplier = 1024L
            } else if (number.group(2) == "MB") {
                multiplier = 1024L * 1024L
            } else if (number.group(2) == "GB") {
                multiplier = 1024L * 1024L * 1024L
            }
            return (value * multiplier).toLong()
        }

        def parseProfileCounterAggregateValue = { String valueText, String aggregateName ->
            def aggregate = (valueText =~ ("(?:^|,\\s*)" +
                    java.util.regex.Pattern.quote(aggregateName) + "\\s+([^,]+)"))
            if (aggregate.find()) {
                return parseProfileCounterValue(aggregate.group(1).toString())
            }
            if (aggregateName.equalsIgnoreCase("max")) {
                def highWaterMarkPeak = (valueText =~ /(?:^|\s)\(Peak:\s*(.+)\)\s*$/)
                if (highWaterMarkPeak.find()) {
                    return parseProfileCounterValue(highWaterMarkPeak.group(1).toString())
                }
            }
            return parseProfileCounterValue(valueText)
        }

        def detailProfileSection = { String profileString ->
            int start = profileString.indexOf("\nDetailProfile(")
            if (start < 0) {
                start = profileString.indexOf("DetailProfile(")
            }
            if (start < 0) {
                return profileString
            }
            int end = profileString.indexOf("\nAppendix:", start)
            return end < 0 ? profileString.substring(start) : profileString.substring(start, end)
        }

        def profileCounterValues = { String profileString, String counterName, String aggregateName = null ->
            def values = []
            def profileSection = detailProfileSection(profileString)
            def matcher = (profileSection =~ ("(?m)^\\s*(?:-\\s*)?" +
                    java.util.regex.Pattern.quote(counterName) + "(?::\\s+|\\s+Current:\\s+)([^\\n]+)"))
            while (matcher.find()) {
                def valueText = matcher.group(1).toString()
                values.add(aggregateName == null ? parseProfileCounterValue(valueText) :
                        parseProfileCounterAggregateValue(valueText, aggregateName))
            }
            return values
        }

        def sumProfileCounter = { String profileString, String counterName ->
            long total = 0
            profileCounterValues(profileString, counterName).each {
                total += it
            }
            return total
        }

        def maxProfileCounter = { String profileString, String counterName ->
            def values = profileCounterValues(profileString, counterName, "max")
            values.addAll(profileCounterValues(profileString, "${counterName}Peak"))
            return values.isEmpty() ? 0L : values.max()
        }

        def runProfileQuery = { String name, String query, Closure checker ->
            profile(name) {
                run {
                    sql "/* ${name} */ ${query}"
                    sleep(1000)
                }
                check { profileString, exception ->
                    if (exception != null) {
                        logger.error("Profile failed, profile result:\n${profileString}", exception)
                        throw exception
                    }
                    assert profileString.contains("Is  Cached:  No") ||
                            profileString.contains("Is Cached: No") :
                            "query should not hit SQL/query cache, profile=${profileString}"
                    checker(profileString)
                }
            }
        }

        def assertScannerShape = { String label, long scannerNum, boolean expectMultipleScanners,
                String profileString ->
            if (expectMultipleScanners) {
                assert scannerNum > 1L :
                        "${label}: expected more than one scanner, profile=${profileString}"
            } else {
                assert scannerNum == 1L :
                        "${label}: expected single scanner, profile=${profileString}"
            }
        }

        def assertColdWriteThrough = { String label, String profileString,
                boolean expectMultipleScanners ->
            def remoteOnlyTriggered = maxProfileCounter(profileString, "RemoteOnlyOnMissTriggered")
            def skipCacheIo = sumProfileCounter(profileString, "NumSkipCacheIOTotal")
            def remoteBytes = sumProfileCounter(profileString, "BytesScannedFromRemote")
            def writeCacheBytes = sumProfileCounter(profileString, "BytesWriteIntoCache")
            def scannerNum = sumProfileCounter(profileString, "NumScanners")
            logger.info("${label} cold write-through counters: remoteOnlyTriggered=${remoteOnlyTriggered}, " +
                    "skipCacheIo=${skipCacheIo}, remoteBytes=${remoteBytes}, " +
                    "writeCacheBytes=${writeCacheBytes}, scannerNum=${scannerNum}")

            assert remoteOnlyTriggered == 0L :
                    "${label}: threshold should be disabled, profile=${profileString}"
            assert skipCacheIo == 0L :
                    "${label}: cold write-through should not use skip-cache IO, profile=${profileString}"
            assert writeCacheBytes > 0L :
                    "${label}: expected cold read to write file cache, profile=${profileString}"
            assertScannerShape(label, scannerNum, expectMultipleScanners, profileString)
            return [
                    remoteBytes: remoteBytes,
                    writeCacheBytes: writeCacheBytes,
                    scannerNum: scannerNum
            ]
        }

        def assertThresholdRemoteOnly = { String label, String profileString,
                boolean expectMultipleScanners, long thresholdBytes ->
            def remoteOnlyTriggered = maxProfileCounter(profileString, "RemoteOnlyOnMissTriggered")
            def thresholdMetricBytes =
                    maxProfileCounter(profileString, "RemoteOnlyOnMissThresholdBytes")
            def skipCacheIo = sumProfileCounter(profileString, "NumSkipCacheIOTotal")
            def remoteBytes = sumProfileCounter(profileString, "BytesScannedFromRemote")
            def writeCacheBytes = sumProfileCounter(profileString, "BytesWriteIntoCache")
            def scannerNum = sumProfileCounter(profileString, "NumScanners")
            logger.info("${label} threshold counters: remoteOnlyTriggered=${remoteOnlyTriggered}, " +
                    "thresholdMetricBytes=${thresholdMetricBytes}, skipCacheIo=${skipCacheIo}, " +
                    "remoteBytes=${remoteBytes}, writeCacheBytes=${writeCacheBytes}, " +
                    "scannerNum=${scannerNum}")

            assert remoteOnlyTriggered == 1L :
                    "${label}: expected threshold to switch the query to remote-only-on-miss, profile=${profileString}"
            assert thresholdMetricBytes == thresholdBytes :
                    "${label}: expected threshold bytes in profile, thresholdBytes=${thresholdBytes}, " +
                    "profile=${profileString}"
            assert skipCacheIo > 0L :
                    "${label}: expected later cache misses to skip cache writes, profile=${profileString}"
            assert writeCacheBytes <= thresholdBytes :
                    "${label}: file cache writes should not exceed threshold, thresholdBytes=" +
                    "${thresholdBytes}, profile=${profileString}"
            if (thresholdBytes == 0L) {
                assert writeCacheBytes == 0L :
                        "${label}: zero threshold should not write any file cache, profile=${profileString}"
            } else if (thresholdBytes >= cacheBlockSize) {
                assert writeCacheBytes > 0L :
                        "${label}: threshold should allow at least one cache block write before trigger, " +
                        "profile=${profileString}"
            }
            assertScannerShape(label, scannerNum, expectMultipleScanners, profileString)
            return [
                    remoteBytes: remoteBytes,
                    writeCacheBytes: writeCacheBytes,
                    skipCacheIo: skipCacheIo,
                    thresholdMetricBytes: thresholdMetricBytes,
                    scannerNum: scannerNum
            ]
        }

        def assertInvertedIndexColdWriteThrough = { String label, String profileString,
                long minReaderMisses, long minInvertedRemoteBytes ->
            def remoteOnlyTriggered = maxProfileCounter(profileString, "RemoteOnlyOnMissTriggered")
            def skipCacheIo = sumProfileCounter(profileString, "NumSkipCacheIOTotal")
            def writeCacheBytes = sumProfileCounter(profileString, "BytesWriteIntoCache")
            def invertedRemoteBytes =
                    sumProfileCounter(profileString, "InvertedIndexBytesScannedFromRemote")
            def queryCacheMiss = sumProfileCounter(profileString, "InvertedIndexQueryCacheMiss")
            def searcherCacheMiss =
                    sumProfileCounter(profileString, "InvertedIndexSearcherCacheMiss")
            logger.info("${label} inverted cold write-through counters: " +
                    "remoteOnlyTriggered=${remoteOnlyTriggered}, skipCacheIo=${skipCacheIo}, " +
                    "writeCacheBytes=${writeCacheBytes}, invertedRemoteBytes=${invertedRemoteBytes}, " +
                    "queryCacheMiss=${queryCacheMiss}, searcherCacheMiss=${searcherCacheMiss}, " +
                    "minReaderMisses=${minReaderMisses}, " +
                    "minInvertedRemoteBytes=${minInvertedRemoteBytes}")

            assert remoteOnlyTriggered == 0L :
                    "${label}: threshold should be disabled, profile=${profileString}"
            assert skipCacheIo == 0L :
                    "${label}: cold inverted-index read should not skip cache writes, profile=${profileString}"
            assert writeCacheBytes > 0L :
                    "${label}: expected inverted-index read to write file cache, profile=${profileString}"
            assert invertedRemoteBytes >= minInvertedRemoteBytes :
                    "${label}: query should trigger enough inverted-index remote reads, " +
                    "minInvertedRemoteBytes=${minInvertedRemoteBytes}, profile=${profileString}"
            assert queryCacheMiss >= minReaderMisses :
                    "${label}: query cache should not hide inverted-index reads, profile=${profileString}"
            assert searcherCacheMiss >= minReaderMisses :
                    "${label}: searcher cache should not hide inverted-index reads, profile=${profileString}"
        }

        def assertInvertedIndexThresholdRemoteOnly = { String label, String profileString,
                long thresholdBytes, long minReaderMisses, long minInvertedRemoteBytes ->
            def remoteOnlyTriggered = maxProfileCounter(profileString, "RemoteOnlyOnMissTriggered")
            def thresholdMetricBytes =
                    maxProfileCounter(profileString, "RemoteOnlyOnMissThresholdBytes")
            def skipCacheIo = sumProfileCounter(profileString, "NumSkipCacheIOTotal")
            def writeCacheBytes = sumProfileCounter(profileString, "BytesWriteIntoCache")
            def invertedRemoteBytes =
                    sumProfileCounter(profileString, "InvertedIndexBytesScannedFromRemote")
            def queryCacheMiss = sumProfileCounter(profileString, "InvertedIndexQueryCacheMiss")
            def searcherCacheMiss =
                    sumProfileCounter(profileString, "InvertedIndexSearcherCacheMiss")
            logger.info("${label} inverted threshold counters: " +
                    "remoteOnlyTriggered=${remoteOnlyTriggered}, " +
                    "thresholdMetricBytes=${thresholdMetricBytes}, skipCacheIo=${skipCacheIo}, " +
                    "writeCacheBytes=${writeCacheBytes}, invertedRemoteBytes=${invertedRemoteBytes}, " +
                    "queryCacheMiss=${queryCacheMiss}, searcherCacheMiss=${searcherCacheMiss}, " +
                    "minReaderMisses=${minReaderMisses}, " +
                    "minInvertedRemoteBytes=${minInvertedRemoteBytes}")

            assert remoteOnlyTriggered == 1L :
                    "${label}: expected threshold to switch the query to remote-only-on-miss, " +
                    "profile=${profileString}"
            assert thresholdMetricBytes == thresholdBytes :
                    "${label}: expected positive threshold in profile, thresholdBytes=${thresholdBytes}, " +
                    "profile=${profileString}"
            assert skipCacheIo > 0L :
                    "${label}: expected inverted-index cache misses to skip cache writes, " +
                    "profile=${profileString}"
            assert writeCacheBytes > 0L && writeCacheBytes <= thresholdBytes :
                    "${label}: positive threshold should allow some writes before blocking later " +
                    "inverted-index cache writes, thresholdBytes=${thresholdBytes}, " +
                    "profile=${profileString}"
            assert invertedRemoteBytes >= minInvertedRemoteBytes :
                    "${label}: query should trigger enough inverted-index remote reads, " +
                    "minInvertedRemoteBytes=${minInvertedRemoteBytes}, profile=${profileString}"
            assert queryCacheMiss >= minReaderMisses :
                    "${label}: query cache should not hide inverted-index reads, profile=${profileString}"
            assert searcherCacheMiss >= minReaderMisses :
                    "${label}: searcher cache should not hide inverted-index reads, profile=${profileString}"
        }

        def assertInvertedIndexSearcherCacheHitThresholdRemoteOnly = { String label,
                String profileString, long thresholdBytes, long minReaderHits,
                long minInvertedRemoteBytes ->
            def remoteOnlyTriggered = maxProfileCounter(profileString, "RemoteOnlyOnMissTriggered")
            def thresholdMetricBytes =
                    maxProfileCounter(profileString, "RemoteOnlyOnMissThresholdBytes")
            def skipCacheIo = sumProfileCounter(profileString, "NumSkipCacheIOTotal")
            def writeCacheBytes = sumProfileCounter(profileString, "BytesWriteIntoCache")
            def invertedRemoteBytes =
                    sumProfileCounter(profileString, "InvertedIndexBytesScannedFromRemote")
            def queryCacheMiss = sumProfileCounter(profileString, "InvertedIndexQueryCacheMiss")
            def searcherCacheHit =
                    sumProfileCounter(profileString, "InvertedIndexSearcherCacheHit")
            logger.info("${label} inverted searcher-cache-hit counters: " +
                    "remoteOnlyTriggered=${remoteOnlyTriggered}, " +
                    "thresholdMetricBytes=${thresholdMetricBytes}, skipCacheIo=${skipCacheIo}, " +
                    "writeCacheBytes=${writeCacheBytes}, invertedRemoteBytes=${invertedRemoteBytes}, " +
                    "queryCacheMiss=${queryCacheMiss}, searcherCacheHit=${searcherCacheHit}, " +
                    "minReaderHits=${minReaderHits}, minInvertedRemoteBytes=${minInvertedRemoteBytes}")

            assert remoteOnlyTriggered == 1L :
                    "${label}: expected threshold to switch the query to remote-only-on-miss, " +
                    "profile=${profileString}"
            assert thresholdMetricBytes == thresholdBytes :
                    "${label}: expected threshold bytes in profile, thresholdBytes=${thresholdBytes}, " +
                    "profile=${profileString}"
            assert skipCacheIo > 0L :
                    "${label}: expected cached-searcher posting reads to skip cache writes, " +
                    "profile=${profileString}"
            assert writeCacheBytes == 0L :
                    "${label}: zero threshold should not write inverted-index file cache, " +
                    "profile=${profileString}"
            assert invertedRemoteBytes >= minInvertedRemoteBytes :
                    "${label}: query should trigger enough inverted-index remote posting reads, " +
                    "minInvertedRemoteBytes=${minInvertedRemoteBytes}, profile=${profileString}"
            assert queryCacheMiss >= minReaderHits :
                    "${label}: query cache should miss for the different term, profile=${profileString}"
            assert searcherCacheHit >= minReaderHits :
                    "${label}: query should reuse cached inverted-index searchers, profile=${profileString}"
        }

        def runCase = { String tableName, int buckets, long scannerLimit,
                long scannerConcurrency, boolean expectMultipleScanners ->
            sql "DROP TABLE IF EXISTS ${tableName} FORCE"
            sql """
                CREATE TABLE ${tableName} (
                    id INT NOT NULL,
                    group_id INT NOT NULL,
                    payload VARCHAR(4096) NOT NULL,
                    pad VARCHAR(4096) NOT NULL
                ) ENGINE=OLAP
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS ${buckets}
                PROPERTIES (
                    "replication_num" = "1",
                    "disable_auto_compaction" = "true"
                )
            """
            insertRows(tableName)

            def rowCount = sql "SELECT COUNT(*) FROM ${tableName}"
            assert rowCount[0][0] == rowsPerTable

            sql "set enable_profile = true"
            sql "set profile_level = 2"
            sql "set enable_sql_cache = false"
            sql "set enable_query_cache = false"
            sql "set enable_file_cache = false"
            sql "set enable_page_cache = false"
            sql "set parallel_fragment_exec_instance_num = 1"
            sql "set enable_parallel_scan = true"
            sql "set parallel_scan_max_scanners_count = ${scannerLimit}"
            sql "set parallel_scan_min_rows_per_scanner = 1024"
            if (supportMaxScannersConcurrency) {
                sql "set max_scanners_concurrency = ${scannerConcurrency}"
            }
            sql "set parallel_pipeline_task_num = ${expectMultipleScanners ? 4 : 1}"

            def query = "SELECT SUM(LENGTH(payload) + LENGTH(pad)) FROM ${tableName}"

            clearFileCache()
            sql "set remote_scan_no_write_file_cache_threshold_bytes = -1"
            runProfileQuery("${tableName}_cold_write_through", query) { profileString ->
                assertColdWriteThrough("${tableName}_cold_write_through", profileString,
                        expectMultipleScanners)
            }

            def thresholdCases = [
                    [name: "zero", bytes: 0L],
                    [name: "half_block", bytes: halfCacheBlockSize],
                    [name: "one_block", bytes: cacheBlockSize],
                    [name: "two_and_half_blocks", bytes: cacheBlockSize * 2 + halfCacheBlockSize]
            ]
            thresholdCases.each { thresholdCase ->
                clearFileCache()
                sql "set remote_scan_no_write_file_cache_threshold_bytes = ${thresholdCase.bytes}"
                def thresholdVariable = sql """
                    show variables like 'remote_scan_no_write_file_cache_threshold_bytes'
                """
                assert !thresholdVariable.isEmpty() &&
                        thresholdVariable[0][1].toString().toLong() == thresholdCase.bytes.toLong() :
                        "failed to set remote_scan_no_write_file_cache_threshold_bytes to " +
                        "${thresholdCase.bytes}, actual=${thresholdVariable}"
                def profileName = "${tableName}_threshold_${thresholdCase.name}"
                runProfileQuery(profileName, query) { profileString ->
                    assertThresholdRemoteOnly(
                            profileName, profileString, expectMultipleScanners, thresholdCase.bytes.toLong())
                }
            }
        }

        def createInvertedIndexTable = { String tableName ->
            sql "DROP TABLE IF EXISTS ${tableName} FORCE"
            sql """
                CREATE TABLE ${tableName} (
                    id INT NOT NULL,
                    body TEXT NOT NULL,
                    pad VARCHAR(4096) NOT NULL,
                    INDEX body_idx(body) USING INVERTED
                    PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT ''
                ) ENGINE=OLAP
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "disable_auto_compaction" = "true",
                    "inverted_index_storage_format" = "V2"
                )
            """
            insertInvertedIndexRows(tableName)

            def rowCount = sql "SELECT COUNT(*) FROM ${tableName}"
            assert rowCount[0][0] == rowsPerTable
        }

        def setupInvertedIndexQuery = {
            sql "set enable_profile = true"
            sql "set profile_level = 2"
            sql "set enable_sql_cache = false"
            sql "set enable_query_cache = false"
            sql "set enable_file_cache = false"
            sql "set enable_page_cache = false"
            sql "set enable_inverted_index_query = true"
            sql "set enable_common_expr_pushdown_for_inverted_index = true"
            sql "set enable_match_without_inverted_index = false"
            sql "set parallel_fragment_exec_instance_num = 1"
            sql "set enable_parallel_scan = true"
            sql "set parallel_scan_max_scanners_count = 1"
            sql "set parallel_scan_min_rows_per_scanner = 1024"
            if (supportMaxScannersConcurrency) {
                sql "set max_scanners_concurrency = 1"
            }
            sql "set parallel_pipeline_task_num = 1"
        }

        def runInvertedIndexCase = { String tableNamePrefix ->
            def minReaderMisses = (rowsPerTable / invertedIndexBatchRows).toLong()
            def minInvertedRemoteBytes = minReaderMisses * 1024L

            def coldTable = "${tableNamePrefix}_cold"
            createInvertedIndexTable(coldTable)
            setupInvertedIndexQuery()
            def coldQuery = "SELECT COUNT() FROM ${coldTable} WHERE body MATCH 'needle'"

            clearFileCache()
            sql "set remote_scan_no_write_file_cache_threshold_bytes = -1"
            runProfileQuery("${coldTable}_inverted_cold_write_through", coldQuery) { profileString ->
                assertInvertedIndexColdWriteThrough(
                        "${coldTable}_inverted_cold_write_through", profileString,
                        minReaderMisses, minInvertedRemoteBytes)
            }

            def thresholdTable = "${tableNamePrefix}_threshold"
            createInvertedIndexTable(thresholdTable)
            setupInvertedIndexQuery()
            def thresholdQuery = "SELECT COUNT() FROM ${thresholdTable} WHERE body MATCH 'needle'"

            clearFileCache()
            def invertedThresholdBytes = cacheBlockSize
            sql "set remote_scan_no_write_file_cache_threshold_bytes = ${invertedThresholdBytes}"
            runProfileQuery("${thresholdTable}_inverted_threshold_one_block",
                    thresholdQuery) { profileString ->
                assertInvertedIndexThresholdRemoteOnly(
                        "${thresholdTable}_inverted_threshold_one_block", profileString,
                        invertedThresholdBytes, minReaderMisses, minInvertedRemoteBytes)
            }

            def cachedSearcherTable = "${tableNamePrefix}_sch_hit"
            createInvertedIndexTable(cachedSearcherTable)
            setupInvertedIndexQuery()
            def buildSearcherQuery =
                    "SELECT COUNT() FROM ${cachedSearcherTable} WHERE body MATCH 'needle'"
            def cachedSearcherQuery =
                    "SELECT COUNT() FROM ${cachedSearcherTable} WHERE body MATCH 'group1'"

            clearFileCache()
            sql "set remote_scan_no_write_file_cache_threshold_bytes = -1"
            runProfileQuery("${cachedSearcherTable}_build_searcher_cache",
                    buildSearcherQuery) { profileString ->
                def searcherCacheMiss =
                        sumProfileCounter(profileString, "InvertedIndexSearcherCacheMiss")
                assert searcherCacheMiss >= minReaderMisses :
                        "${cachedSearcherTable}: first query should build searcher cache, " +
                        "profile=${profileString}"
            }

            clearFileCache()
            def cachedSearcherThresholdBytes = 0L
            sql "set remote_scan_no_write_file_cache_threshold_bytes = ${cachedSearcherThresholdBytes}"
            runProfileQuery("${cachedSearcherTable}_inverted_searcher_cache_hit_zero_threshold",
                    cachedSearcherQuery) { profileString ->
                assertInvertedIndexSearcherCacheHitThresholdRemoteOnly(
                        "${cachedSearcherTable}_inverted_searcher_cache_hit_zero_threshold",
                        profileString, cachedSearcherThresholdBytes, minReaderMisses,
                        minInvertedRemoteBytes)
            }
        }

        runCase("remote_scan_no_write_file_cache_single", 1, 1L, 1L, false)
        runCase("remote_scan_no_write_file_cache_multi", 8, 8L, 4L, true)
        runInvertedIndexCase("remote_scan_no_write_file_cache_inverted_index")
    }
}
