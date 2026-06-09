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

suite("test_file_cache_query_limit_segment_meta_profile", "docker") {
    def cacheBlockSize = 262144L
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)
    options.msNum = 1
    options.beConfigs += [
        "enable_file_cache=true",
        "enable_read_cache_file_directly=false",
        "disable_storage_page_cache=true",
        "disable_segment_cache=true",
        "enable_java_support=false",
        "enable_evict_file_cache_in_advance=false",
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
        def chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

        def clearFileCache = {
            def result = Http.GET("http://${beHost}:${beHttpPort}/api/file_cache?op=clear&sync=true", true)
            logger.info("clear file cache result: ${result}")
        }

        def getBeParam = { String paramName ->
            def (code, out, err) = curl(
                    "GET", String.format("http://%s:%s/api/show_config?conf_item=%s",
                    beHost, beHttpPort, paramName))
            assert code == 0 : "show_config ${paramName} failed, out=${out}, err=${err}"
            assert out.contains(paramName) : "show_config ${paramName} missing item, out=${out}"
            def resultList = parseJson(out)[0]
            assert resultList.size() == 4 : "unexpected show_config result=${out}"
            def paramValue = resultList[2].toString()
            logger.info("BE config ${paramName} current value: ${paramValue}")
            return paramValue
        }

        def setBeParam = { String paramName, String paramValue ->
            logger.info("set BE config ${paramName}=${paramValue}")
            def (code, out, err) = curl(
                    "POST", String.format("http://%s:%s/api/update_config?%s=%s",
                    beHost, beHttpPort, paramName, paramValue))
            assert code == 0 && out.contains("OK") :
                    "update_config ${paramName}=${paramValue} failed, out=${out}, err=${err}"
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

        def createTableAndLoad = { String tableName, boolean verifyRowCount = true ->
            sql "DROP TABLE IF EXISTS ${tableName} FORCE"
            sql """
                CREATE TABLE ${tableName} (
                    id INT NOT NULL,
                    group_id INT NOT NULL,
                    payload VARCHAR(4096) NOT NULL,
                    pad VARCHAR(4096) NOT NULL
                ) ENGINE=OLAP
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "disable_auto_compaction" = "true"
                )
            """
            insertRows(tableName)

            if (verifyRowCount) {
                def rowCount = sql "SELECT COUNT(*) FROM ${tableName}"
                assert rowCount[0][0] == rowsPerTable
            }
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

        def profileCounterValues = { String profileString, String counterName,
                String aggregateName = null ->
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

        def collectFileCacheCounters = { String label, String profileString ->
            def counters = [
                remoteOnlyTriggered: maxProfileCounter(profileString, "RemoteOnlyOnMissTriggered"),
                thresholdMetricBytes: maxProfileCounter(profileString, "RemoteOnlyOnMissThresholdBytes"),
                skipCacheIo: sumProfileCounter(profileString, "NumSkipCacheIOTotal"),
                writeCacheBytes: sumProfileCounter(profileString, "BytesWriteIntoCache"),
                segmentMetaWriteCacheBytes:
                        sumProfileCounter(profileString, "SegmentFooterIndexBytesWriteIntoCache"),
                segmentMetaRemoteIo:
                        sumProfileCounter(profileString, "SegmentFooterIndexNumRemoteIOTotal"),
                scannerNum: sumProfileCounter(profileString, "NumScanners")
            ]
            logger.info("${label} file-cache counters: ${counters}")
            return counters
        }

        def setupQuerySession = { long thresholdBytes ->
            sql "set enable_profile = true"
            sql "set profile_level = 2"
            sql "set enable_sql_cache = false"
            sql "set enable_query_cache = false"
            sql "set enable_file_cache = false"
            sql "set enable_page_cache = false"
            sql "set enable_segment_cache = false"
            sql "set parallel_fragment_exec_instance_num = 1"
            sql "set enable_parallel_scan = true"
            sql "set parallel_scan_max_scanners_count = 1"
            sql "set parallel_scan_min_rows_per_scanner = 1024"
            if (supportMaxScannersConcurrency) {
                sql "set max_scanners_concurrency = 1"
            }
            sql "set parallel_pipeline_task_num = 1"
            sql "set file_cache_query_limit_bytes = ${thresholdBytes}"
            logger.info("query file_cache_query_limit_bytes set to ${thresholdBytes}")
        }

        def setupParallelPreloadQuerySession = { long thresholdBytes ->
            setupQuerySession(thresholdBytes)
            sql "set parallel_scan_max_scanners_count = 4"
            sql "set parallel_scan_min_rows_per_scanner = 1024"
            if (supportMaxScannersConcurrency) {
                sql "set max_scanners_concurrency = 4"
            }
            sql "set parallel_pipeline_task_num = 4"
        }

        def runProfileQuery = { String name, String query, long thresholdBytes ->
            def counters = null
            logger.info("run profile query ${name}, thresholdBytes=${thresholdBytes}, sql=${query}")
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
                    counters = collectFileCacheCounters(name, profileString)
                    if (thresholdBytes >= 0) {
                        assert counters.thresholdMetricBytes == thresholdBytes :
                                "${name}: expected threshold bytes ${thresholdBytes}, " +
                                "profile=${profileString}"
                    }
                }
            }
            assert counters != null : "${name}: profile check was not executed"
            return counters
        }

        def runCase = { String tableName, boolean segmentMetaCounts, long thresholdBytes ->
            setBeParam("file_cache_query_limit_segment_meta", segmentMetaCounts.toString())
            createTableAndLoad(tableName, false)
            setupQuerySession(thresholdBytes)
            clearFileCache()
            def query = "SELECT SUM(LENGTH(payload) + LENGTH(pad)) FROM ${tableName}"
            def counters = runProfileQuery(tableName, query, thresholdBytes)
            logger.info("case ${tableName} finished, segmentMetaCounts=${segmentMetaCounts}, " +
                    "thresholdBytes=${thresholdBytes}, counters=${counters}")
            return counters
        }

        def runParallelPreloadCase = { String tableName, boolean segmentMetaCounts ->
            long tinyThresholdBytes = 1L
            createTableAndLoad(tableName, false)
            setBeParam("file_cache_query_limit_segment_meta", segmentMetaCounts.toString())
            setupParallelPreloadQuerySession(tinyThresholdBytes)
            clearFileCache()
            def query = "SELECT SUM(id + group_id + LENGTH(payload)) FROM ${tableName} " +
                    "WHERE group_id >= 0"
            def counters = runProfileQuery(tableName, query, tinyThresholdBytes)
            assert counters.scannerNum > 1L :
                    "parallel preload case should use multiple scanners, counters=${counters}"
            assert counters.remoteOnlyTriggered == 1L :
                    "tiny threshold should trigger remote-only-on-miss, counters=${counters}"
            assert counters.skipCacheIo > 0L :
                    "tiny threshold should skip later cache writes, counters=${counters}"
            logger.info("parallel preload case ${tableName} finished, " +
                    "segmentMetaCounts=${segmentMetaCounts}, counters=${counters}")
            return counters
        }

        def originalSegmentMetaConfig = getBeParam("file_cache_query_limit_segment_meta")
        logger.info("original file_cache_query_limit_segment_meta=${originalSegmentMetaConfig}")
        assert getBeParam("enable_read_cache_file_directly").equalsIgnoreCase("false")
        try {
            def baseline = runCase("file_cache_limit_segment_meta_baseline", false, -1L)
            assert baseline.remoteOnlyTriggered == 0L :
                    "baseline should not trigger remote-only-on-miss, counters=${baseline}"
            assert baseline.skipCacheIo == 0L :
                    "baseline should not skip cache IO, counters=${baseline}"
            assert baseline.segmentMetaWriteCacheBytes > 0L :
                    "baseline should write segment footer/meta file cache, counters=${baseline}"
            assert baseline.writeCacheBytes > baseline.segmentMetaWriteCacheBytes :
                    "baseline query should also write data pages, counters=${baseline}"

            long thresholdBytes = Math.max(cacheBlockSize, baseline.segmentMetaWriteCacheBytes)
            logger.info("derived positive query limit threshold: thresholdBytes=${thresholdBytes}, " +
                    "cacheBlockSize=${cacheBlockSize}, " +
                    "baselineSegmentMetaWriteCacheBytes=${baseline.segmentMetaWriteCacheBytes}, " +
                    "baselineWriteCacheBytes=${baseline.writeCacheBytes}")
            assert baseline.writeCacheBytes > thresholdBytes :
                    "baseline write bytes should exceed derived threshold, " +
                    "thresholdBytes=${thresholdBytes}, counters=${baseline}"

            def withoutSegmentMeta = runCase(
                    "file_cache_limit_segment_meta_not_counted", false, thresholdBytes)
            assert withoutSegmentMeta.remoteOnlyTriggered == 1L :
                    "threshold should still trigger on data cache writes, counters=${withoutSegmentMeta}"
            assert withoutSegmentMeta.skipCacheIo > 0L :
                    "threshold should skip later cache writes, counters=${withoutSegmentMeta}"
            assert withoutSegmentMeta.segmentMetaWriteCacheBytes > 0L :
                    "segment footer/meta should write cache before data threshold triggers, " +
                    "counters=${withoutSegmentMeta}"
            assert withoutSegmentMeta.writeCacheBytes > thresholdBytes :
                    "when segment footer/meta is not counted, total profile writes can exceed " +
                    "the query threshold by those bytes, thresholdBytes=${thresholdBytes}, " +
                    "counters=${withoutSegmentMeta}"
            logger.info("segment meta not counted result: thresholdBytes=${thresholdBytes}, " +
                    "writeCacheBytes=${withoutSegmentMeta.writeCacheBytes}, " +
                    "segmentMetaWriteCacheBytes=${withoutSegmentMeta.segmentMetaWriteCacheBytes}, " +
                    "writeMinusThreshold=${withoutSegmentMeta.writeCacheBytes - thresholdBytes}, " +
                    "remoteOnlyTriggered=${withoutSegmentMeta.remoteOnlyTriggered}, " +
                    "skipCacheIo=${withoutSegmentMeta.skipCacheIo}")

            def withSegmentMeta = runCase(
                    "file_cache_limit_segment_meta_counted", true, thresholdBytes)
            assert withSegmentMeta.remoteOnlyTriggered == 1L :
                    "counted segment footer/meta should trigger remote-only-on-miss, " +
                    "counters=${withSegmentMeta}"
            assert withSegmentMeta.skipCacheIo > 0L :
                    "counted segment footer/meta should make later misses skip cache writes, " +
                    "counters=${withSegmentMeta}"
            assert withSegmentMeta.segmentMetaWriteCacheBytes > 0L :
                    "counted segment footer/meta should still have admitted cache writes, " +
                    "counters=${withSegmentMeta}"
            assert withSegmentMeta.writeCacheBytes <= thresholdBytes :
                    "when segment footer/meta is counted, total admitted profile writes should " +
                    "respect the query threshold, thresholdBytes=${thresholdBytes}, " +
                    "counters=${withSegmentMeta}"
            logger.info("segment meta counted result: thresholdBytes=${thresholdBytes}, " +
                    "writeCacheBytes=${withSegmentMeta.writeCacheBytes}, " +
                    "segmentMetaWriteCacheBytes=${withSegmentMeta.segmentMetaWriteCacheBytes}, " +
                    "thresholdMinusWrite=${thresholdBytes - withSegmentMeta.writeCacheBytes}, " +
                    "remoteOnlyTriggered=${withSegmentMeta.remoteOnlyTriggered}, " +
                    "skipCacheIo=${withSegmentMeta.skipCacheIo}")

            long tinyThresholdBytes = 1L
            def tinyWithoutSegmentMeta = runCase(
                    "file_cache_limit_segment_meta_tiny_not_counted", false, tinyThresholdBytes)
            assert tinyWithoutSegmentMeta.remoteOnlyTriggered == 1L :
                    "tiny threshold should trigger remote-only-on-miss, " +
                    "counters=${tinyWithoutSegmentMeta}"
            assert tinyWithoutSegmentMeta.skipCacheIo > 0L :
                    "tiny threshold should skip data cache writes, " +
                    "counters=${tinyWithoutSegmentMeta}"
            assert tinyWithoutSegmentMeta.segmentMetaWriteCacheBytes > 0L :
                    "when segment footer/meta is not counted, tiny threshold should still allow " +
                    "segment footer/meta cache writes, counters=${tinyWithoutSegmentMeta}"
            assert tinyWithoutSegmentMeta.writeCacheBytes > 0L :
                    "when segment footer/meta is not counted, total profile writes should be " +
                    "greater than zero, counters=${tinyWithoutSegmentMeta}"
            logger.info("tiny threshold segment meta not counted result: " +
                    "thresholdBytes=${tinyThresholdBytes}, " +
                    "writeCacheBytes=${tinyWithoutSegmentMeta.writeCacheBytes}, " +
                    "segmentMetaWriteCacheBytes=" +
                    "${tinyWithoutSegmentMeta.segmentMetaWriteCacheBytes}, " +
                    "remoteOnlyTriggered=${tinyWithoutSegmentMeta.remoteOnlyTriggered}, " +
                    "skipCacheIo=${tinyWithoutSegmentMeta.skipCacheIo}")

            def tinyWithSegmentMeta = runCase(
                    "file_cache_limit_segment_meta_tiny_counted", true, tinyThresholdBytes)
            assert tinyWithSegmentMeta.remoteOnlyTriggered == 1L :
                    "tiny threshold should trigger remote-only-on-miss when segment footer/meta " +
                    "is counted, counters=${tinyWithSegmentMeta}"
            assert tinyWithSegmentMeta.skipCacheIo > 0L :
                    "tiny threshold should skip cache writes when segment footer/meta is counted, " +
                    "counters=${tinyWithSegmentMeta}"
            assert tinyWithSegmentMeta.segmentMetaWriteCacheBytes == 0L :
                    "when segment footer/meta is counted, tiny threshold should block segment " +
                    "footer/meta cache writes, counters=${tinyWithSegmentMeta}"
            assert tinyWithSegmentMeta.writeCacheBytes == 0L :
                    "when segment footer/meta is counted, tiny threshold should block all " +
                    "profile cache writes, counters=${tinyWithSegmentMeta}"
            logger.info("tiny threshold segment meta counted result: " +
                    "thresholdBytes=${tinyThresholdBytes}, " +
                    "writeCacheBytes=${tinyWithSegmentMeta.writeCacheBytes}, " +
                    "segmentMetaWriteCacheBytes=${tinyWithSegmentMeta.segmentMetaWriteCacheBytes}, " +
                    "remoteOnlyTriggered=${tinyWithSegmentMeta.remoteOnlyTriggered}, " +
                    "skipCacheIo=${tinyWithSegmentMeta.skipCacheIo}")

            def preloadWithoutSegmentMeta = runParallelPreloadCase(
                    "file_cache_limit_segment_meta_preload_not_counted", false)
            assert preloadWithoutSegmentMeta.segmentMetaWriteCacheBytes > 0L :
                    "without segment meta accounting, parallel preload footer/meta should still " +
                    "write cache, counters=${preloadWithoutSegmentMeta}"
            assert preloadWithoutSegmentMeta.writeCacheBytes >=
                    preloadWithoutSegmentMeta.segmentMetaWriteCacheBytes :
                    "aggregate file-cache writes should include segment footer/meta writes, " +
                    "counters=${preloadWithoutSegmentMeta}"
            logger.info("parallel preload segment meta not counted result: " +
                    "writeCacheBytes=${preloadWithoutSegmentMeta.writeCacheBytes}, " +
                    "segmentMetaWriteCacheBytes=" +
                    "${preloadWithoutSegmentMeta.segmentMetaWriteCacheBytes}, " +
                    "scannerNum=${preloadWithoutSegmentMeta.scannerNum}")

            def preloadWithSegmentMeta = runParallelPreloadCase(
                    "file_cache_limit_segment_meta_preload_counted", true)
            assert preloadWithSegmentMeta.segmentMetaWriteCacheBytes == 0L :
                    "when segment footer/meta is counted, tiny threshold should block " +
                    "parallel preload footer/meta cache writes, " +
                    "counters=${preloadWithSegmentMeta}"
            assert preloadWithSegmentMeta.writeCacheBytes == 0L :
                    "when segment footer/meta is counted, tiny threshold should block all " +
                    "profile cache writes in the parallel preload query, " +
                    "counters=${preloadWithSegmentMeta}"
            logger.info("parallel preload segment meta counted result: " +
                    "writeCacheBytes=${preloadWithSegmentMeta.writeCacheBytes}, " +
                    "segmentMetaWriteCacheBytes=${preloadWithSegmentMeta.segmentMetaWriteCacheBytes}, " +
                    "scannerNum=${preloadWithSegmentMeta.scannerNum}")
        } finally {
            logger.info("restore file_cache_query_limit_segment_meta=${originalSegmentMetaConfig}")
            setBeParam("file_cache_query_limit_segment_meta", originalSegmentMetaConfig)
            sql "set file_cache_query_limit_bytes = -1"
        }
    }
}
