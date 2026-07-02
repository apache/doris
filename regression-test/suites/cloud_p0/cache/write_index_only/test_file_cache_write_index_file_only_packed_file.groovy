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

suite("test_file_cache_write_index_file_only_packed_file", "docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)

    options.beConfigs += [
        'enable_file_cache=true',
        'enable_file_cache_write_index_file_only=true',
        'enable_file_cache_adaptive_write=true',
        'enable_file_cache_keep_base_compaction_output=true',
        'file_cache_keep_base_compaction_output_min_hit_ratio=0',
        'enable_flush_file_cache_async=false',
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'enable_evict_file_cache_in_advance=false',
        'enable_packed_file=true',
        'small_file_threshold_bytes=104857600',
        'packed_file_size_threshold_bytes=104857600',
        'file_cache_each_block_size=4096',
        'file_cache_path=[{"path":"/opt/apache-doris/be/storage/file_cache","total_size":83886080,"query_limit":83886080}]'
    ]

    def tableName = "test_file_cache_write_index_file_only_packed_file"
    def loadRows = 1200
    def compactionBatchRows = 200
    def indexTokenPrefix = "tok"
    def backendIdToIp = [:]
    def backendIdToHttpPort = [:]
    def backendIdToBrpcPort = [:]

    def clearFileCache = { beHost, beHttpPort ->
        def result = Http.GET("http://${beHost}:${beHttpPort}/api/file_cache?op=clear&sync=true", true)
        logger.info("clear file cache result: ${result}")
    }

    def uniqueIndexToken = { int seed ->
        def alphabet = "abcdefghijklmnopqrstuvwxyz"
        def value = seed
        def suffix = new StringBuilder()
        for (int i = 0; i < 5; i++) {
            suffix.append(alphabet.charAt(value % alphabet.length()))
            value = (int) (value / alphabet.length())
        }
        return "${indexTokenPrefix}${suffix}"
    }

    def insertRows = { int batch, int rowCount ->
        def data = new StringBuilder()
        (0..<rowCount).each { idx ->
            def payload = (1..24).collect { java.util.UUID.randomUUID().toString() }.join("")
            def indexToken = uniqueIndexToken(batch * 100000 + idx)
            data.append("${batch * 100000 + idx + 1}\t")
            data.append("${rowCount - idx}\t")
            data.append("tag_${batch}_${idx}\t")
            data.append("quick brown profile text ${indexToken} row ${batch} ${idx}\t")
            data.append("${payload}\n")
        }
        streamLoad {
            table "${tableName}"
            set 'column_separator', '\t'
            set 'columns', 'id,sort_key,tag,body,payload'
            inputText data.toString()
            time 60000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                logger.info("stream load result: ${result}")
                def json = parseJson(result)
                assert json.Status.toString().equalsIgnoreCase("success") : result
                assert json.NumberLoadedRows.toString().toInteger() == rowCount : result
            }
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

    def sumProfileCounter = { String profileString, String counterName ->
        long total = 0
        def matcher = (profileString =~ ("(?m)^\\s*(?:-\\s*)?" +
                java.util.regex.Pattern.quote(counterName) + ":\\s+([^\\n]+)"))
        while (matcher.find()) {
            total += parseProfileCounterValue(matcher.group(1).toString())
        }
        return total
    }

    def sumProfileSummaryCounter = { String profileString, String counterName ->
        long total = 0
        def counterRegex = counterName.split(/\s+/).collect {
            java.util.regex.Pattern.quote(it)
        }.join("\\s+")
        def matcher = (profileString =~ ("(?m)^\\s*(?:-\\s*)?" + counterRegex + ":\\s+([^\\n]+)"))
        while (matcher.find()) {
            total += parseProfileCounterValue(matcher.group(1).toString())
        }
        return total
    }

    def assertIndexOnlyProfile = { String profileString, String label,
            boolean requireSegmentFooterCacheBytes, boolean requireDataPageRemoteBytes ->
        def invertedIndexCacheBytes =
                sumProfileCounter(profileString, "InvertedIndexBytesScannedFromCache")
        def invertedIndexRemoteBytes =
                sumProfileCounter(profileString, "InvertedIndexBytesScannedFromRemote")
        def segmentFooterIndexCacheBytes =
                sumProfileCounter(profileString, "SegmentFooterIndexBytesScannedFromCache")
        def segmentFooterIndexRemoteBytes =
                sumProfileCounter(profileString, "SegmentFooterIndexBytesScannedFromRemote")
        def totalCacheBytes = sumProfileCounter(profileString, "BytesScannedFromCache")
        def totalRemoteBytes = sumProfileCounter(profileString, "BytesScannedFromRemote")
        def parallelFragmentExecInstanceNum =
                sumProfileSummaryCounter(profileString, "Parallel Fragment Exec Instance Num")
        def totalInstancesNum = sumProfileSummaryCounter(profileString, "Total Instances Num")
        def classifiedRemoteBytes = invertedIndexRemoteBytes + segmentFooterIndexRemoteBytes
        def dataPageRemoteBytes = totalRemoteBytes - classifiedRemoteBytes

        logger.info("${label} profile counters: invertedIndexCacheBytes=${invertedIndexCacheBytes}, " +
                "invertedIndexRemoteBytes=${invertedIndexRemoteBytes}, " +
                "segmentFooterIndexCacheBytes=${segmentFooterIndexCacheBytes}, " +
                "segmentFooterIndexRemoteBytes=${segmentFooterIndexRemoteBytes}, " +
                "parallelFragmentExecInstanceNum=${parallelFragmentExecInstanceNum}, " +
                "totalInstancesNum=${totalInstancesNum}, totalCacheBytes=${totalCacheBytes}, " +
                "totalRemoteBytes=${totalRemoteBytes}, dataPageRemoteBytes=${dataPageRemoteBytes}")

        assert parallelFragmentExecInstanceNum == 1L :
                "${label}: expected profile query to use one parallel fragment exec instance, profile=${profileString}"
        assert invertedIndexCacheBytes > 0 :
                "${label}: expected independent inverted index file to be read from local file cache, profile=${profileString}"
        assert invertedIndexCacheBytes > 4096 :
                "${label}: expected query to read more than the initial inverted index buffer, profile=${profileString}"
        assert invertedIndexRemoteBytes == 0L :
                "${label}: independent inverted index should not be read from remote storage, profile=${profileString}"
        if (requireSegmentFooterCacheBytes) {
            assert segmentFooterIndexCacheBytes > 0 :
                    "${label}: expected segment footer/index to be read from local file cache, profile=${profileString}"
        }
        assert segmentFooterIndexRemoteBytes == 0L :
                "${label}: segment footer/index should not be read from remote storage, profile=${profileString}"
        if (requireDataPageRemoteBytes) {
            assert totalRemoteBytes > classifiedRemoteBytes :
                    "${label}: expected ordinary data pages to be read from remote storage, " +
                    "totalRemoteBytes=${totalRemoteBytes}, classifiedRemoteBytes=" +
                    "${classifiedRemoteBytes}, dataPageRemoteBytes=${dataPageRemoteBytes}, " +
                    "profile=${profileString}"
        }
        // File cache is block-aligned, so footer/index cache ranges may include nearby data pages.
        // Only assert data-page remote reads for stages where the query reaches uncached data pages.
    }

    def logRowsetsLayout = { String label ->
        def currentTablets = sql_return_maparray """ SHOW TABLETS FROM ${tableName} """
        assert currentTablets.size() == 1
        def currentTablet = currentTablets[0]
        def tabletStatus = show_tablet_compaction(currentTablet)
        def rowsets = tabletStatus.rowsets == null ? [] : tabletStatus.rowsets
        def staleRowsets = tabletStatus.stale_rowsets == null ? [] : tabletStatus.stale_rowsets
        logger.info("${label} rowsets layout: tabletId=${currentTablet.TabletId}, " +
                "rowsetCount=${rowsets.size()}, staleRowsetCount=${staleRowsets.size()}, " +
                "rowsets=${rowsets}, staleRowsets=${staleRowsets}")
    }

    docker(options) {
        getBackendIpHttpAndBrpcPort(backendIdToIp, backendIdToHttpPort, backendIdToBrpcPort)

        sql """ DROP TABLE IF EXISTS ${tableName} FORCE """
        sql """
            CREATE TABLE ${tableName} (
                id INT,
                sort_key INT,
                tag VARCHAR(64),
                body VARCHAR(2048),
                payload STRING,
                INDEX body_idx(body) USING INVERTED PROPERTIES("parser" = "english") COMMENT ''
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "V2"
            )
        """

        def tablets = sql_return_maparray """ SHOW TABLETS FROM ${tableName} """
        assert tablets.size() == 1
        def tablet = tablets[0]
        def beHost = backendIdToIp[tablet.BackendId]
        def beHttpPort = backendIdToHttpPort[tablet.BackendId]

        clearFileCache(beHost, beHttpPort)
        insertRows(1, loadRows)
        sql """ SYNC """

        def loadProfileTag = "file_cache_write_index_only_packed_file_load_profile"
        def loadProfileChecked = false
        profile(loadProfileTag) {
            sql """ SET enable_profile = true """
            sql """ SET profile_level = 2 """
            sql """ SET parallel_pipeline_task_num = 1 """
            sql """ SET inverted_index_max_expansions = 4096 """
            run {
                logRowsetsLayout("before packed file load query")
                sql """ /* ${loadProfileTag} */ SELECT id + 1 FROM ${tableName} WHERE body MATCH_REGEXP '^${indexTokenPrefix}.*' ORDER BY sort_key LIMIT 10 """
                sleep(500)
            }
            check { profileString, exception ->
                loadProfileChecked = true
                if (exception != null) {
                    throw exception
                }
                logger.info("profile snippet: {}", profileString.take(3000))
                assertIndexOnlyProfile(profileString, "packed file load", true, true)
            }
        }
        assert loadProfileChecked : "packed file load profile check was not executed"

        (2..3).each { batch ->
            insertRows(batch, compactionBatchRows)
        }
        sql """ SYNC """

        clearFileCache(beHost, beHttpPort)
        trigger_and_wait_compaction(tableName, "full")

        def compactionProfileTag = "file_cache_write_index_only_packed_file_compaction_profile"
        def compactionProfileChecked = false
        profile(compactionProfileTag) {
            sql """ SET enable_profile = true """
            sql """ SET profile_level = 2 """
            sql """ SET parallel_pipeline_task_num = 1 """
            sql """ SET inverted_index_max_expansions = 4096 """
            run {
                logRowsetsLayout("before packed file full compaction query")
                sql """
                    /* ${compactionProfileTag} */
                    SELECT COUNT(*), SUM(id), SUM(sort_key), SUM(LENGTH(payload))
                    FROM ${tableName}
                    WHERE body MATCH_REGEXP '^${indexTokenPrefix}.*'
                """
                sleep(500)
            }
            check { profileString, exception ->
                compactionProfileChecked = true
                if (exception != null) {
                    throw exception
                }
                logger.info("profile snippet: {}", profileString.take(3000))
                assertIndexOnlyProfile(profileString, "packed file full compaction", false, true)
            }
        }
        assert compactionProfileChecked : "packed file full compaction profile check was not executed"
    }
}
