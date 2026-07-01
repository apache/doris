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

suite("test_file_cache_write_index_file_only_compaction_segment_data", "docker") {
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
        'enable_packed_file=false',
        'file_cache_each_block_size=4096',
        'file_cache_path=[{"path":"/opt/apache-doris/be/storage/file_cache","total_size":83886080,"query_limit":83886080}]'
    ]

    def tableName = "test_file_cache_write_index_file_only_compaction_segment_data"
    def batchRows = 1200
    def backendIdToIp = [:]
    def backendIdToHttpPort = [:]
    def backendIdToBrpcPort = [:]

    def clearFileCache = { beHost, beHttpPort ->
        def result = Http.GET("http://${beHost}:${beHttpPort}/api/file_cache?op=clear&sync=true", true)
        logger.info("clear file cache result: ${result}")
    }

    def insertRows = { int batch, int rowCount ->
        def data = new StringBuilder()
        (0..<rowCount).each { idx ->
            def payload = (1..32).collect { java.util.UUID.randomUUID().toString() }.join("")
            def rowId = batch * 100000 + idx + 1
            data.append("${rowId}\t")
            data.append("${idx}\t")
            data.append("tag_${batch}_${idx}\t")
            data.append("scan all segment data row ${batch} ${idx}\t")
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

    def assertCompactionSegmentDataProfile = { String profileString, String label ->
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
        def dataPageRemoteBytes = totalRemoteBytes - segmentFooterIndexRemoteBytes

        logger.info("${label} profile counters: invertedIndexCacheBytes=${invertedIndexCacheBytes}, " +
                "invertedIndexRemoteBytes=${invertedIndexRemoteBytes}, " +
                "segmentFooterIndexCacheBytes=${segmentFooterIndexCacheBytes}, " +
                "segmentFooterIndexRemoteBytes=${segmentFooterIndexRemoteBytes}, " +
                "parallelFragmentExecInstanceNum=${parallelFragmentExecInstanceNum}, " +
                "totalInstancesNum=${totalInstancesNum}, totalCacheBytes=${totalCacheBytes}, " +
                "totalRemoteBytes=${totalRemoteBytes}, dataPageRemoteBytes=${dataPageRemoteBytes}")

        assert parallelFragmentExecInstanceNum == 1L :
                "${label}: expected profile query to use one parallel fragment exec instance, profile=${profileString}"
        assert invertedIndexCacheBytes == 0L :
                "${label}: table has no inverted index, profile=${profileString}"
        assert invertedIndexRemoteBytes == 0L :
                "${label}: table has no inverted index, profile=${profileString}"
        assert segmentFooterIndexRemoteBytes == 0L :
                "${label}: segment footer/index should not be read from remote storage, profile=${profileString}"
        assert dataPageRemoteBytes > 0L :
                "${label}: expected compacted segment data pages to be read from remote storage, " +
                "profile=${profileString}"
        assert totalRemoteBytes > totalCacheBytes :
                "${label}: expected compacted segment data to remain mostly uncached; " +
                "totalRemoteBytes=${totalRemoteBytes}, totalCacheBytes=${totalCacheBytes}, " +
                "profile=${profileString}"
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
                payload STRING
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
            )
        """

        def tablets = sql_return_maparray """ SHOW TABLETS FROM ${tableName} """
        assert tablets.size() == 1
        def tablet = tablets[0]
        def beHost = backendIdToIp[tablet.BackendId]
        def beHttpPort = backendIdToHttpPort[tablet.BackendId]

        clearFileCache(beHost, beHttpPort)
        (1..3).each { batch ->
            insertRows(batch, batchRows)
        }
        sql """ SYNC """

        clearFileCache(beHost, beHttpPort)
        trigger_and_wait_compaction(tableName, "full")

        def compactionProfileTag = "file_cache_write_index_only_compaction_segment_data_profile"
        def compactionProfileChecked = false
        profile(compactionProfileTag) {
            sql """ SET enable_profile = true """
            sql """ SET profile_level = 2 """
            sql """ SET parallel_pipeline_task_num = 1 """
            run {
                logRowsetsLayout("before full scan query after compaction")
                sql """
                    /* ${compactionProfileTag} */
                    SELECT COUNT(DISTINCT id), SUM(id), SUM(sort_key), SUM(LENGTH(tag)),
                           SUM(LENGTH(body)), SUM(LENGTH(payload))
                    FROM ${tableName}
                """
                sleep(500)
            }
            check { profileString, exception ->
                compactionProfileChecked = true
                if (exception != null) {
                    throw exception
                }
                logger.info("profile snippet: {}", profileString.take(3000))
                assertCompactionSegmentDataProfile(profileString, "full compaction segment data")
            }
        }
        assert compactionProfileChecked : "full compaction segment data profile check was not executed"
    }
}
