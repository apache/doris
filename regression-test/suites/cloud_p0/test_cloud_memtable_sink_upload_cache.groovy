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

suite("test_cloud_memtable_sink_upload_cache", "p0, docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(3)
    options.enableDebugPoints()
    options.feConfigs += ['cloud_stream_load_default_memtable_sink_upload=true']
    options.beConfigs += ['enable_file_cache=true', 'small_file_threshold_bytes=1048576']
    docker(options) {
        sql "DROP TABLE IF EXISTS cloud_memtable_sink_upload_cache"
        sql """
            CREATE TABLE cloud_memtable_sink_upload_cache (
                k BIGINT NOT NULL, v BIGINT, INDEX idx_k(k) USING INVERTED
            ) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num"="1", "disable_auto_compaction"="true",
                        "inverted_index_storage_format"="V2")
        """
        def tablet = sql_return_maparray("SHOW TABLETS FROM cloud_memtable_sink_upload_cache")[0]
        def backends = sql_return_maparray("SHOW BACKENDS")
        def target = backends.find { it.BackendId == tablet.BackendId }
        def remote = backends.find { it.BackendId != tablet.BackendId }
        def ms = cluster.getAllMetaservices()[0]
        def cached = { be, file ->
            !Http.GET("http://${be.Host}:${be.HttpPort}/api/file_cache?op=list_cache&value=${file}", true).isEmpty()
        }
        def warmupStats = {
            Http.GET("http://${target.Host}:${target.HttpPort}/api/warmup_event_driven_stats", true)
                    .data.find { it.job_id == 0 }
        }
        def skipWarmup = "FileCacheBlockDownloader::download_segment_file.skip_warmup"
        GetDebugPoint().enableDebugPointForAllBEs("LoadStreamWriter.append_data.unexpected_transfer")
        try {
            [false, true].each { packed ->
                setBeConfigTemporary(['enable_packed_file': packed.toString()]) {
                    [false, true].each { local ->
                        def sink = local ? target : remote
                        def tag = "${packed ? 'packed' : 'plain'}_${local ? 'local' : 'remote'}"
                        def before = warmupStats()
                        long finishedSegments = before?.finish?.seg?.num?.get('1h') ?: 0
                        long finishedIndexes = before?.finish?.idx?.num?.get('1h') ?: 0
                        // For the same-BE case, cache must come from uploading, even without warmup.
                        if (local) {
                            GetDebugPoint().enableDebugPointForAllBEs(skipWarmup)
                        }
                        try {
                            streamLoad {
                                table "cloud_memtable_sink_upload_cache"
                                directToBe sink.Host, sink.HttpPort as int
                                set "column_separator", ","
                                set "memtable_on_sink_node", "true"
                                set "group_commit", "off_mode"
                                inputStream new ByteArrayInputStream("1,10\n2,20\n".getBytes())
                                check { result, exception, startTime, endTime ->
                                    if (exception != null) { throw exception }
                                    def response = parseJson(result)
                                    quickTest("load_${tag}", """
                                        SELECT '${response.Status}', ${response.NumberLoadedRows},
                                            ${response.NumberFilteredRows}
                                    """, true)
                                }
                            }
                            def partition = sql_return_maparray(
                                    "SHOW PARTITIONS FROM cloud_memtable_sink_upload_cache")[0]
                            def meta
                            getSegmentFilesFromMs("${ms.host}:${ms.httpPort}", tablet.TabletId,
                                    partition.VisibleVersion) { code, body ->
                                assertEquals(200, code)
                                meta = parseJson(body)
                            }
                            def ids = meta.segment_ids
                            quickTest("layout_${tag}", """
                                SELECT ${meta.num_segments as int},
                                    ${(meta.packed_slice_locations ?: [:]).size() > 0}
                            """, true)
                            def files = ids.collectMany { id ->
                                ["${meta.rowset_id_v2}_${id}.dat", "${meta.rowset_id_v2}_${id}.idx"]
                            }
                            // Wait for download callbacks, then inspect cache keys before querying data.
                            // list_cache alone also lists blocks whose downloads have not completed.
                            awaitUntil(60) {
                                def stats = warmupStats()
                                stats != null &&
                                    stats.finish.seg.num['1h'] >= finishedSegments + ids.size() &&
                                    stats.finish.idx.num['1h'] >= finishedIndexes + ids.size() &&
                                    files.every { cached(target, it) }
                            }
                            if (packed) {
                                // Drain sink-side async cache writes before asserting their absence.
                                awaitUntil(60) {
                                    // BRPC serves HTML to Java's user agent, and plain text to curl.
                                    def (code, out, err) = curl("GET",
                                            "http://${sink.Host}:${sink.BrpcPort}/vars/packed_file_cache_async_write_count")
                                    assertEquals(0, code, err)
                                    out.trim().split(':')[-1].trim().toLong() == 0
                                }
                            }
                            quickTest("cache_${tag}", """
                                SELECT ${files.every { cached(target, it) }},
                                    ${files.every { !cached(remote, it) }}
                            """, true)
                        } finally {
                            if (local) {
                                GetDebugPoint().disableDebugPointForAllBEs(skipWarmup)
                            }
                        }
                    }
                }
            }
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("LoadStreamWriter.append_data.unexpected_transfer")
        }
        order_qt_rows "SELECT k, SUM(v), COUNT(*) FROM cloud_memtable_sink_upload_cache GROUP BY k"
    }
}
