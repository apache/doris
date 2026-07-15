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

suite("test_async_file_cache_write", "docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)
    options.msNum = 1
    options.beConfigs += [
        "enable_file_cache=true",
        "enable_async_file_cache_write=true",
        "enable_inflight_write_buffer_index=true",
        "enable_read_cache_file_directly=false",
        "enable_cache_read_from_peer=false",
        "disable_storage_page_cache=true",
        "disable_segment_cache=true",
        "enable_evict_file_cache_in_advance=false",
        "file_cache_each_block_size=262144",
        "file_cache_enter_disk_resource_limit_mode_percent=99",
        "file_cache_path=[{\"path\":\"/opt/apache-doris/be/storage/file_cache\",\"total_size\":134217728,\"query_limit\":134217728}]"
    ]

    docker(options) {
        def clusters = sql "SHOW CLUSTERS"
        assertFalse(clusters.isEmpty())
        sql "use @${clusters[0][0]}"

        def backends = sql "SHOW BACKENDS"
        assertEquals(backends.size(), 1)
        def beHost = backends[0][1]
        def beHttpPort = backends[0][4]
        def beBrpcPort = backends[0][5]

        def clearFileCache = {
            def (code, out, err) = curl(
                    "GET", "http://${beHost}:${beHttpPort}/api/file_cache?op=clear&sync=true")
            assertTrue(code == 0 && out.contains("OK"),
                    "clear file cache failed, out=${out}, err=${err}")
        }

        def readMetric = { String metricSuffix ->
            def metrics = new URL("http://${beHost}:${beBrpcPort}/brpc_metrics").text
            long total = 0
            metrics.eachLine { String line ->
                if (!line.startsWith("#") && line.contains(metricSuffix)) {
                    def matcher = line =~ ~".*${metricSuffix}\\s+([0-9]+)\\s*"
                    if (matcher.matches()) {
                        total += matcher[0][1] as long
                    }
                }
            }
            return total
        }

        def setBeConfig = { String name, String value ->
            def (code, out, err) = curl(
                    "POST", "http://${beHost}:${beHttpPort}/api/update_config?${name}=${value}")
            assertTrue(code == 0 && out.contains("OK"),
                    "update_config ${name}=${value} failed, out=${out}, err=${err}")
        }

        def waitForPendingWrites = {
            long deadline = System.currentTimeMillis() + 30000L
            while (System.currentTimeMillis() < deadline &&
                    readMetric("async_cache_write_pending_count") != 0L) {
                sleep(100)
            }
            assertEquals(readMetric("async_cache_write_pending_count"), 0L)
        }

        sql "DROP TABLE IF EXISTS test_async_file_cache_write_phase1"
        sql """
            CREATE TABLE test_async_file_cache_write_phase1 (
                k BIGINT NOT NULL,
                v STRING NOT NULL
            )
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
            )
        """
        sql """
            INSERT INTO test_async_file_cache_write_phase1
            SELECT number, repeat(md5(cast(number AS STRING)), 16)
            FROM numbers("number" = "32768")
        """
        sql "SYNC"

        clearFileCache()
        long submittedBefore = readMetric("async_cache_write_submitted_total")
        order_qt_async_file_cache_first """
            SELECT sum(k), sum(length(v))
            FROM test_async_file_cache_write_phase1
        """
        long submittedAfter = readMetric("async_cache_write_submitted_total")
        assertTrue(submittedAfter > submittedBefore,
                "cold query should submit async cache writes: before=${submittedBefore}, " +
                        "after=${submittedAfter}")

        waitForPendingWrites()
        long probeHitBefore = readMetric("cached_remote_file_reader_probe_hit_downloaded_total")
        order_qt_async_file_cache_second """
            SELECT sum(k), sum(length(v))
            FROM test_async_file_cache_write_phase1
        """
        long probeHitAfter = readMetric("cached_remote_file_reader_probe_hit_downloaded_total")
        assertTrue(probeHitAfter > probeHitBefore,
                "second query should read downloaded cache blocks: before=${probeHitBefore}, " +
                        "after=${probeHitAfter}")

        setBeConfig("enable_async_file_cache_write", "false")
        clearFileCache()
        submittedBefore = readMetric("async_cache_write_submitted_total")
        order_qt_async_file_cache_sync_fallback """
            SELECT sum(k), sum(length(v))
            FROM test_async_file_cache_write_phase1
        """
        submittedAfter = readMetric("async_cache_write_submitted_total")
        assertEquals(submittedAfter, submittedBefore)
    }
}
