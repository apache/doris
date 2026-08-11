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

suite("test_file_cache_remote_index_byte_metrics", "docker") {

    final String remoteClusterName = "remote_metrics_cluster"
    final String invertedMetric = "doris_be_inverted_index_bytes_read_from_remote"
    final String segmentMetric = "doris_be_segment_footer_index_bytes_read_from_remote"

    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'heartbeat_interval_second=1',
        'auto_check_statistics_in_minutes=60',
        'sys_log_verbose_modules=org',
    ]
    options.beConfigs += [
        'report_tablet_interval_seconds=1',
        'schedule_sync_tablets_interval_s=18000',
        'disable_auto_compaction=true',
        'enable_file_cache=true',
        'enable_cache_read_from_peer=false',
        'enable_peer_s3_race=false',
        'enable_packed_file=false',
        'file_cache_each_block_size=4096',
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'file_cache_exit_disk_resource_limit_mode_percent=98',
        'file_cache_enter_need_evict_cache_in_advance_percent=99',
        'file_cache_exit_need_evict_cache_in_advance_percent=98',
        'JEMALLOC_CONF="percpu_arena:percpu,background_thread:true,metadata_thp:auto,muzzy_decay_ms:5000,dirty_decay_ms:5000,oversize_threshold:0,prof:false,prof_active:false,lg_prof_interval:-1,lg_extent_max_active_fit:8"',
    ]
    options.extraHosts += [
        'host.docker.internal:host-gateway',
        'metrics-test-bucket.host.docker.internal:host-gateway',
    ]
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true

    def readMetric = { String host, Object httpPort, String metricName ->
        def metricsText = Http.GET("http://${host}:${httpPort}/metrics", false, false).toString()
        def matcher = metricsText =~ ('(?m)^' + java.util.regex.Pattern.quote(metricName) + '\\s+(\\d+)$')
        assertTrue(matcher.find(), "metric not found: ${metricName}")
        return matcher.group(1).toLong()
    }

    def clearFileCache = { String host, Object httpPort ->
        def result = Http.GET("http://${host}:${httpPort}/api/file_cache?op=clear&sync=true", true, false)
        assertEquals("OK", result.status)
    }

    def findBackendByClusterName = { rows, String clusterName ->
        return rows.find { row ->
            def tag = (row.Tag ?: "").toString()
            tag.contains("\"compute_group_name\"") && tag.contains("\"${clusterName}\"")
        }
    }

    def firstInsert = (1..24).collect { i ->
        def body = (i % 6 == 0 || i % 7 == 0) ?
                "quick brown profile needlequick row ${i}" :
                "quick brown profile ordinarytoken row ${i}"
        return "(${i}, ${200 - i}, 'title_${i}', '${body}', 'payload_${i}_abcdefghijklmnopqrstuvwxyz')"
    }.join(",\n")

    def secondInsert = (25..48).collect { i ->
        def body = (i % 6 == 0 || i % 7 == 0) ?
                "quick brown profile needlequick row ${i}" :
                "quick brown profile ordinarytoken row ${i}"
        return "(${i}, ${200 - i}, 'title_${i}', '${body}', 'payload_${i}_abcdefghijklmnopqrstuvwxyz')"
    }.join(",\n")

    docker(options) {
        def tableName = "test_file_cache_remote_index_byte_metrics_tbl"

        sql "use @compute_cluster"
        sql """ DROP TABLE IF EXISTS ${tableName} FORCE """
        sql """
            CREATE TABLE ${tableName} (
                id INT,
                sort_key INT,
                title VARCHAR(128),
                body STRING,
                payload STRING,
                INDEX body_idx(body) USING INVERTED PROPERTIES("parser" = "english") COMMENT ''
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 4
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "V2"
            )
        """
        sql """ INSERT INTO ${tableName} VALUES ${firstInsert} """
        sql """ INSERT INTO ${tableName} VALUES ${secondInsert} """
        sql """ SYNC """

        cluster.addBackend(1, remoteClusterName)
        awaitUntil(60) {
            findBackendByClusterName(sql_return_maparray("show backends"), remoteClusterName) != null
        }

        def remoteBe = findBackendByClusterName(sql_return_maparray("show backends"), remoteClusterName)
        assertNotNull(remoteBe)
        def remoteBeHost = remoteBe.Host.toString()
        def remoteBeHttpPort = remoteBe.HttpPort

        sql "use @${remoteClusterName}"

        clearFileCache(remoteBeHost, remoteBeHttpPort)
        long segmentBefore = readMetric(remoteBeHost, remoteBeHttpPort, segmentMetric)
        def fullScanResult = sql """ SELECT id FROM ${tableName} ORDER BY id """
        assertEquals(48, fullScanResult.size())
        awaitUntil(30) {
            readMetric(remoteBeHost, remoteBeHttpPort, segmentMetric) > segmentBefore
        }
        long segmentAfter = readMetric(remoteBeHost, remoteBeHttpPort, segmentMetric)
        assertTrue(segmentAfter > segmentBefore,
                "${segmentMetric} should increase after remote full scan, before=${segmentBefore}, after=${segmentAfter}")

        clearFileCache(remoteBeHost, remoteBeHttpPort)
        long invertedBefore = readMetric(remoteBeHost, remoteBeHttpPort, invertedMetric)
        def invertedQueryResult = sql """
            SELECT id
            FROM ${tableName}
            WHERE body MATCH_ALL 'needlequick'
            ORDER BY id
        """
        assertTrue(invertedQueryResult.size() > 0)
        awaitUntil(30) {
            readMetric(remoteBeHost, remoteBeHttpPort, invertedMetric) > invertedBefore
        }
        long invertedAfter = readMetric(remoteBeHost, remoteBeHttpPort, invertedMetric)
        assertTrue(invertedAfter > invertedBefore,
                "${invertedMetric} should increase after remote inverted index query, before=${invertedBefore}, after=${invertedAfter}")
    }
}
