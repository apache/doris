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
import org.apache.doris.regression.util.NodeType
import groovy.json.JsonSlurper

suite('test_warmup_table_docker', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
    ]
    options.beConfigs += [
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'enable_evict_file_cache_in_advance=false',
        'enable_only_warm_up_idx=true',
    ]
    options.cloudMode = true
    options.beNum = 1
    options.feNum = 1

    def testTable = "test_warmup_table"

    def clearFileCache = {ip, port ->
        def url = "http://${ip}:${port}/api/file_cache?op=clear&sync=true"
        def response = new URL(url).text
        def json = new JsonSlurper().parseText(response)

        // Check the status
        if (json.status != "OK") {
            throw new RuntimeException("Clear cache on ${ip}:${port} failed: ${json.status}")
        }
    }

    def clearFileCacheOnAllBackends = {
        def backends = sql """SHOW BACKENDS"""

        for (be in backends) {
            def ip = be[1]
            def port = be[4]
            clearFileCache(ip, port)
        }

        // clear file cache is async, wait it done
        sleep(5000)
    }

    def getBrpcMetrics = {ip, port, name ->
        def url = "http://${ip}:${port}/brpc_metrics"
        def metrics = new URL(url).text
        def matcher = metrics =~ ~"${name}\\s+(\\d+)"
        if (matcher.find()) {
            return matcher[0][1] as long
        } else {
            return 0
        }
    }

    def updateBeConf = {cluster, key, value ->
        def backends = sql """SHOW BACKENDS"""
        def cluster_bes = backends.findAll { it[19].contains("""\"compute_group_name\" : \"${cluster}\"""") }
        for (be in cluster_bes) {
            def ip = be[1]
            def port = be[4]
            def (code, out, err) = update_be_config(ip, port, key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    }

    docker(options) {
        def clusterName = "warmup_cluster"

        // Add one cluster
        cluster.addBackend(1, clusterName)

        // Ensure we are in the cluster
        sql """use @${clusterName}"""
        
        try {
            sql "set global enable_audit_plugin = false"
        } catch (Exception e) {
            logger.info("set global enable_audit_plugin = false failed: " + e.getMessage())
        }

        sql """ DROP TABLE IF EXISTS ${testTable} """
        sql """ CREATE TABLE IF NOT EXISTS ${testTable} (
            `k1` int(11) NULL,
            `k2` int(11) NULL,
            `v3` int(11) NULL,
            `text` text NULL,
            INDEX idx_text (`text`) USING INVERTED PROPERTIES("parser" = "english")
        ) unique KEY(`k1`, `k2`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
        """

        // Load data
        int dataCount = 4000000
        streamLoad {
            table "${testTable}"
            set 'column_separator', ','
            inputIterator (new Iterator<String>() {
                int current = 0
                @Override
                boolean hasNext() {
                    return current < dataCount
                }
                @Override
                String next() {
                    current++
                    if (current % 2 == 0) {
                        return "${current},${current},${current},hello doris ${current}"
                    } else {
                        return "${current},${current},${current},hello world ${current}"
                    }
                }
            })

            check { res, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(res)
                assertEquals("success", json.Status.toLowerCase())
            }
        }
        sql "sync"

        def backends = sql """SHOW BACKENDS"""
        def ip = backends[0][1]
        def brpcPort = backends[0][5]

        sleep(3000)
        def cache_size_after_load = getBrpcMetrics(ip, brpcPort, "cache_cache_size")

        // Clear file cache to ensure warm up actually does something
        clearFileCacheOnAllBackends()

        sleep(3000)
        def cache_size_after_clear = getBrpcMetrics(ip, brpcPort, "cache_cache_size")
        assertEquals(cache_size_after_clear, 0)

        // Set enable_only_warm_up_idx = true
        updateBeConf(clusterName, "enable_only_warm_up_idx", "true")

        // Trigger warm up
        def jobId = sql "WARM UP CLUSTER ${clusterName} WITH TABLE ${testTable}"
        assertNotNull(jobId)
        def id = jobId[0][0]

        // Wait for warm up job to finish
        def waitJobFinished = { job_id ->
            for (int i = 0; i < 60; i++) {
                def result = sql "SHOW WARM UP JOB WHERE ID = ${job_id}"
                if (result.size() > 0) {
                    def status = result[0][3]
                    logger.info("Warm up job ${job_id} status: ${status}")
                    if (status == "FINISHED") {
                        return true
                    } else if (status == "CANCELLED") {
                        throw new RuntimeException("Warm up job ${job_id} cancelled")
                    }
                }
                sleep(1000)
            }
            return false
        }

        assertTrue(waitJobFinished(id), "Warm up job ${id} did not finish in time")
        sleep(3000)
        def cache_size_after_warm = getBrpcMetrics(ip, brpcPort, "cache_cache_size")

        logger.info("Cache size after load: ${cache_size_after_load}, after clear: ${cache_size_after_clear}, after warm up: ${cache_size_after_warm}")
        assertTrue(cache_size_after_warm < cache_size_after_load);

        def s3ReadBefore = getBrpcMetrics(ip, brpcPort, "cached_remote_reader_s3_read")

        // Verify data can be read
        def result = sql "SELECT COUNT() FROM ${testTable} WHERE text MATCH_ANY 'doris'"
        sleep(3000)
        // Get metrics after query
        def s3ReadAfter = getBrpcMetrics(ip, brpcPort, "cached_remote_reader_s3_read")
        
        // Check no cache miss (s3 read count should not increase)
        assertEquals(s3ReadBefore, s3ReadAfter)
    }
}
