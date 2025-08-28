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
import groovy.json.JsonSlurper
import org.awaitility.Awaitility;
import static java.util.concurrent.TimeUnit.SECONDS;
import org.apache.doris.regression.util.NodeType

suite('test_read_from_peer', 'docker') {
    if (!isCloudMode()) {
        return;
    }
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'sys_log_verbose_modules=org',
        'heartbeat_interval_second=1',
        'workload_group_check_interval_ms=1'
    ]
    options.beConfigs += [
        'file_cache_each_block_size=131072',
        // 'sys_log_verbose_modules=*',
    ]
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true
    options.enableDebugPoints()
    
    def tableName = "test_read_from_table"

    def clusterBe = { clusterName ->
        def bes = sql_return_maparray "show backends"
        def clusterBes = bes.findAll { be -> be.Tag.contains(clusterName) }
        logger.info("cluster {}, bes {}", clusterName, clusterBes)
        clusterBes[0]
    }

    def testCase = { String clusterName, String runType, String fetchFrom ->
        def startTime = System.currentTimeMillis()
        GetDebugPoint().enableDebugPointForAllBEs("CachedRemoteFileReader.read_at_impl.change_type", [type: runType])
        
        try {
            sql """
                use @$clusterName
            """

            def be = clusterBe(clusterName)
            def haveCacheBe = clusterBe("compute_cluster")

            switch (fetchFrom) {
                case "s3":
                    // Read from S3: disable peer reads
                    GetDebugPoint().enableDebugPoint(be.Host, be.HttpPort as int, NodeType.BE, "CachedRemoteFileReader.read_at_impl.peer_read_fn_failed")
                    break

                case "peer":
                    // Read from peer: disable S3 reads and enable peer reads
                    GetDebugPoint().enableDebugPoint(be.Host, be.HttpPort as int, NodeType.BE, "CachedRemoteFileReader.read_at_impl.s3_read_fn_failed")
                    GetDebugPoint().enableDebugPoint(be.Host, be.HttpPort as int, NodeType.BE, "CachedRemoteFileReader::_fetch_from_peer_cache_blocks", 
                        [host: haveCacheBe.Host, port: haveCacheBe.BrpcPort])
                    break
                    
                case "winner":
                    GetDebugPoint().enableDebugPoint(be.Host, be.HttpPort as int, NodeType.BE, "CachedRemoteFileReader::_fetch_from_peer_cache_blocks", 
                        [host: haveCacheBe.Host, port: haveCacheBe.BrpcPort])
                    break

                case "hit_cache":
                    break
                    
                default:
                    throw new IllegalArgumentException("Invalid type: $fetchFrom. Expected: peer, s3, or winner")
            }
            
            // Execute the query and measure time
            def queryStartTime = System.currentTimeMillis()
            sql """
                select count(*) from $tableName
            """
            def queryTime = System.currentTimeMillis() - queryStartTime
            logger.info("Test completed - Type:{}, FetchFrom: {}, Cluster: {}, Query execution time: {}ms", runType, fetchFrom, clusterName, queryTime)
        } catch (Exception e) {
            def totalTime = System.currentTimeMillis() - startTime
            logger.info("Test failed after {}ms - Type: {}, FetchFrom: {}, Cluster: {} Error: {}", totalTime, runType, fetchFrom, clusterName, e.message)
            throw e
        }
    }

    docker(options) {
        // 添加一个新的cluster, 只从s3上读
        cluster.addBackend(1, "readS3cluster")

        // 添加一个新的cluster, 只从peer上读
        cluster.addBackend(1, "readPeercluster")

        // 添加一个新的cluster, 从s3 和 peer 并发读，采用winner
        cluster.addBackend(1, "readWinnercluster")


        sql """CREATE TABLE IF NOT EXISTS ${tableName} (
                  `k1` int(11) NULL,
                  `k2` tinyint(4) NULL,
                  `k3` smallint(6) NULL,
                  `k4` bigint(20) NULL,
                  `k5` largeint(40) NULL,
                  `k6` float NULL,
                  `k7` double NULL,
                  `k8` decimal(9, 0) NULL,
                  `k9` char(10) NULL,
                  `k10` varchar(1024) NULL,
                  `k11` text NULL,
                  `k12` date NULL,
                  `k13` datetime NULL
                ) ENGINE=OLAP
                DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        """

        sql """
            use @compute_cluster
        """

        // in compute_cluster be-1, cache all data in file cache
        def txnId = -1;
        // version 2
        streamLoad {
            table "${tableName}"
            set 'column_separator', ','
            set 'compress_type', 'gz'
            set 'cloud_cluster', 'compute_cluster'
            file 'new_all_types.csv.gz'
            time 10000 // limit inflight 10s
            setFeAddr cluster.getAllFrontends().get(0).host, cluster.getAllFrontends().get(0).httpPort

            check { loadResult, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${loadResult}".toString())
                def json = parseJson(loadResult)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(16000000, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
                txnId = json.TxnId
            }
        }

        sql """
            select count(*) from $tableName
        """

        testCase("compute_cluster", "s3", "hit_cache")
        testCase("readS3cluster", "s3", "s3")
        testCase("readPeercluster", "peer", "peer")
        testCase("readWinnercluster", "winner", "winner")
    }
}
