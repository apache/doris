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
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true
    options.enableDebugPoints()
    
    def table = "test_read_from_table"

    def clusterBe = { clusterName ->
        def bes = sql_return_maparray "show backends"
        def clusterBes = bes.findAll { be -> be.Tag.contains(clusterName) }
        logger.info("cluster {}, bes {}", clusterName, clusterBes)
        clusterBes[0]
    }

    def testCase = { String clusterName, String type ->
        def startTime = System.currentTimeMillis()
        
        try {
            sql """
                use @$clusterName
            """

            def be = clusterBe(clusterName)
            def haveCacheBe = clusterBe("compute_cluster")

            switch (type) {
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
                    
                default:
                    throw new IllegalArgumentException("Invalid type: $type. Expected: peer, s3, or winner")
            }
            
            // Execute the query and measure time
            def queryStartTime = System.currentTimeMillis()
            sql """
                select * from $table
            """
            def queryEndTime = System.currentTimeMillis()
            
            def totalTime = System.currentTimeMillis() - startTime
            def queryTime = queryEndTime - queryStartTime
            
            println "Test completed - Type: $type, Cluster: $clusterName"
            println "Total execution time: ${totalTime}ms"
            println "Query execution time: ${queryTime}ms"
            
        } catch (Exception e) {
            def totalTime = System.currentTimeMillis() - startTime
            println "Test failed after ${totalTime}ms - Type: $type, Cluster: $clusterName"
            println "Error: ${e.message}"
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

        sql """CREATE TABLE $table (
            `k1` int(11) NULL,
            `k2` int(11) NULL
            )
            DUPLICATE KEY(`k1`, `k2`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_num"="1"
            );
        """

        sql """
            use @compute_cluster
        """

        // in compute_cluster be-1, cache all data in file cache
        sql """
            insert into $table values (1, 1)
        """

        sql """
            select * from $table
        """

        testCase("readS3cluster", "s3")
        testCase("readPeercluster", "peer")
        testCase("readWinnercluster", "winner")
    }
}
