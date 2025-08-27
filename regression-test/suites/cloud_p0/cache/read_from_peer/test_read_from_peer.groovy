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

    docker(options) {
        def clusterName = "newcluster1"
        // 添加一个新的cluster add_new_cluster
        cluster.addBackend(1, clusterName)

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

        sql """
            use @newcluster1
        """

        // read from newcluster1, use debug point disable newcluster1 be-2 read from s3, just read from peer
        // 1. debug point set newcluster1 be-2 disable read from s3
        GetDebugPoint().enableDebugPointForAllBEs("CachedRemoteFileReader.read_at_impl.s3_read_fn_failed");
        def haveCacheBe = clusterBe("compute_cluster")
        // 2. debug point set newcluster1 be-2 peer ip and port
        GetDebugPoint().enableDebugPointForAllBEs("CachedRemoteFileReader::_fetch_from_peer_cache_blocks", [host:haveCacheBe.Host, port:haveCacheBe.BrpcPort]);

        sql """
            select * from $table
        """
    }
}
