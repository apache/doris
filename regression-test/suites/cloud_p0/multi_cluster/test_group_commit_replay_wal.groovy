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
import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

suite('test_group_commit_replay_wal', 'multi_cluster,docker') {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'enable_workload_group=false',
        'sys_log_verbose_modules=org.apache.doris.qe.ConnectContext'
    ]
    options.beConfigs += [
        'enable_debug_points=true'
    ]

    docker(options) {
        // add cluster1
        cluster.addBackend(1, "cpuster1")
        def ret = sql_return_maparray """show clusters"""
        logger.info("clusters: " + ret)
        def cluster0 = ret.stream().filter(cluster -> cluster.is_current == "TRUE").findFirst().orElse(null)
        def cluster0Name = cluster0['cluster'] as String
        logger.info("current cluster: " + cluster0Name)
        def cluster1 = ret.stream().filter(cluster -> cluster.cluster == "cpuster1").findFirst().orElse(null)
        assertTrue(cluster1 != null)
        sql """set property 'DEFAULT_CLOUD_CLUSTER' = '$cluster0Name'"""

        def backends = sql_return_maparray """show backends"""
        logger.info("backends: " + backends)
        long cluster1BeId = 0
        def cluster1BeHost = ""
        int cluster1BePort = 0
        for (final def backend in backends) {
            if (backend['Tag'].toString().contains("cpuster1")) {
                cluster1BeId = backend['BackendId'] as long
                cluster1BeHost = backend['Host']
                cluster1BePort = backend['HttpPort'] as int
            }
        }
        logger.info("cluster1BeId: " + cluster1BeId + ", cluster1BeHost: " + cluster1BeHost + ", cluster1BePort: " + cluster1BePort)
        assertTrue(cluster1BeId > 0)

        def testTable = "test_group_commit_replay_wal"
        sql """
            create table ${testTable} (`k` int, `v` int)
            UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES (
                "group_commit_interval_ms"="100"
            );
        """

        GetDebugPoint().enableDebugPointForAllBEs("LoadBlockQueue._finish_group_commit_load.load_error")
        streamLoad {
            table "${testTable}"
            set 'column_separator', ','
            set 'group_commit', 'async_mode'
            unset 'label'
            file 'test_group_commit_replay_wal.csv'
            time 10000
            directToBe cluster1BeHost, cluster1BePort
        }
        def rowCount = 0
        for (int i = 0; i < 30; i++) {
            def result = sql "select count(*) from ${testTable}"
            logger.info("rowCount: ${result}")
            rowCount = result[0][0]
            if (rowCount == 5) {
                break
            }
            Thread.sleep(500)
        }
        assertEquals(5, rowCount)
    }
}
