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


suite('test_balance_use_compute_group_properties', 'docker') {
    if (!isCloudMode()) {
        return;
    }
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=1',
        'sys_log_verbose_modules=org',
        'heartbeat_interval_second=1',
        'rehash_tablet_after_be_dead_seconds=3600',
        'cloud_warm_up_for_rebalance_type=sync_warmup',
        'cloud_pre_heating_time_limit_sec=30'
    ]
    options.beConfigs += [
        'report_tablet_interval_seconds=1',
        'schedule_sync_tablets_interval_s=18000',
        'disable_auto_compaction=true',
        'sys_log_verbose_modules=*'
    ]
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true
    options.enableDebugPoints()

    def mergeDirs = { base, add ->
        base + add.collectEntries { host, hashFiles ->
            [(host): base[host] ? (base[host] + hashFiles) : hashFiles]
        }
    }

    def global_config_cluster = "compute_cluster"
    def without_warmup_cluster = "without_warmup"
    def async_warmup_cluster = "async_warmup"
    def sync_warmup_cluster = "sync_warmup"

    def testCase = { table -> 
        def ms = cluster.getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort

        // alter each cluster different balance type
        sql """ALTER COMPUTE GROUP $without_warmup_cluster PROPERTIES ('balance_type'='without_warmup')"""
        sql """ALTER COMPUTE GROUP $async_warmup_cluster PROPERTIES ('balance_type'='async_warmup', 'balance_warm_up_task_timeout'='10')"""
        sql """ALTER COMPUTE GROUP $sync_warmup_cluster PROPERTIES ('balance_type'='sync_warmup')"""

        sql """CREATE TABLE $table (
            `k1` int(11) NULL,
            `v1` VARCHAR(2048)
            )
            DUPLICATE KEY(`k1`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k1`) BUCKETS 2
            PROPERTIES (
            "replication_num"="1"
            );
        """
        sql """
            insert into $table values (10, '1'), (20, '2')
        """
        sql """
            insert into $table values (30, '3'), (40, '4')
        """

        def beforeBalanceEveryClusterCache = [:]

        def clusterNameToBeIdx = [:]
        clusterNameToBeIdx[global_config_cluster] = [1]
        clusterNameToBeIdx[without_warmup_cluster] = [2]
        clusterNameToBeIdx[async_warmup_cluster] = [3]
        clusterNameToBeIdx[sync_warmup_cluster] = [4]

        // generate primary tablet in each cluster
        for (clusterName in [global_config_cluster, without_warmup_cluster, async_warmup_cluster, sync_warmup_cluster]) {
            sql """ use @$clusterName """
            sql """ select * from $table """
            def beIdxs = clusterNameToBeIdx[clusterName]
            def bes = []
            beIdxs.each { beIdx ->
                def be = cluster.getBeByIndex(beIdx)
                bes << be
            }
            logger.info("clusterName {} be idxs {}, bes {}", clusterName, beIdxs, bes)

            // before add be
            def beforeGetFromFe = getTabletAndBeHostFromFe(table)
            def beforeGetFromBe = getTabletAndBeHostFromBe(bes)
            logger.info("before add be fe tablets {}, be tablets {}", beforeGetFromFe, beforeGetFromBe)
            // version 2
            def beforeCacheDirVersion2 = getTabletFileCacheDirFromBe(msHttpPort, table, 2)
            logger.info("cache dir version 2 {}", beforeCacheDirVersion2)
            // version 3
            def beforeCacheDirVersion3 = getTabletFileCacheDirFromBe(msHttpPort, table, 3)
            logger.info("cache dir version 3 {}", beforeCacheDirVersion3)

            def beforeWarmUpResult = sql_return_maparray """ADMIN SHOW REPLICA DISTRIBUTION FROM $table"""
            logger.info("before warm up result {}", beforeWarmUpResult)
            def merged23CacheDir = [beforeCacheDirVersion2, beforeCacheDirVersion3]
                .inject([:]) { acc, m -> mergeDirs(acc, m) }
            beforeBalanceEveryClusterCache[clusterName] = [beforeGetFromFe, beforeGetFromBe, merged23CacheDir]
        }
        logger.info("before balance every cluster cache {}", beforeBalanceEveryClusterCache)

        // disable cloud balance
        setFeConfig('enable_cloud_multi_replica', true)
        cluster.addBackend(1, global_config_cluster)
        cluster.addBackend(1, without_warmup_cluster)
        cluster.addBackend(1, async_warmup_cluster)
        cluster.addBackend(1, sync_warmup_cluster)
        GetDebugPoint().enableDebugPointForAllBEs("CloudBackendService.check_warm_up_cache_async.return_task_false")
        setFeConfig('enable_cloud_multi_replica', false)

        clusterNameToBeIdx[global_config_cluster] = [1, 5]
        clusterNameToBeIdx[without_warmup_cluster] = [2, 6]
        clusterNameToBeIdx[async_warmup_cluster] = [3, 7]
        clusterNameToBeIdx[sync_warmup_cluster] = [4, 8]
        
        // sleep 11s, wait balance
        // and sync_warmup cluster task 10s timeout
        sleep(11 * 1000)

        def afterBalanceEveryClusterCache = [:]

        for (clusterName in [global_config_cluster, without_warmup_cluster, async_warmup_cluster, sync_warmup_cluster]) {
            sql """ use @$clusterName """
            sql """ select * from $table """
            def beIdxs = clusterNameToBeIdx[clusterName]
            def bes = []
            beIdxs.each { beIdx ->
                def be = cluster.getBeByIndex(beIdx)
                bes << be
            }
            logger.info("after add be clusterName {} be idxs {}, bes {}", clusterName, beIdxs, bes)

            // after add be
            def afterGetFromFe = getTabletAndBeHostFromFe(table)
            def afterGetFromBe = getTabletAndBeHostFromBe(bes)
            // version 2
            def afterCacheDirVersion2 = getTabletFileCacheDirFromBe(msHttpPort, table, 2)
            logger.info("cache dir version 2 {}", afterCacheDirVersion2)
            // version 3
            def afterCacheDirVersion3 = getTabletFileCacheDirFromBe(msHttpPort, table, 3)
            logger.info("cache dir version 3 {}", afterCacheDirVersion3)
            def merged23CacheDir = [afterCacheDirVersion2, afterCacheDirVersion3]
                .inject([:]) { acc, m -> mergeDirs(acc, m) }
            afterBalanceEveryClusterCache[clusterName] = [afterGetFromFe, afterGetFromBe, merged23CacheDir]
            logger.info("after add be clusterName {} fe tablets {}, be tablets {}, cache dir {}", clusterName, afterGetFromFe, afterGetFromBe, merged23CacheDir)
        }
        logger.info("after add be balance every cluster cache {}", afterBalanceEveryClusterCache)

        // assert first map keys
        def assertFirstMapKeys = { clusterRet, expectedEqual ->
            def firstMap = clusterRet[0]
            def keys = firstMap.keySet().toList()
            if (expectedEqual) {
                assert firstMap[keys[0]] == firstMap[keys[1]]
            } else {
                assert firstMap[keys[0]] != firstMap[keys[1]]
            }
        }

        // check afterBalanceEveryClusterCache
        // fe config cloud_warm_up_for_rebalance_type=sync_warmup
        def global_config_cluster_ret = afterBalanceEveryClusterCache[global_config_cluster]
        logger.info("global_config_cluster_ret {}", global_config_cluster_ret)
        // fe tablets not changed
        assertFirstMapKeys(global_config_cluster_ret, true)

        def without_warmup_cluster_ret = afterBalanceEveryClusterCache[without_warmup_cluster]
        logger.info("without_warmup_cluster_ret {}", without_warmup_cluster_ret)
        // fe tablets has changed
        assertFirstMapKeys(without_warmup_cluster_ret, false)

        def async_warmup_cluster_ret = afterBalanceEveryClusterCache[async_warmup_cluster]
        logger.info("async_warmup_cluster_ret {}", async_warmup_cluster_ret)
        // fe tablets has changed, due to task timeout
        assertFirstMapKeys(async_warmup_cluster_ret, false)

        def sync_warmup_cluster_ret = afterBalanceEveryClusterCache[sync_warmup_cluster]
        logger.info("sync_warmup_cluster_ret {}", sync_warmup_cluster_ret)
        // fe tablets not changed
        assertFirstMapKeys(sync_warmup_cluster_ret, true)

        logger.info("success check after balance every cluster cache, cluster's balance type is worked")
    }

    docker(options) {
        cluster.addBackend(1, without_warmup_cluster)
        cluster.addBackend(1, async_warmup_cluster)
        cluster.addBackend(1, sync_warmup_cluster)
        testCase("test_balance_warm_up_tbl")
    }
}
