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


suite('test_balance_warm_up_task_abnormal', 'docker') {
    if (!isCloudMode()) {
        return;
    }

    // Randomly enable or disable packed_file to test both scenarios
    def enablePackedFile = new Random().nextBoolean()
    logger.info("Running test with enable_packed_file=${enablePackedFile}")

    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=1',
        'sys_log_verbose_modules=org',
        'heartbeat_interval_second=1',
        'rehash_tablet_after_be_dead_seconds=3600',
        'cloud_warm_up_for_rebalance_type=sync_warmup',
        'cloud_pre_heating_time_limit_sec=10'
    ]
    options.beConfigs += [
        'report_tablet_interval_seconds=1',
        'schedule_sync_tablets_interval_s=18000',
        'disable_auto_compaction=true',
        'sys_log_verbose_modules=*',
        "enable_packed_file=${enablePackedFile}",
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
    
    def testCase = { table -> 
        def ms = cluster.getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort
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

        // before add be
        def beforeGetFromFe = getTabletAndBeHostFromFe(table)
        def beforeGetFromBe = getTabletAndBeHostFromBe(cluster.getAllBackends())
        // version 2
        def beforeCacheDirVersion2 = getTabletFileCacheDirFromBe(msHttpPort, table, 2)
        logger.info("cache dir version 2 {}", beforeCacheDirVersion2)
        // version 3
        def beforeCacheDirVersion3 = getTabletFileCacheDirFromBe(msHttpPort, table, 3)
        logger.info("cache dir version 3 {}", beforeCacheDirVersion3)

        def beforeWarmUpResult = sql_return_maparray """ADMIN SHOW REPLICA DISTRIBUTION FROM $table"""
        logger.info("before warm up result {}", beforeWarmUpResult)
        assert beforeWarmUpResult.any { row ->
            Integer.valueOf((String) row.ReplicaNum) == 2
        }

        // disable cloud balance
        setFeConfig('enable_cloud_multi_replica', true)
        cluster.addBackend(1, "compute_cluster")
        // sync warm up task always return false
        GetDebugPoint().enableDebugPointForAllBEs("CloudBackendService.check_warm_up_cache_async.return_task_false")
        setFeConfig('enable_cloud_multi_replica', false)

        // wait for some time to make sure warm up task is processed, but mapping is not changed
        sleep(15 * 1000)
        // check mapping is not changed
        def afterAddBeResult = sql_return_maparray """ADMIN SHOW REPLICA DISTRIBUTION FROM $table"""
        logger.info("after add be result {}", afterAddBeResult)
        // two be, but 2 replica num is still mapping in old be
        assert afterAddBeResult.any { row ->
            Integer.valueOf((String) row.ReplicaNum) == 2
        }

        // test recover from abnormal
        sql """ALTER COMPUTE GROUP compute_cluster PROPERTIES ('balance_type'='without_warmup')"""

        awaitUntil(60) {
            def afterAlterResult = sql_return_maparray """ADMIN SHOW REPLICA DISTRIBUTION FROM $table"""
            logger.info("after alter balance policy result {}", afterAlterResult)
            // now mapping is changed to 1 replica in each be
            afterAlterResult.any { row ->
                Integer.valueOf((String) row.ReplicaNum) == 1
            }
        }
    }

    docker(options) {
        testCase("test_balance_warm_up_task_abnormal_tbl")
    }
}
