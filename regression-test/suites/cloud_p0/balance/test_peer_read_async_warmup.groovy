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


suite('test_peer_read_async_warmup', 'docker') {
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
        'cloud_warm_up_for_rebalance_type=peer_read_async_warmup',
        // disable Auto Analysis Job Executor
        'auto_check_statistics_in_minutes=60',
    ]
    options.beConfigs += [
        'report_tablet_interval_seconds=1',
        'schedule_sync_tablets_interval_s=18000',
        'disable_auto_compaction=true',
        'sys_log_verbose_modules=*',
        'enable_cache_read_from_peer=true',
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

    def getBrpcMetrics = {ip, port, name ->
        def url = "http://${ip}:${port}/brpc_metrics"
        if ((context.config.otherConfigs.get("enableTLS")?.toString()?.equalsIgnoreCase("true")) ?: false) {
            url = url.replace("http://", "https://") + " --cert " + context.config.otherConfigs.get("trustCert") + " --cacert " + context.config.otherConfigs.get("trustCACert") + " --key " + context.config.otherConfigs.get("trustCAKey")
        }
        def metrics = new URL(url).text
        def matcher = metrics =~ ~"${name}\\s+(\\d+)"
        if (matcher.find()) {
            return matcher[0][1] as long
        } else {
            throw new RuntimeException("${name} not found for ${ip}:${port}")
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

        // disable cloud balance
        setFeConfig('enable_cloud_multi_replica', true)
        cluster.addBackend(1, "compute_cluster")
        GetDebugPoint().enableDebugPointForAllBEs("FileCacheBlockDownloader::download_segment_file_sleep", [sleep_time: 50])
        setFeConfig('enable_cloud_multi_replica', false)
        awaitUntil(500) {
            def afterRebalanceResult = sql_return_maparray """ADMIN SHOW REPLICA DISTRIBUTION FROM $table"""
            logger.info("after rebalance result {}", afterRebalanceResult)
            afterRebalanceResult.any { row -> 
                Integer.valueOf((String) row.ReplicaNum) == 1
            }
        } 

        sql """
            insert into $table values (50, '4'), (60, '6')
        """
        // version 4, in new be, but not in old be
        def beforeCacheDirVersion4 = getTabletFileCacheDirFromBe(msHttpPort, table, 4)
        logger.info("cache dir version 4 {}", beforeCacheDirVersion4)

        sql """
            insert into $table values (70, '7'), (80, '8')
        """
        // version 5, in new be, but not in old be
        def beforeCacheDirVersion5 = getTabletFileCacheDirFromBe(msHttpPort, table, 5)
        logger.info("cache dir version 5 {}", beforeCacheDirVersion5)

        def afterMerged2345CacheDir = [beforeCacheDirVersion2, beforeCacheDirVersion3, beforeCacheDirVersion4, beforeCacheDirVersion5]
            .inject([:]) { acc, m -> mergeDirs(acc, m) }
        logger.info("after version 2,3,4,5 fe tablets {}, be tablets {}, cache dir {}", beforeGetFromFe, beforeGetFromBe, afterMerged2345CacheDir)

        def oldBe = sql_return_maparray('show backends').get(0)
        def newAddBe = sql_return_maparray('show backends').get(1)

        def newAddBeCacheDir = afterMerged2345CacheDir.get(newAddBe.Host)
        logger.info("new add be cache dir {}", newAddBeCacheDir)
        // version 4, 5
        assertTrue(newAddBeCacheDir.size() == 2, "new add be should have version 4,5 cache file")
        // warm up task blocked by debug point, so old be should not have version 4,5 cache file
        assertFalse(afterMerged2345CacheDir[oldBe.Host].containsAll(newAddBeCacheDir), "old be should not have version 4,5 cache file")

        // The query triggers reading the file cache from the peer
        profile("test_peer_read_async_warmup_profile") {
            sql """ set enable_profile = true;"""
            sql """ set profile_level = 2;"""
            run {
                sql """/* test_peer_read_async_warmup_profile */ select * from $table"""
                sleep(1000)
            }

            check { profileString, exception ->
                log.info(profileString)
                // Use a regular expression to match the numeric value inside parentheses after "NumPeerIOTotal:"
                def matcher = (profileString =~ /-  NumPeerIOTotal:\s+(\d+)/)
                def total = 0
                while (matcher.find()) {
                    total += matcher.group(1).toInteger()
                    logger.info("NumPeerIOTotal: {}", matcher.group(1))
                }
                assertTrue(total > 0)
            } 
        }

        // peer read cache, so it should read version 2,3 cache file from old be, not s3
        assertTrue(0 != getBrpcMetrics(newAddBe.Host, newAddBe.BrpcPort, "cached_remote_reader_peer_read"), "new add be should have peer read cache")
        assertTrue(0 == getBrpcMetrics(newAddBe.Host, newAddBe.BrpcPort, "cached_remote_reader_s3_read"), "new add be should not have s3 read cache")
    }

    docker(options) {
        testCase("test_peer_read_async_warmup_tbl")
    }
}
