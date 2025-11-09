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


suite('test_balance_warm_up_use_peer_cache', 'docker') {
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
        'cloud_warm_up_for_rebalance_type=async_warmup',
        'cloud_pre_heating_time_limit_sec=30',
        // disable Auto Analysis Job Executor
        'auto_check_statistics_in_minutes=60',
    ]
    options.beConfigs += [
        'report_tablet_interval_seconds=1',
        'schedule_sync_tablets_interval_s=18000',
        'disable_auto_compaction=true',
        'sys_log_verbose_modules=*',
        'cache_read_from_peer_expired_seconds=100',
        'enable_cache_read_from_peer=true'
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
        GetDebugPoint().enableDebugPointForAllBEs("FileCacheBlockDownloader.download_blocks.balance_task")
        setFeConfig('enable_cloud_multi_replica', false)

        sleep(5 * 1000)
        sql """
            insert into $table values (50, '4'), (60, '6')
        """
        // version 4, new rs after warm up task
        def beforeCacheDirVersion4 = getTabletFileCacheDirFromBe(msHttpPort, table, 4)
        logger.info("cache dir version 4 {}", beforeCacheDirVersion4)
        def afterMerged23CacheDir = [beforeCacheDirVersion2, beforeCacheDirVersion3, beforeCacheDirVersion4]
            .inject([:]) { acc, m -> mergeDirs(acc, m) }
        logger.info("after version 4 fe tablets {}, be tablets {}, cache dir {}", beforeGetFromFe, beforeGetFromBe, afterMerged23CacheDir)

        // after cloud_pre_heating_time_limit_sec = 30s 
        sleep(40 * 1000)
        // after 30s task timeout, check tablet in new be

        def oldBe = sql_return_maparray('show backends').get(0)
        def newAddBe = sql_return_maparray('show backends').get(1)
        // balance tablet
        awaitUntil(500) {
            def afterWarmUpTaskTimeoutResult = sql_return_maparray """ADMIN SHOW REPLICA DISTRIBUTION FROM $table"""
            logger.info("after warm up result {}", afterWarmUpTaskTimeoutResult)
            afterWarmUpTaskTimeoutResult.any { row -> 
                Integer.valueOf((String) row.ReplicaNum) == 1
            }
        }

        // from be1 -> be2, warm up this tablet
        // after add be
        def afterGetFromFe = getTabletAndBeHostFromFe(table)
        def afterGetFromBe = getTabletAndBeHostFromBe(cluster.getAllBackends())
        // version 2
        def afterCacheDirVersion2 = getTabletFileCacheDirFromBe(msHttpPort, table, 2)
        logger.info("after cache dir version 2 {}", afterCacheDirVersion2)
        // version 3
        def afterCacheDirVersion3 = getTabletFileCacheDirFromBe(msHttpPort, table, 3)
        logger.info("after cache dir version 3 {}", afterCacheDirVersion3)
        sleep(5 * 1000)
        // version 4
        def afterCacheDirVersion4 = getTabletFileCacheDirFromBe(msHttpPort, table, 4)
        logger.info("after cache dir version 4 {}", afterCacheDirVersion4)

        def afterMergedCacheDir = [afterCacheDirVersion2, afterCacheDirVersion3, afterCacheDirVersion4]
            .inject([:]) { acc, m -> mergeDirs(acc, m) }

        logger.info("after fe tablets {}, be tablets {}, cache dir {}", afterGetFromFe, afterGetFromBe, afterMergedCacheDir)
        def newAddBeCacheDir = afterMergedCacheDir.get(newAddBe.Host)
        logger.info("new add be cache dir {}", newAddBeCacheDir)
        assert newAddBeCacheDir.size() != 0
        assert afterMerged23CacheDir[oldBe.Host].containsAll(afterMergedCacheDir[newAddBe.Host])

        def be = cluster.getBeByBackendId(newAddBe.BackendId.toLong())
        def dataPath = new File("${be.path}/storage/file_cache")
        logger.info("Checking file_cache directory: {}", dataPath.absolutePath)
        logger.info("Directory exists: {}", dataPath.exists())

        def subDirs = []

        def collectDirs
        collectDirs = { File dir ->
            if (dir.exists()) {
                dir.eachDir { subDir ->
                    logger.info("Found subdir: {}", subDir.name)
                    subDirs << subDir.name
                    collectDirs(subDir) 
                }
            }
        }

        collectDirs(dataPath)
        logger.info("BE {} file_cache subdirs: {}", newAddBe.Host, subDirs)

        // check new be not have version 2,3,4 cache file
        newAddBeCacheDir.each { hashFile ->
            assertFalse(subDirs.any { subDir -> subDir.startsWith(hashFile) }, 
            "Expected cache file pattern ${hashFile} should not found in BE ${newAddBe.Host}'s file_cache directory. " + 
            "Available subdirs: ${subDirs}")
        }

        // The query triggers reading the file cache from the peer
        profile("test_balance_warm_up_use_peer_cache_profile") {
            sql """ set enable_profile = true;"""
            sql """ set profile_level = 2;"""
            run {
                sql """/* test_balance_warm_up_use_peer_cache_profile */ select * from $table"""
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

        subDirs.clear()
        collectDirs(dataPath)
        logger.info("after query, BE {} file_cache subdirs: {}", newAddBe.Host, subDirs) 
        // peer read cache, so it should have version 2,3,4 cache file
        newAddBeCacheDir.each { hashFile ->
            assertTrue(subDirs.any { subDir -> subDir.startsWith(hashFile) }, 
            "Expected cache file pattern ${hashFile} should found in BE ${newAddBe.Host}'s file_cache directory. " + 
            "Available subdirs: ${subDirs}")
        }
        assert(0 != getBrpcMetrics(newAddBe.Host, newAddBe.BrpcPort, "cached_remote_reader_peer_read"))
        assert(0 == getBrpcMetrics(newAddBe.Host, newAddBe.BrpcPort, "cached_remote_reader_s3_read"))
    }

    docker(options) {
        testCase("test_balance_warm_up_use_peer_cache_tbl")
    }
}
