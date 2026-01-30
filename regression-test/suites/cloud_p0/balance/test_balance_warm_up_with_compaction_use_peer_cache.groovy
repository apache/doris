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


suite('test_balance_warm_up_with_compaction_use_peer_cache', 'docker') {
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
        'cumulative_compaction_min_deltas=5',
        'cache_read_from_peer_expired_seconds=100',
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
        sql """set enable_file_cache=true"""
        sql """CREATE TABLE $table (
                `id` BIGINT,
                `deleted` TINYINT,
                `type` String,
                `author` String,
                `timestamp` DateTimeV2,
                `comment` String,
                `dead` TINYINT,
                `parent` BIGINT,
                `poll` BIGINT,
                `children` Array<BIGINT>,
                `url` String,
                `score` INT,
                `title` String,
                `parts` Array<INT>,
                `descendants` INT,
                INDEX idx_comment (`comment`) USING INVERTED PROPERTIES("parser" = "english") COMMENT 'inverted index for comment'
            )
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 2 
            PROPERTIES ("replication_num" = "1");
        """
        sql """
            insert into $table values  (344083, 1, 'comment', 'spez', '2008-10-26 13:49:29', 'Stay tuned...',  0, 343906, 0, [31, 454446], '', 0, '', [], 0), (33, 0, 'comment', 'spez', '2006-10-10 23:50:40', 'winnar winnar chicken dinnar!',  0, 31, 0, [34, 454450], '', 0, '', [], 0);
        """
        sql """
            insert into $table values  (44, 1, 'comment', 'spez', '2006-10-11 23:00:48', 'Welcome back, Randall',  0, 43, 0, [454465], '', 0, '', [], 0), (46, 0, 'story', 'goldfish', '2006-10-11 23:39:28', '',  0, 0, 0, [454470], 'http://www.rentometer.com/', 0, ' VCs Prefer to Fund Nearby Firms - New York Times', [], 0);
        """
        sql """
            insert into $table values  (344089, 1, 'comment', 'spez', '2008-10-26 13:49:29', 'Stay tuned...',  0, 343906, 0, [31, 454446], '', 0, '', [], 0), (33, 0, 'comment', 'spez', '2006-10-10 23:50:40', 'winnar winnar chicken dinnar!',  0, 31, 0, [34, 454450], '', 0, '', [], 0);
        """
        sql """
            insert into $table values  (449, 1, 'comment', 'spez', '2006-10-11 23:00:48', 'Welcome back, Randall',  0, 43, 0, [454465], '', 0, '', [], 0), (469, 0, 'story', 'goldfish', '2006-10-11 23:39:28', '',  0, 0, 0, [454470], 'http://www.rentometer.com/', 0, ' VCs Prefer to Fund Nearby Firms - New York Times', [], 0);
        """
        sql """
            insert into $table values  (344084, 1, 'comment', 'spez', '2008-10-26 13:49:29', 'Stay tuned...',  0, 343906, 0, [31, 454446], '', 0, '', [], 0), (33, 0, 'comment', 'spez', '2006-10-10 23:50:40', 'winnar winnar chicken dinnar!',  0, 31, 0, [34, 454450], '', 0, '', [], 0);
        """
        sql """
            insert into $table values  (849, 1, 'comment', 'spez', '2006-10-11 23:00:48', 'Welcome back, Randall',  0, 43, 0, [454465], '', 0, '', [], 0), (869, 0, 'story', 'goldfish', '2006-10-11 23:39:28', '',  0, 0, 0, [454470], 'http://www.rentometer.com/', 0, ' VCs Prefer to Fund Nearby Firms - New York Times', [], 0);
        """
        // trigger compaction to generate some cache files
        trigger_and_wait_compaction(table, "cumulative")
        sleep(5 * 1000)

        def beforeCacheDirVersion7 = getTabletFileCacheDirFromBe(msHttpPort, table, 7)

        // before add be
        def beforeGetFromFe = getTabletAndBeHostFromFe(table)
        def beforeGetFromBe = getTabletAndBeHostFromBe(cluster.getAllBackends())

        def beforeWarmUpResult = sql_return_maparray """ADMIN SHOW REPLICA DISTRIBUTION FROM $table"""
        logger.info("before warm up result {}", beforeWarmUpResult)

        // disable cloud balance
        setFeConfig('enable_cloud_multi_replica', true)
        cluster.addBackend(1, "compute_cluster")
        GetDebugPoint().enableDebugPointForAllBEs("FileCacheBlockDownloader.download_blocks.balance_task")
        setFeConfig('enable_cloud_multi_replica', false)

      
        def afterMergedVersion7CacheDir = [beforeCacheDirVersion7]
            .inject([:]) { acc, m -> mergeDirs(acc, m) }
        logger.info("after version 7 fe tablets {}, be tablets {}, cache dir {}", beforeGetFromFe, beforeGetFromBe, afterMergedVersion7CacheDir)

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
        // version 7
        def afterCacheDirVersion7 = getTabletFileCacheDirFromBe(msHttpPort, table, 7)
        logger.info("after cache dir version 7 {}", afterCacheDirVersion7)
    
        def afterMergedCacheDir = [afterCacheDirVersion7]
            .inject([:]) { acc, m -> mergeDirs(acc, m) }

        logger.info("after fe tablets {}, be tablets {}, cache dir {}", afterGetFromFe, afterGetFromBe, afterMergedCacheDir)
        // calc file cache hash on new added BE, but these cache files should not exist on new BE yet
        def newAddBeCacheDir = afterMergedCacheDir.get(newAddBe.Host)
        logger.info("new add be cache dir {}", newAddBeCacheDir)
        assert newAddBeCacheDir.size() != 0
        assert afterMergedVersion7CacheDir[oldBe.Host].containsAll(afterMergedCacheDir[newAddBe.Host])

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

        // check new be not have version 7 cache file
        newAddBeCacheDir.each { hashFile ->
            assertFalse(subDirs.any { subDir -> subDir.startsWith(hashFile) }, 
            "Expected cache file pattern ${hashFile} should not found in BE ${newAddBe.Host}'s file_cache directory. " + 
            "Available subdirs: ${subDirs}")
        }

        // The query triggers reading the file cache from the peer
        profile("test_balance_warm_up_with_compaction_use_peer_cache_profile") {
            sql """ set enable_profile = true;"""
            sql """ set profile_level = 2;"""
            run {
                sql """/* test_balance_warm_up_with_compaction_use_peer_cache_profile */ SELECT count() FROM $table WHERE comment MATCH_ALL 'Welcome'"""
                sleep(1000)
            }

            check { profileString, exception ->
                log.info(profileString)
                // Use a regular expression to match the numeric value inside parentheses after "NumPeerIOTotal:"
                def matcher = (profileString =~ /-  InvertedIndexNumPeerIOTotal:\s+(\d+)/)
                def total = 0
                while (matcher.find()) {
                    total += matcher.group(1).toInteger()
                    logger.info("InvertedIndexNumPeerIOTotal: {}", matcher.group(1))
                }
                assertTrue(total > 0)
            } 
        }
        subDirs.clear()
        collectDirs(dataPath)
        logger.info("after query, BE {} file_cache subdirs: {}", newAddBe.Host, subDirs) 
        // peer read cache, so it should have version 7 cache file
        newAddBeCacheDir.each { hashFile ->
            assertTrue(subDirs.any { subDir -> subDir.startsWith(hashFile) }, 
            "Expected cache file pattern ${hashFile} should found in BE ${newAddBe.Host}'s file_cache directory. " + 
            "Available subdirs: ${subDirs}")
        }
        assert(0 != getBrpcMetrics(newAddBe.Host, newAddBe.BrpcPort, "cached_remote_reader_peer_read"))
        assert(0 == getBrpcMetrics(newAddBe.Host, newAddBe.BrpcPort, "cached_remote_reader_s3_read"))
    }

    docker(options) {
        testCase("test_balance_warm_up_with_compaction_use_peer_cache_tbl")
    }
}
