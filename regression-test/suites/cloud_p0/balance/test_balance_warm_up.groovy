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


suite('test_balance_warm_up', 'docker') {
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
        'cloud_warm_up_for_rebalance_type=async_warmup'
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

        // more tablets accessed. for test metrics `balance_tablet_be_mapping_size`
        sql """CREATE TABLE more_tablets_warm_up_test_tbl (
            `k1` int(11) NULL,
            `v1` VARCHAR(2048)
            )
            DUPLICATE KEY(`k1`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k1`) BUCKETS 10
            PROPERTIES (
            "replication_num"="1"
            );
        """
        sql """
            insert into more_tablets_warm_up_test_tbl values (1, 'value1'), (2, 'value2'), (3, 'value3'), (4, 'value4'), (5, 'value5'), (6, 'value6'), (7, 'value7'), (8, 'value8'), (9, 'value9'), (10, 'value10'), (11, 'value11'), (12, 'value12'), (13, 'value13'), (14, 'value14'), (15, 'value15'), (16, 'value16'), (17, 'value17'), (18, 'value18'), (19, 'value19'), (20, 'value20');
        """

        // before add be
        def beforeGetFromFe = getTabletAndBeHostFromFe(table)
        def beforeGetFromBe = getTabletAndBeHostFromBe(cluster.getAllBackends())
        // version 2
        def beforeDataCacheDirVersion2 = getTabletFileCacheDirFromBe(msHttpPort, table, 2, "dat")
        def beforeIdxCacheDirVersion2 = getTabletFileCacheDirFromBe(msHttpPort, table, 2, "idx")
        logger.info("cache dir version 2 data={}, idx={}", beforeDataCacheDirVersion2, beforeIdxCacheDirVersion2)
        // version 3
        def beforeDataCacheDirVersion3 = getTabletFileCacheDirFromBe(msHttpPort, table, 3, "dat")
        def beforeIdxCacheDirVersion3 = getTabletFileCacheDirFromBe(msHttpPort, table, 3, "idx")
        logger.info("cache dir version 3 data={}, idx={}", beforeDataCacheDirVersion3, beforeIdxCacheDirVersion3)

        // 通用合并函数：按 host 合并多个 map, 并去重
        def mergeCacheDirs = { Map[] maps ->
            def result = [:].withDefault { [] } // 每个不存在的 key 都会生成新的 List
            maps.each { m ->
                if (!m) return
                m.each { host, files ->
                    if (!files) return
                    def target = result[host]
                    if (files instanceof Collection) {
                        target.addAll(files)
                    } else {
                        target << files
                    }
                }
            }
            // 确保每个 host 的文件列表去重后返回
            result.collectEntries { host, files -> [ (host): files.unique() ] }
        }

        def beforeMergedCacheDir = mergeCacheDirs(
            beforeDataCacheDirVersion2,
            beforeIdxCacheDirVersion2,
            beforeDataCacheDirVersion3,
            beforeIdxCacheDirVersion3
        )
        logger.info("before fe tablets {}, be tablets {}, cache dir {}", beforeGetFromFe, beforeGetFromBe, beforeMergedCacheDir)

        def beforeWarmUpResult = sql_return_maparray """ADMIN SHOW REPLICA DISTRIBUTION FROM $table"""
        logger.info("before warm up result {}", beforeWarmUpResult)

        cluster.addBackend(1, "compute_cluster")
        def oldBe = sql_return_maparray('show backends').get(0)
        def newAddBe = sql_return_maparray('show backends').get(1)
        // balance tablet
        awaitUntil(500) {
            def afterWarmUpResult = sql_return_maparray """ADMIN SHOW REPLICA DISTRIBUTION FROM $table"""
            logger.info("after warm up result {}", afterWarmUpResult)
            afterWarmUpResult.any { row -> 
                Integer.valueOf((String) row.ReplicaNum) == 1
            }
        }

        sql """select count(*) from more_tablets_warm_up_test_tbl"""

        // from be1 -> be2, warm up this tablet
        // after add be
        def afterGetFromFe = getTabletAndBeHostFromFe(table)
        def afterGetFromBe = getTabletAndBeHostFromBe(cluster.getAllBackends())
        // version 2
        def afterDataCacheDirVersion2 = getTabletFileCacheDirFromBe(msHttpPort, table, 2, "dat")
        def afterIdxCacheDirVersion2 = getTabletFileCacheDirFromBe(msHttpPort, table, 2, "idx")
        logger.info("after cache dir version 2 data={}, idx={}", afterDataCacheDirVersion2, afterIdxCacheDirVersion2)
        // version 3
        def afterDataCacheDirVersion3 = getTabletFileCacheDirFromBe(msHttpPort, table, 3, "dat")
        def afterIdxCacheDirVersion3 = getTabletFileCacheDirFromBe(msHttpPort, table, 3, "idx")
        logger.info("after cache dir version 3 data={}, idx={}", afterDataCacheDirVersion3, afterIdxCacheDirVersion3)

        def afterMergedCacheDir = mergeCacheDirs(
            afterDataCacheDirVersion2,
            afterIdxCacheDirVersion2,
            afterDataCacheDirVersion3,
            afterIdxCacheDirVersion3 
        )
        logger.info("after fe tablets {}, be tablets {}, cache dir {}", afterGetFromFe, afterGetFromBe, afterMergedCacheDir)
        def newAddBeCacheDir = afterMergedCacheDir.get(newAddBe.Host)
        logger.info("new add be cache dir {}", newAddBeCacheDir)
        assert newAddBeCacheDir.size() != 0
        assert beforeMergedCacheDir[oldBe.Host].containsAll(afterMergedCacheDir[newAddBe.Host])

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

        newAddBeCacheDir.each { hashFile ->
            assertTrue(subDirs.any { subDir -> subDir.startsWith(hashFile) }, 
            "Expected cache file pattern ${hashFile} not found in BE ${newAddBe.Host}'s file_cache directory. " + 
            "Available subdirs: ${subDirs}")
        }

        sleep(105 * 1000)
        // test expired be tablet cache info be removed
        // after cache_read_from_peer_expired_seconds = 100s
        assert(0 == getBrpcMetrics(newAddBe.Host, newAddBe.BrpcPort, "balance_tablet_be_mapping_size"))
        assert(0 == getBrpcMetrics(newAddBe.Host, newAddBe.BrpcPort, "cached_remote_reader_peer_read"))
        assert(0 != getBrpcMetrics(newAddBe.Host, newAddBe.BrpcPort, "cached_remote_reader_s3_read"))
    }

    docker(options) {
        testCase("test_balance_warm_up_tbl")
    }
}
