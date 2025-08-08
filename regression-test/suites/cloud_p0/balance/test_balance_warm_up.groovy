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
        'enable_cloud_warm_up_for_rebalance=true'
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

        def beforeMergedCacheDir = beforeCacheDirVersion2 + beforeCacheDirVersion3.collectEntries { host, hashFiles ->
            [(host): beforeCacheDirVersion2[host] ? (beforeCacheDirVersion2[host] + hashFiles) : hashFiles]
        }
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

        def afterMergedCacheDir = afterCacheDirVersion2 + afterCacheDirVersion3.collectEntries { host, hashFiles ->
            [(host): afterCacheDirVersion2[host] ? (afterCacheDirVersion2[host] + hashFiles) : hashFiles]
        }
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
    }

    docker(options) {
        testCase("test_balance_warm_up_tbl")
    }
}
