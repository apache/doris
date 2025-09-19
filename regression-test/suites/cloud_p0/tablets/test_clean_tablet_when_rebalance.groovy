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
import org.apache.doris.regression.util.Http

suite('test_clean_tablet_when_rebalance', 'docker') {
    if (!isCloudMode()) {
        return;
    }
    def options = new ClusterOptions()
    def rehashTime = 100
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=1',
        'sys_log_verbose_modules=org',
        'heartbeat_interval_second=1'
    ]
    options.feConfigs.add("rehash_tablet_after_be_dead_seconds=$rehashTime")
    options.beConfigs += [
        'report_tablet_interval_seconds=1',
        'write_buffer_size=10240',
        'write_buffer_size_for_agg=10240'
    ]
    options.setFeNum(3)
    options.setBeNum(3)
    options.cloudMode = true
    options.enableDebugPoints()

    def choseDeadBeIndex = 1
    def table = "test_clean_tablet_when_rebalance"

    def selectTriggerRehash = { ->
        for (int i = 0; i < 5; i++) {
            sql """
                select count(*) from $table
            """
        }
    }

    def getTabletInHostFromBe = { bes ->
        def ret = [:]
        bes.each { be ->
            // {"msg":"OK","code":0,"data":{"host":"128.2.51.2","tablets":[{"tablet_id":10560},{"tablet_id":10554},{"tablet_id":10552}]},"count":3}
            def data = Http.GET("http://${be.host}:${be.httpPort}/tablets_json?limit=all", true).data
            def tablets = data.tablets.collect { it.tablet_id as String }
            tablets.each {
                if (ret[it] != null) {
                    ret[it].add(data.host)
                } else {
                    ret[it] = [data.host]
                }
            }
        }
        ret
    }

    def testCase = { deadTime, mergedCacheDir -> 
        boolean beDeadLong = deadTime > rehashTime ? true : false
        logger.info("begin exec beDeadLong {}", beDeadLong)

        selectTriggerRehash.call()

        def beforeGetFromFe = getTabletAndBeHostFromFe(table)
        def beforeGetFromBe = getTabletAndBeHostFromBe(cluster.getAllBackends())
        logger.info("before fe tablets {}, be tablets {}", beforeGetFromFe, beforeGetFromBe)
        beforeGetFromFe.each {
            assertTrue(beforeGetFromBe.containsKey(it.Key))
            assertEquals(beforeGetFromBe[it.Key], it.Value[1])
        }

        cluster.stopBackends(choseDeadBeIndex)
        awaitUntil(50) {
            def showTablets = sql_return_maparray("SHOW TABLETS FROM ${table}")
            def bes = showTablets
                .collect { it.BackendId }
                .unique()
            logger.info("before start bes {}, tablets {}", bes, showTablets)
            bes.size() == 2
        }
        // rehash
        selectTriggerRehash.call()
        // curl be, tablets in 2 bes

        if (beDeadLong) {
            setFeConfig('enable_cloud_partition_balance', false)
            setFeConfig('enable_cloud_table_balance', false)
            setFeConfig('enable_cloud_global_balance', false)
        }

        // wait report logic
        sleep(deadTime * 1000)
        cluster.startBackends(choseDeadBeIndex)
        def afterGetFromFe = getTabletAndBeHostFromFe(table)
        def afterGetFromBe = getTabletAndBeHostFromBe(cluster.getAllBackends())
        logger.info("after stop one be, rehash fe tablets {}, be tablets {}", afterGetFromFe, afterGetFromBe)

        awaitUntil(50) {
            def showTablets = sql_return_maparray("SHOW TABLETS FROM ${table}")
            def bes = showTablets
                .collect { it.BackendId }
                .unique()
            logger.info("after start bes {}, tablets {}", bes, showTablets)
            bes.size() == (beDeadLong ? 2 : 3)
        }

        selectTriggerRehash.call()
        // wait report logic
        // tablet report clean not work, before sleep, in fe secondary not been clear
        afterGetFromFe = getTabletAndBeHostFromFe(table)
        afterGetFromBe = getTabletInHostFromBe(cluster.getAllBackends())
        logger.info("before sleep rehash time, fe tablets {}, be tablets {}", afterGetFromFe, afterGetFromBe)
        def redundancyTablet = null
        afterGetFromFe.each {
            assertTrue(afterGetFromBe.containsKey(it.Key))
            if (afterGetFromBe[it.Key].size() == 2) {
                redundancyTablet = it.Key
                logger.info("find tablet {} redundancy in {}", it.Key, afterGetFromBe[it.Key])
            }
            assertTrue(afterGetFromBe[it.Key].contains(it.Value[1]))
        }

        sleep(rehashTime * 1000 + 10 * 1000)
        // tablet report clean will work, after sleep, in fe secondary been clear

        afterGetFromFe = getTabletAndBeHostFromFe(table)
        afterGetFromBe = getTabletAndBeHostFromBe(cluster.getAllBackends())
        if (!beDeadLong) {
            def checkAfterGetFromBe = getTabletInHostFromBe(cluster.getAllBackends())
            assertEquals(1, checkAfterGetFromBe[redundancyTablet].size())
        }
        logger.info("after sleep rehash time, fe tablets {}, be tablets {}", afterGetFromFe, afterGetFromBe)
        afterGetFromFe.each {
            assertTrue(afterGetFromBe.containsKey(it.Key))
            assertEquals(afterGetFromBe[it.Key], it.Value[1])
        }

        // TODO(freemandealer)
        // Once the freemandealer implements file cache cleanup during restart, enabling lines 107 to 145 will allow testing to confirm that after the rebalance, the tablet file cache on the BE will be cleared. In the current implementation, after restarting the BE and triggering the rebalance, the tablets in the tablet manager will be cleared, but the file cache cannot be cleaned up.
        /*
        if (beDeadLong) {
            // check tablet file cache has been deleted
            // after fe tablets {10309=[10003, 175.41.51.3], 10311=[10002, 175.41.51.2], 10313=[10003, 175.41.51.3]},
            afterGetFromFe.each {
                logger.info("tablet_id {}, before host {}, after host {}", it.Key, beforeGetFromFe[it.Key][1], it.Value[1])
                if (beforeGetFromFe[it.Key][1] == it.Value[1]) {
                    return
                }
                logger.info("tablet_id {} has been reblanced from {} to {}", it.Key, beforeGetFromFe[it.Key][1], it.Value[1])
                // check before tablet file cache dir has been droped

                def tabletId = it.Key
                def backendId = beforeGetFromFe[it.Key][0]
                def backendHost = beforeGetFromFe[it.Key][1]
                def be = cluster.getBeByBackendId(backendId.toLong())
                def dataPath = new File("${be.path}/storage/file_cache")
                def subDirs = []
                
                def collectDirs
                collectDirs = { File dir ->
                    if (dir.exists()) {
                        dir.eachDir { subDir ->
                            subDirs << subDir.name
                            collectDirs(subDir) 
                        }
                    }
                }
                
                collectDirs(dataPath)
                logger.info("BE {} file_cache subdirs: {}", backendHost, subDirs)
                def cacheDir = mergedCacheDir[backendHost]

                // add check
                cacheDir.each { hashFile ->
                    assertFalse(subDirs.any { subDir -> subDir.startsWith(hashFile) }, 
                    "Found unexpected cache file pattern ${hashFile} in BE ${backendHost}'s file_cache directory. " + 
                    "Matching subdir found in: ${subDirs}")
                }
            }
        }
        */
    }

    docker(options) {
        def ms = cluster.getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort
        sql """CREATE TABLE $table (
            `k1` int(11) NULL,
            `k2` int(11) NULL,
            `v1` varchar(2048)
            )
            DUPLICATE KEY(`k1`, `k2`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            PROPERTIES (
            "replication_num"="1"
            );
        """
        sql """
            insert into $table values (1, 1, 'v1'), (2, 2, 'v2'), (3, 3, 'v3'), 
                (4, 4,'v4'), (5, 5, 'v5'), (6, 6, 'v6'), (100, 100, 'v100'), (7, 7, 'v7')
        """
        def cacheDirVersion2 = getTabletFileCacheDirFromBe(msHttpPort, table, 2)
        // 'rehash_tablet_after_be_dead_seconds=100'
        // be-1 dead, but not dead for a long time
        testCase(5, cacheDirVersion2)
        // be-1 dead, and dead for a long time
        testCase(200, cacheDirVersion2)
    }
}
