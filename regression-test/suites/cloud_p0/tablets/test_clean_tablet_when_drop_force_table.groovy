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


suite('test_clean_tablet_when_drop_force_table', 'docker') {
    if (!isCloudMode()) {
        return;
    }
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=1',
        'sys_log_verbose_modules=org',
        'heartbeat_interval_second=1',
        'rehash_tablet_after_be_dead_seconds=5'
    ]
    options.beConfigs += [
        'report_tablet_interval_seconds=1',
        'write_buffer_size=10240',
        'write_buffer_size_for_agg=10240'
    ]
    options.setFeNum(3)
    options.setBeNum(3)
    options.cloudMode = true
    options.enableDebugPoints()
    
    def testCase = { table, waitTime, useDp=false-> 
        def ms = cluster.getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort
        sql """CREATE TABLE $table (
            `k1` int(11) NULL,
            `k2` int(11) NULL,
            `v1` VARCHAR(2048)
            )
            DUPLICATE KEY(`k1`, `k2`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            PROPERTIES (
            "replication_num"="1"
            );
        """
        def random = new Random()
        def generateRandomString = { int length ->
            random.with {
                def chars = ('A'..'Z').collect() + ('a'..'z').collect() + ('0'..'9').collect()
                (1..length).collect { chars[nextInt(chars.size())] }.join('')
            }
        }
        def valuesList = (1..30000).collect { i -> 
            def randomStr = generateRandomString(2000)
            "($i, $i, '$randomStr')"
        }.join(", ")
        sql """
            set global max_allowed_packet = 1010241024
        """

        context.reconnectFe()
        sql """
            insert into $table values ${valuesList}
        """

        for (int i = 0; i < 5; i++) {
            sql """
                select count(*) from $table
            """
        }

        valuesList = (30001..60000).collect { i -> 
            def randomStr = generateRandomString(2000)
            "($i, $i, '$randomStr')"
        }.join(", ")
        sql """
            set global max_allowed_packet = 1010241024
        """
        context.reconnectFe()
        sql """
            insert into $table values ${valuesList}
        """

        // before drop table force
        def beforeGetFromFe = getTabletAndBeHostFromFe(table)
        def beforeGetFromBe = getTabletAndBeHostFromBe(cluster.getAllBackends())
        // version 2
        def cacheDirVersion2 = getTabletFileCacheDirFromBe(msHttpPort, table, 2)
        // version 3
        def cacheDirVersion3 = getTabletFileCacheDirFromBe(msHttpPort, table, 3)

        def mergedCacheDir = cacheDirVersion2 + cacheDirVersion3.collectEntries { host, hashFiles ->
            [(host): cacheDirVersion2[host] ? (cacheDirVersion2[host] + hashFiles) : hashFiles]
        }
        
        logger.info("fe tablets {}, be tablets {}, cache dir {}", beforeGetFromFe, beforeGetFromBe, mergedCacheDir)
        beforeGetFromFe.each {
            assertTrue(beforeGetFromBe.containsKey(it.Key))
            assertEquals(beforeGetFromBe[it.Key], it.Value[1])
        }
        if (useDp) {
            GetDebugPoint().enableDebugPointForAllBEs("WorkPoolCloudDropTablet.drop_tablet_callback.failed")
        }
        // after drop table force

        sql """
            DROP TABLE $table FORCE
        """
        def futrue
        if (useDp) {
            futrue = thread {
                sleep(10 * 1000)
                GetDebugPoint().disableDebugPointForAllBEs("WorkPoolCloudDropTablet.drop_tablet_callback.failed")
            }
        }
        def start = System.currentTimeMillis() / 1000
        // tablet can't find in be 
        awaitUntil(500) {
            def beTablets = getTabletAndBeHostFromBe(cluster.getAllBackends()).keySet()
            logger.info("before drop tablets {}, after tablets {}", beforeGetFromFe, beTablets)
            beforeGetFromFe.keySet().every { !getTabletAndBeHostFromBe(cluster.getAllBackends()).containsKey(it) }
        }
        logger.info("table {}, cost {}s", table, System.currentTimeMillis() / 1000 - start)
        assertTrue(System.currentTimeMillis() / 1000 - start > waitTime)
        if (useDp) {
            futrue.get()
        }

        sleep(25 * 1000)

        // check cache file has been deleted
        beforeGetFromFe.each {
            def tabletId = it.Key
            def backendId = it.Value[0]
            def backendHost = it.Value[1]
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

    docker(options) {
        // because rehash_tablet_after_be_dead_seconds=5
        testCase("test_clean_tablet_when_drop_force_table_1", 5)
        // report retry
        testCase("test_clean_tablet_when_drop_force_table_2", 10, true) 
    }
}
