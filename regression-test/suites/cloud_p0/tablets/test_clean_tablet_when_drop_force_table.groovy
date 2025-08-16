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
    
    def testCase = { tableName, waitTime, useDp=false-> 
        def ms = cluster.getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort
        sql """CREATE TABLE IF NOT EXISTS ${tableName} (
                  `k1` int(11) NULL,
                  `k2` tinyint(4) NULL,
                  `k3` smallint(6) NULL,
                  `k4` bigint(20) NULL,
                  `k5` largeint(40) NULL,
                  `k6` float NULL,
                  `k7` double NULL,
                  `k8` decimal(9, 0) NULL,
                  `k9` char(10) NULL,
                  `k10` varchar(1024) NULL,
                  `k11` text NULL,
                  `k12` date NULL,
                  `k13` datetime NULL
                ) ENGINE=OLAP
                DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        """
        def txnId = -1;
        // version 2
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'compress_type', 'gz'
            file 'all_types.csv.gz'
            time 10000 // limit inflight 10s
            setFeAddr cluster.getAllFrontends().get(0).host, cluster.getAllFrontends().get(0).httpPort

            check { loadResult, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${loadResult}".toString())
                def json = parseJson(loadResult)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(80000, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
                txnId = json.TxnId
            }
        }
     
        context.reconnectFe()
        for (int i = 0; i < 5; i++) {
            sql """
                select count(*) from $tableName
            """
        }
        // version 3
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'compress_type', 'gz'
            file 'all_types.csv.gz'
            time 10000 // limit inflight 10s
            setFeAddr cluster.getAllFrontends().get(0).host, cluster.getAllFrontends().get(0).httpPort

            check { loadResult, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${loadResult}".toString())
                def json = parseJson(loadResult)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(80000, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
                txnId = json.TxnId
            }
        }

        // before drop table force
        def beforeGetFromFe = getTabletAndBeHostFromFe(tableName)
        def beforeGetFromBe = getTabletAndBeHostFromBe(cluster.getAllBackends())
        // version 2
        def cacheDirVersion2 = getTabletFileCacheDirFromBe(msHttpPort, tableName, 2)
        // version 3
        def cacheDirVersion3 = getTabletFileCacheDirFromBe(msHttpPort, tableName, 3)

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
            DROP TABLE $tableName FORCE
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
        logger.info("table {}, cost {}s", tableName, System.currentTimeMillis() / 1000 - start)
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
