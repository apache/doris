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

suite('test_clean_stale_rs_file_cache', 'docker') {
    if (!isCloudMode()) {
        return;
    }
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=1',
        'sys_log_verbose_modules=org',
        'heartbeat_interval_second=1'
    ]
    options.beConfigs += [
        'report_tablet_interval_seconds=1',
        'cumulative_compaction_min_deltas=5',
        'tablet_rowset_stale_sweep_by_size=false',
        'tablet_rowset_stale_sweep_time_sec=60',
        'vacuum_stale_rowsets_interval_s=10'
    ]
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true

    def table = "test_clean_stale_rs_file_cache"

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
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_num"="1"
            );
        """
        // version 2
        sql """
            insert into $table values (1, 1, 'v1'), (2, 2, 'v2'), (3, 3, 'v3')
        """
        def cacheDirVersion2 = getTabletFileCacheDirFromBe(msHttpPort, table, 2)
        // version 3
        sql """
            insert into $table values (10, 1, 'v1'), (20, 2, 'v2'), (30, 3, 'v3')
        """
        def cacheDirVersion3 = getTabletFileCacheDirFromBe(msHttpPort, table, 3)
        // version 4
        sql """
            insert into $table values (100, 1, 'v1'), (200, 2, 'v2'), (300, 3, 'v3')
        """
        // version 5
        sql """
            insert into $table values (1000, 1, 'v1'), (2000, 2, 'v2'), (3000, 3, 'v3')
        """
        // version 6
        sql """
            insert into $table values (10000, 1, 'v1'), (20000, 2, 'v2'), (30000, 3, 'v3')
        """

        def mergedCacheDir = cacheDirVersion2 + cacheDirVersion3.collectEntries { host, hashFiles ->
            [(host): cacheDirVersion2[host] ? (cacheDirVersion2[host] + hashFiles) : hashFiles]
        }
        for (int i = 0; i < 5; i++) {
            sql """
                select count(*) from $table
            """
        }
        def beforeGetFromFe = getTabletAndBeHostFromFe(table)
        logger.info("fe tablets {}, cache dir {}", beforeGetFromFe , mergedCacheDir)
        // wait compaction finish, and vacuum_stale_rowsets work
        sleep(80 * 1000)

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
}
