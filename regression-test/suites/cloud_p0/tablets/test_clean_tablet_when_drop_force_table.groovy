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
    
    def backendIdToHost = { ->
        def spb = sql_return_maparray """SHOW BACKENDS"""
        def beIdToHost = [:]
        spb.each {
            beIdToHost[it.BackendId] = it.Host
        }
        beIdToHost 
    }

    def getTabletAndBeHostFromFe = { table ->
        def result = sql_return_maparray """SHOW TABLETS FROM $table"""
        def bes = backendIdToHost.call()
        // tablet : [backendId, host]
        def ret = [:]
        result.each {
            ret[it.TabletId] = [it.BackendId, bes[it.BackendId]]
        }
        ret
    }

    def getTabletAndBeHostFromBe = { ->
        def bes = cluster.getAllBackends()
        def ret = [:]
        bes.each { be ->
            // {"msg":"OK","code":0,"data":{"host":"128.2.51.2","tablets":[{"tablet_id":10560},{"tablet_id":10554},{"tablet_id":10552}]},"count":3}
            def data = Http.GET("http://${be.host}:${be.httpPort}/tablets_json?limit=all", true).data
            def tablets = data.tablets.collect { it.tablet_id as String }
            tablets.each{
                ret[it] = data.host
            }
        }
        ret
    }

    // get rowset_id segment_id from ms
    // curl '175.40.101.1:5000/MetaService/http/get_value?token=greedisgood9999&unicode&key_type=MetaRowsetKey&instance_id=default_instance_id&tablet_id=27700&version=2'
    def getSegmentFilesFromMs = { msHttpPort, tabletId, version, check_func ->
        httpTest {
            endpoint msHttpPort
            op "get"
            uri "/MetaService/http/get_value?token=greedisgood9999&unicode&key_type=MetaRowsetKey&instance_id=default_instance_id&tablet_id=${tabletId}&version=${version}"
            check check_func
        }
    }

    def getRowsetFileCacheDirFromBe = { beHttpPort, msHttpPort, tabletId, version -> 
        def hashValues = []
        def segmentFiles = []
        getSegmentFilesFromMs(msHttpPort, tabletId, version) {
            respCode, body ->
                def json = parseJson(body)
                logger.info("get tablet {} version {} from ms, response {}", tabletId, version, json)
                // {"rowset_id":"0","partition_id":"27695","tablet_id":"27700","txn_id":"7057526525952","tablet_schema_hash":0,"rowset_type":"BETA_ROWSET","rowset_state":"COMMITTED","start_version":"3","end_version":"3","version_hash":"0","num_rows":"1","total_disk_size":"895","data_disk_size":"895","index_disk_size":"0","empty":false,"load_id":{"hi":"-1646598626735601581","lo":"-6677682539881484579"},"delete_flag":false,"creation_time":"1736153402","num_segments":"1","rowset_id_v2":"0200000000000004694889e84c76391cfd52ec7db0a483ba","resource_id":"1","newest_write_timestamp":"1736153402","segments_key_bounds":[{"min_key":"AoAAAAAAAAAC","max_key":"AoAAAAAAAAAC"}],"txn_expiration":"1736167802","segments_overlap_pb":"NONOVERLAPPING","compaction_level":"0","segments_file_size":["895"],"index_id":"27697","schema_version":0,"enable_segments_file_size":true,"has_variant_type_in_schema":false,"enable_inverted_index_file_info":false}
                def segmentNum = json.num_segments as int
                def rowsetId = json.rowset_id_v2 as String
                segmentFiles = (0..<segmentNum).collect { i -> "${rowsetId}_${i}.dat" }
        }

        segmentFiles.each {
            // curl '175.40.51.3:8040/api/file_cache?op=hash&value=0200000000000004694889e84c76391cfd52ec7db0a483ba_0.dat'
            def data = Http.GET("http://${beHttpPort}/api/file_cache?op=hash&value=${it}", true)
            // {"hash":"2b79c649a1766dad371054ee168f0574"}
            logger.info("get tablet {} segmentFile {}, response {}", tabletId, it, data)
            hashValues << data.hash
        }
        hashValues
    }

    // get table's tablet file cache
    def getTabletFileCacheDirFromBe = { msHttpPort, table, version ->
        // beHost HashFile
        def beHostToHashFile = [:]

        def getTabletsAndHostFromFe = getTabletAndBeHostFromFe(table)
        getTabletsAndHostFromFe.each {
            def beHost = it.Value[1]
            def tabletId = it.Key
            def hashRet = getRowsetFileCacheDirFromBe(beHost + ":8040", msHttpPort, tabletId, version)
            hashRet.each {
                def hashFile = it
                if (beHostToHashFile.containsKey(beHost)) {
                    beHostToHashFile[beHost].add(hashFile)
                } else {
                    beHostToHashFile[beHost] = [hashFile]
                }
            }
        }
        beHostToHashFile
    }

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
        def beforeGetFromBe = getTabletAndBeHostFromBe.call()
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
        dockerAwaitUntil(500) {
            def beTablets = getTabletAndBeHostFromBe.call().keySet()
            logger.info("before drop tablets {}, after tablets {}", beforeGetFromFe, beTablets)
            beforeGetFromFe.keySet().every { !getTabletAndBeHostFromBe.call().containsKey(it) }
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
