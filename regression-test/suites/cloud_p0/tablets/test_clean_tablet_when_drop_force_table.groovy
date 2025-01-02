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
        'report_tablet_interval_seconds=1'
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
        // tablet : host
        def ret = [:]
        result.each {
            ret[it.TabletId] = bes[it.BackendId]
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

    def testCase = { table, waitTime, useDp=false-> 
        sql """CREATE TABLE $table (
            `k1` int(11) NULL,
            `k2` int(11) NULL
            )
            DUPLICATE KEY(`k1`, `k2`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            PROPERTIES (
            "replication_num"="1"
            );
        """
        sql """
            insert into $table values (1, 1), (2, 2), (3, 3)
        """

        for (int i = 0; i < 5; i++) {
            sql """
                select * from $table
            """
        }

        // before drop table force
        def beforeGetFromFe = getTabletAndBeHostFromFe(table)
        def beforeGetFromBe = getTabletAndBeHostFromBe.call()
        logger.info("fe tablets {}, be tablets {}", beforeGetFromFe, beforeGetFromBe)
        beforeGetFromFe.each {
            assertTrue(beforeGetFromBe.containsKey(it.Key))
            assertEquals(beforeGetFromBe[it.Key], it.Value)
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
        dockerAwaitUntil(50) {
            def beTablets = getTabletAndBeHostFromBe.call().keySet()
            logger.info("before drop tablets {}, after tablets {}", beforeGetFromFe, beTablets)
            beforeGetFromFe.keySet().every { !getTabletAndBeHostFromBe.call().containsKey(it) }
        }
        logger.info("table {}, cost {}s", table, System.currentTimeMillis() / 1000 - start)
        assertTrue(System.currentTimeMillis() / 1000 - start > waitTime)
        if (useDp) {
            futrue.get()
        }
    }

    docker(options) {
        // because rehash_tablet_after_be_dead_seconds=5
        testCase("test_clean_tablet_when_drop_force_table_1", 5)
        // report retry
        testCase("test_clean_tablet_when_drop_force_table_2", 10, true) 
    }
}
