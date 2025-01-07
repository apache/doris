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
        'report_tablet_interval_seconds=1'
    ]
    options.setFeNum(3)
    options.setBeNum(3)
    options.cloudMode = true
    options.enableDebugPoints()

    def choseDeadBeIndex = 1
    def table = "test_clean_tablet_when_rebalance"

    def testCase = { deadTime -> 
        boolean beDeadLong = deadTime > rehashTime ? true : false
        logger.info("begin exec beDeadLong {}", beDeadLong)

        for (int i = 0; i < 5; i++) {
            sql """
                select * from $table
            """
        }

        def beforeGetFromFe = getTabletAndBeHostFromFe(table)
        def beforeGetFromBe = getTabletAndBeHostFromBe(cluster.getAllBackends())
        logger.info("before fe tablets {}, be tablets {}", beforeGetFromFe, beforeGetFromBe)
        beforeGetFromFe.each {
            assertTrue(beforeGetFromBe.containsKey(it.Key))
            assertEquals(beforeGetFromBe[it.Key], it.Value[1])
        }

        cluster.stopBackends(choseDeadBeIndex)
        dockerAwaitUntil(50) {
            def bes = sql_return_maparray("SHOW TABLETS FROM ${table}")
                    .collect { it.BackendId }
                    .unique()
            logger.info("bes {}", bes)
            bes.size() == 2
        }

        if (beDeadLong) {
            setFeConfig('enable_cloud_partition_balance', false)
            setFeConfig('enable_cloud_table_balance', false)
            setFeConfig('enable_cloud_global_balance', false)
        }
        sleep(deadTime * 1000)

        cluster.startBackends(choseDeadBeIndex)

        dockerAwaitUntil(50) {
           def bes = sql_return_maparray("SHOW TABLETS FROM ${table}")
                .collect { it.BackendId }
                .unique()
            logger.info("bes {}", bes)
            bes.size() == (beDeadLong ? 2 : 3)
        }
        for (int i = 0; i < 5; i++) {
            sql """
                select * from $table
            """
            sleep(1000)
        }
        beforeGetFromFe = getTabletAndBeHostFromFe(table)
        beforeGetFromBe = getTabletAndBeHostFromBe(cluster.getAllBackends())
        logger.info("after fe tablets {}, be tablets {}", beforeGetFromFe, beforeGetFromBe)
        beforeGetFromFe.each {
            assertTrue(beforeGetFromBe.containsKey(it.Key))
            assertEquals(beforeGetFromBe[it.Key], it.Value[1])
        }
    }

    docker(options) {
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
        // 'rehash_tablet_after_be_dead_seconds=10'
        // be-1 dead, but not dead for a long time
        testCase(5)
        // be-1 dead, and dead for a long time
        testCase(200)
    }
}
