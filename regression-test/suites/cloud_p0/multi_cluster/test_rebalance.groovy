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

suite('test_rebalance_in_cloud', 'multi_cluster,docker') {
    if (!isCloudMode()) {
        return;
    }

    def clusterOptions = [
        new ClusterOptions(),
        new ClusterOptions(),
    ]
    for (options in clusterOptions) {
        options.setFeNum(3)
        options.setBeNum(1)
        options.cloudMode = true
        options.connectToFollower = true
        options.enableDebugPoints()
        options.feConfigs += [
            'cloud_cluster_check_interval_second=1',
            'cloud_tablet_rebalancer_interval_second=1',
            'cloud_balance_tablet_percent_per_run=0.5',

            'sys_log_verbose_modules=org',
        ]
    }
    clusterOptions[0].feConfigs += ['enable_cloud_warm_up_for_rebalance=true',             'cloud_pre_heating_time_limit_sec=300']
    clusterOptions[1].feConfigs += ['enable_cloud_warm_up_for_rebalance=false']


    for (int i = 0; i < clusterOptions.size(); i++) {
        log.info("begin warm up {}", i == 0 ? "ON" : "OFF")
        docker(clusterOptions[i]) {
            sql """
                CREATE TABLE table100 (
                class INT,
                id INT,
                score INT SUM
                )
                AGGREGATE KEY(class, id)
                DISTRIBUTED BY HASH(class) BUCKETS 48
            """

            sql """
                CREATE TABLE table_p2 ( k1 int(11) NOT NULL, k2 varchar(20) NOT NULL, k3 int sum NOT NULL )
                AGGREGATE KEY(k1, k2)
                PARTITION BY RANGE(k1) (
                PARTITION p1992 VALUES [("-2147483648"), ("19930101")),
                PARTITION p1993 VALUES [("19930101"), ("19940101")),
                PARTITION p1994 VALUES [("19940101"), ("19950101")),
                PARTITION p1995 VALUES [("19950101"), ("19960101")),
                PARTITION p1996 VALUES [("19960101"), ("19970101")),
                PARTITION p1997 VALUES [("19970101"), ("19980101")),
                PARTITION p1998 VALUES [("19980101"), ("19990101")))
                DISTRIBUTED BY HASH(k1) BUCKETS 3
            """
            GetDebugPoint().enableDebugPointForAllFEs("CloudReplica.getBackendIdImpl.primaryClusterToBackends");
            sql """set global forward_to_master=false"""
            
            // add a be
            cluster.addBackend(1, null)
            
            awaitUntil(30) {
                def bes = sql """show backends"""
                log.info("bes: {}", bes)
                bes.size() == 2
            }

            awaitUntil(5) {
                def ret = sql """ADMIN SHOW REPLICA DISTRIBUTION FROM table100"""
                log.info("replica distribution table100: {}", ret)
                ret.size() == 2
            }

            def result = sql_return_maparray """ADMIN SHOW REPLICA DISTRIBUTION FROM table100; """
            assertEquals(2, result.size())
            int replicaNum = 0

            for (def row : result) {
                log.info("replica distribution: ${row} ".toString())
                replicaNum = Integer.valueOf((String) row.ReplicaNum)
                if (replicaNum == 0) {
                    // due to debug point, observer not hash replica
                } else {
                    assertTrue(replicaNum <= 25 && replicaNum >= 23)
                }
            }

            awaitUntil(5) {
                def ret = sql """ADMIN SHOW REPLICA DISTRIBUTION FROM table_p2 PARTITION(p1992)"""
                log.info("replica distribution table_p2: {}", ret)
                ret.size() == 2
            }


            result = sql_return_maparray """ ADMIN SHOW REPLICA DISTRIBUTION FROM table_p2 PARTITION(p1992) """
            assertEquals(2, result.size())
            for (def row : result) {
                replicaNum = Integer.valueOf((String) row.ReplicaNum)
                log.info("replica distribution: ${row} ".toString())
                if (replicaNum != 0) {
                    assertTrue(replicaNum <= 2 && replicaNum >= 1)
                }
            }

            result = sql_return_maparray """ ADMIN SHOW REPLICA DISTRIBUTION FROM table_p2 PARTITION(p1993) """
            assertEquals(2, result.size())
            for (def row : result) {
                replicaNum = Integer.valueOf((String) row.ReplicaNum)
                log.info("replica distribution: ${row} ".toString())
                if (replicaNum != 0) {
                    assertTrue(replicaNum <= 2 && replicaNum >= 1)
                }
            }

            result = sql_return_maparray """ ADMIN SHOW REPLICA DISTRIBUTION FROM table_p2 PARTITION(p1994) """
            assertEquals(2, result.size())
            for (def row : result) {
                replicaNum = Integer.valueOf((String) row.ReplicaNum)
                log.info("replica distribution: ${row} ".toString())
                if (replicaNum != 0) {
                    assertTrue(replicaNum <= 2 && replicaNum >= 1)
                }
            }

            result = sql_return_maparray """ ADMIN SHOW REPLICA DISTRIBUTION FROM table_p2 PARTITION(p1995) """
            assertEquals(2, result.size())
            for (def row : result) {
                replicaNum = Integer.valueOf((String) row.ReplicaNum)
                log.info("replica distribution: ${row} ".toString())
                if (replicaNum != 0) {
                    assertTrue(replicaNum <= 2 && replicaNum >= 1)
                }
            }

            result = sql_return_maparray """ ADMIN SHOW REPLICA DISTRIBUTION FROM table_p2 PARTITION(p1996) """
            assertEquals(2, result.size())
            for (def row : result) {
                replicaNum = Integer.valueOf((String) row.ReplicaNum)
                log.info("replica distribution: ${row} ".toString())
                if (replicaNum != 0) {
                    assertTrue(replicaNum <= 2 && replicaNum >= 1)
                }
            }

            result = sql_return_maparray """ ADMIN SHOW REPLICA DISTRIBUTION FROM table_p2 PARTITION(p1997) """
            assertEquals(2, result.size())
            for (def row : result) {
                replicaNum = Integer.valueOf((String) row.ReplicaNum)
                log.info("replica distribution: ${row} ".toString())
                if (replicaNum != 0) {
                    assertTrue(replicaNum <= 2 && replicaNum >= 1)
                }
            }

            if (i == 1) {
                // just test warm up
                return
            }

            GetDebugPoint().enableDebugPointForAllFEs("CloudTabletRebalancer.checkInflghtWarmUpCacheAsync.beNull");
            // add a be
            cluster.addBackend(1, null)
            // warm up
            sql """admin set frontend config("enable_cloud_warm_up_for_rebalance"="true")"""

            // test rebalance thread still work
            awaitUntil(30) {
                def bes = sql """show backends"""
                log.info("bes: {}", bes)
                bes.size() == 3
            }

            awaitUntil(5) {
                def ret = sql """ADMIN SHOW REPLICA DISTRIBUTION FROM table100"""
                log.info("replica distribution table100: {}", ret)
                ret.size() == 3
            }

            result = sql_return_maparray """ADMIN SHOW REPLICA DISTRIBUTION FROM table100; """
            assertEquals(3, result.size())
            log.info("replica distribution: ${result} ".toString())

            // test 10s not balance, due to debug point
            for (int j = 0; j < 10; j++) {
                assertTrue(result.any { row ->  
                    Integer.valueOf((String) row.ReplicaNum) == 0 
                })
                sleep(1 * 1000)
            }
            GetDebugPoint().disableDebugPointForAllFEs("CloudTabletRebalancer.checkInflghtWarmUpCacheAsync.beNull");
            awaitUntil(10) {
                def ret = sql_return_maparray """ADMIN SHOW REPLICA DISTRIBUTION FROM table100"""
                log.info("replica distribution table100: {}", ret)
                ret.any { row -> 
                    Integer.valueOf((String) row.ReplicaNum) == 16 
                }
            }
        }
        logger.info("Successfully run {} times", i + 1)
    }
}
