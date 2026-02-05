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
import org.codehaus.groovy.runtime.IOGroovyMethods

suite('test_expanding_node_balance', 'docker') {
    if (!isCloudMode()) {
        return;
    }

    // Randomly enable or disable packed_file to test both scenarios
    def enablePackedFile = new Random().nextBoolean()
    logger.info("Running test with enable_packed_file=${enablePackedFile}")

    def clusterOptions = [
        new ClusterOptions(),
        new ClusterOptions(),
        new ClusterOptions(),
    ]

    for (options in clusterOptions) {
        options.feConfigs += [
            'cloud_cluster_check_interval_second=1',
            'cloud_tablet_rebalancer_interval_second=20',
            'sys_log_verbose_modules=org',
            'heartbeat_interval_second=1',
            'rehash_tablet_after_be_dead_seconds=3600',
            'cloud_warm_up_for_rebalance_type=peer_read_async_warmup',
            // disable Auto Analysis Job Executor
            'auto_check_statistics_in_minutes=60',
        ]
        options.beConfigs += [
            "enable_packed_file=${enablePackedFile}",
        ]
        options.cloudMode = true
        options.setFeNum(1)
        options.setBeNum(1)
        options.enableDebugPoints()
    }


    def testCase = { command, expectCost ->
        sql """
        CREATE TABLE `fact_sales` (
            `order_id` varchar(255) NOT NULL,
            `order_line_id` varchar(255) NOT NULL,
            `order_date` date NOT NULL,
            `time_of_day` varchar(50) NOT NULL,
            `season` varchar(50) NOT NULL,
            `month` int NOT NULL,
            `location_id` varchar(255) NOT NULL,
            `region` varchar(100) NOT NULL,
            `product_name` varchar(255) NOT NULL,
            `quantity` int NOT NULL,
            `sales_amount` double NOT NULL,
            `discount_percentage` int NOT NULL,
            `product_id` varchar(255) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`order_id`, `order_line_id`)
        DISTRIBUTED BY HASH(`order_id`) BUCKETS 256
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        )
        """

        cluster.addBackend(15, "compute_cluster")

        sql """
        $command
        """
        def begin = System.currentTimeMillis();
        awaitUntil(1000, 10) {
            def showRet = sql_return_maparray """ADMIN SHOW REPLICA DISTRIBUTION FROM fact_sales"""
            logger.info("show result {}", showRet)
            showRet.any { row -> 
                Integer.valueOf((String) row.ReplicaNum) == 16
            }
        }
        def cost = (System.currentTimeMillis() - begin) / 1000;
        log.info("exec command: {}\n  time cost: {}s", command, cost)
        assertTrue(cost < expectCost, "cost assert wrong")
    }

    docker(clusterOptions[0]) {
        def command = 'select 1'; 
        // assert < 300s
        testCase(command, 300)
    }

    docker(clusterOptions[1]) {
        def command = 'admin set frontend config("cloud_tablet_rebalancer_interval_second"="0");' 
        // assert < 50s
        testCase(command, 50)
    }

    docker(clusterOptions[2]) {
        GetDebugPoint().enableDebugPointForAllFEs("CloudTabletRebalancer.balanceEnd.tooLong")
        // do nothing
        def command = 'select 1'
        // assert < 50s
        testCase(command, 50)
    }
}
