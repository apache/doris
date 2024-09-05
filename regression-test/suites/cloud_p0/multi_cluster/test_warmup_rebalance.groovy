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
import groovy.json.JsonSlurper
import org.awaitility.Awaitility;
import static java.util.concurrent.TimeUnit.SECONDS;
import org.codehaus.groovy.runtime.IOGroovyMethods

suite('test_warmup_rebalance_in_cloud', 'multi_cluster') {
    if (!isCloudMode()) {
        return;
    }
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'enable_cloud_warm_up_for_rebalance=true',
        'cloud_tablet_rebalancer_interval_second=1',
        'cloud_balance_tablet_percent_per_run=0.5',
        'sys_log_verbose_modules=org',
        'cloud_pre_heating_time_limit_sec=600'
    ]
    options.setFeNum(2)
    options.setBeNum(3)
    options.cloudMode = true
    options.enableDebugPoints()
    def check = { String feLogPath ->
        log.info("search fe log path: {}", feLogPath)
        Map<String, List<String>> circularRebalanceMap = [:]
        boolean isCircularRebalanceDetected = false

        new File(feLogPath).text.tokenize('\n')
        .findAll { it =~ /pre cache ([0-9]+) from ([0-9]+) to ([0-9]+), cluster ([a-zA-Z0-9_]+)/ }
        .each { line ->
            def (tabletId, fromBe, toBe, clusterId) = (line =~ /pre cache ([0-9]+) from ([0-9]+) to ([0-9]+), cluster ([a-zA-Z0-9_]+)/)[0][1..-1]

            String clusterPreCacheKey = "$clusterId-$tabletId"

            if (!circularRebalanceMap.containsKey(clusterPreCacheKey)) {
                circularRebalanceMap[clusterPreCacheKey] = new ArrayList<>()
            }

            List<String> paths = circularRebalanceMap[clusterPreCacheKey]

            if (paths.contains(toBe)) {
                isCircularRebalanceDetected = true
                log.info("Circular rebalance detected for tabletId: {}, clusterId: {}", tabletId, clusterId)
                assertFalse(true)
            }

            paths << fromBe
            circularRebalanceMap[clusterPreCacheKey] = paths

            if (!paths.contains(toBe)) {
                paths << (toBe as String)
            }
        }

        if (!isCircularRebalanceDetected) {
            log.info("No circular rebalance detected.")
        }
    }

    docker(options) {
        def clusterName = "newcluster1"
        // 添加一个新的cluster add_new_cluster
        cluster.addBackend(2, clusterName)

        def ret = sql_return_maparray """show clusters"""
        log.info("show clusters: {}", ret)
        assertEquals(2, ret.size())

        GetDebugPoint().enableDebugPointForAllBEs("CloudBackendService.check_warm_up_cache_async.return_task_false")
        sql """set global forward_to_master=false"""

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
        INSERT INTO table100 VALUES (1, 1, 100);
        """

        dockerAwaitUntil(5) {
            ret = sql """ADMIN SHOW REPLICA DISTRIBUTION FROM table100"""
            log.info("replica distribution table100: {}", ret)
            ret.size() == 5
        }

        sql """use @newcluster1"""
        def result = sql_return_maparray """ADMIN SHOW REPLICA DISTRIBUTION FROM table100; """
        assertEquals(5, result.size())
        int replicaNum = 0

        for (def row : result) {
            log.info("replica distribution: ${row} ".toString())
            if (row.CloudClusterName == "newcluster1") {
                replicaNum = Integer.valueOf((String) row.ReplicaNum)
                assertTrue(replicaNum <= 25 && replicaNum >= 23)
            }
        }
        def fe1 = cluster.getFeByIndex(1)
        String feLogPath = fe1.getLogFilePath()
        // stop be id 1, 4
        cluster.stopBackends(1, 4)
        // check log
        sleep(10 * 1000)
        check feLogPath

        // start be id 1, 4
        cluster.startBackends(1, 4)
        GetDebugPoint().enableDebugPointForAllBEs("CloudBackendService.check_warm_up_cache_async.return_task_false")
        // check log
        sleep(10 * 1000)
        check feLogPath
    }
}
