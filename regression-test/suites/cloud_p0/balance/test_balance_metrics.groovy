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


suite('test_balance_metrics', 'docker') {
    if (!isCloudMode()) {
        return;
    }

    // Randomly enable or disable packed_file to test both scenarios
    def enablePackedFile = new Random().nextBoolean()
    logger.info("Running test with enable_packed_file=${enablePackedFile}")

    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=2',
        'sys_log_verbose_modules=org',
        'heartbeat_interval_second=1',
        'rehash_tablet_after_be_dead_seconds=3600',
        'cloud_warm_up_for_rebalance_type=without_warmup'
    ]
    options.beConfigs += [
        'report_tablet_interval_seconds=1',
        'schedule_sync_tablets_interval_s=18000',
        'disable_auto_compaction=true',
        'sys_log_verbose_modules=*',
        "enable_packed_file=${enablePackedFile}",
    ]
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true
    options.enableDebugPoints()

    def getFEMetrics = {ip, port, name ->
        def url = "http://${ip}:${port}/metrics"
        logger.info("getFEMetrics1, url: ${url}, name: ${name}")
        def metrics = new URL(url).text
        def pattern = java.util.regex.Pattern.compile(java.util.regex.Pattern.quote(name) + "\\s+(\\d+)")
        def matcher = pattern.matcher(metrics)
        if (matcher.find()) {
            def ret = matcher[0][1] as long
            logger.info("getFEMetrics2, ${url}, name:${name}, value:${ret}")
            return ret
        } else {
            throw new RuntimeException("${name} not found for ${ip}:${port}")
        }
    }
    
    def testCase = { table -> 
        def master = cluster.getMasterFe()
        def allEditlogNum = 0;
        def future = thread {
            awaitUntil(300) {
                def name = """doris_fe_cloud_partition_balance_num{cluster_id="compute_cluster_id", cluster_name="compute_cluster"}"""
                def value = getFEMetrics(master.host, master.httpPort, name)
                allEditlogNum += value
                logger.info("balance metrics value: ${value}, allEditlogNum: ${allEditlogNum}")
                return value == 0 && allEditlogNum > 0
            }
        }
        sql """CREATE TABLE $table (
            `k1` int(11) NULL,
            `v1` VARCHAR(2048)
            )
            DUPLICATE KEY(`k1`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k1`) BUCKETS 200
            PROPERTIES (
            "replication_num"="1"
            );
        """
        // generate some balance tasks
        cluster.addBackend(1)
        future.get()
        // wait for rebalancer to do its job
        assertTrue(allEditlogNum > 0, "balance metrics not increased")

        allEditlogNum = 0
        for (i in 0..30) {
            sleep(1000)
            def name = """doris_fe_cloud_partition_balance_num{cluster_id="compute_cluster_id", cluster_name="compute_cluster"}"""
            def value = getFEMetrics(master.host, master.httpPort, name)
            allEditlogNum += value
            logger.info("Final balance metrics value: ${value}, allEditlogNum: ${allEditlogNum}")
        }
        // after all balance tasks done, the metric should not increase
        assertTrue(allEditlogNum == 0, "final balance metrics not increased")

        cluster.addBackend(1, "other_cluster")
        sleep(5000)
        def name = """doris_fe_cloud_partition_balance_num{cluster_id="other_cluster_id", cluster_name="other_cluster"}"""
        def value = getFEMetrics(master.host, master.httpPort, name)
        logger.info("other cluster balance metrics value: ${value}")
    }

    docker(options) {
        testCase("test_balance_metrics_tbl")
    }
}
