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
import groovy.json.JsonOutput

suite('test_drop_cluster_clean_metrics', 'docker') {
    if (!isCloudMode()) {
        return;
    }
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
        'sys_log_verbose_modules=*'
    ]
    options.setFeNum(2)
    options.setBeNum(2)
    options.cloudMode = true
    options.enableDebugPoints()

    def drop_cluster_api = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/drop_cluster?token=$token"
            body request_body
            check check_func
        }
    }

    def getFEMetrics = {ip, port, name ->
        def url = "http://${ip}:${port}/metrics"
        logger.info("getFEMetrics1, url: ${url}, name: ${name}")
        def metrics = new URL(url).text

        def metricLinePattern = java.util.regex.Pattern.compile(java.util.regex.Pattern.quote(name))

        def matcher = metricLinePattern.matcher(metrics)
        boolean found = false
        while (matcher.find()) {
            found = true
            logger.info("getFEMetrics MATCH FOUND: ${matcher.group(0)}")
        }

        if (found) {
            return true
        } else {
            def snippet = metrics.length() > 2000 ? metrics.substring(0, 2000) + "..." : metrics
            logger.info("getFEMetrics NO MATCH for name=${name}, metrics snippet:\n${snippet}")
            return false
        }
    }

    def testCase = { -> 
        def ms = cluster.getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort
        def fe = cluster.getOneFollowerFe();
        sleep(3000) // wait for metrics ready
        def metrics1 = """cluster_id="compute_cluster_id","""
        assertTrue(getFEMetrics(fe.host, fe.httpPort, metrics1))

        // drop compute cluster
        def beClusterMap = [cluster_id:"compute_cluster_id"]
        def instance = [instance_id: "default_instance_id", cluster: beClusterMap]
        def jsonOutput = new JsonOutput()
        def dropFeClusterBody = jsonOutput.toJson(instance)
        drop_cluster_api.call(msHttpPort, dropFeClusterBody) {
            respCode, body ->
                log.info("drop fe cluster http cli result: ${body} ${respCode}".toString())
                def json = parseJson(body)
        }
        sleep(3000) // wait for metrics cleaned
        assertFalse(getFEMetrics(fe.host, fe.httpPort, metrics1))

        cluster.addBackend(2, "new_cluster") 

        sleep(3000) // wait for metrics cleaned
        def metrics2 = """cluster_id="new_cluster_id","""
        assertTrue(getFEMetrics(fe.host, fe.httpPort, metrics2))
    }

    docker(options) {
        testCase()
    }
}