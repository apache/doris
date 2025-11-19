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
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import org.apache.doris.regression.suite.ClusterOptions

suite("test_cloud_add_backend_heartbeat", 'p0, docker') {
    if (!isCloudMode()) {
        return
    }

    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'sys_log_verbose_modules=org',
        'heartbeat_interval_second=1'
    ]
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true

    // curl "175.43.101.1:5000/MetaService/http/v1/injection_point?token=greedisgood9999&op=set&name=resource_manager::set_safe_drop_time&behavior=change_args&value=[-1]"
    def inject_to_ms_api = { msHttpPort, key, value, check_func ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/v1/injection_point?token=${token}&op=set&name=${key}&behavior=change_args&value=${value}"
            check check_func
        }
    }

    def enable_ms_inject_api = { msHttpPort, check_func ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/v1/injection_point?token=${token}&op=enable"
            check check_func
        }
    }

    docker(options) {
        def ms = cluster.getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort
        logger.info("ms1 addr={}, port={}, ms endpoint={}", ms.host, ms.httpPort, msHttpPort)
        // inject point, to change abort_txn_with_coordinator
        inject_to_ms_api.call(msHttpPort, "MetaServiceImpl::abort_txn_with_coordinator", URLEncoder.encode('[true]', "UTF-8")) {
                respCode, body ->
                    log.info("inject MetaServiceImpl::abort_txn_with_coordinator resp: ${body} ${respCode}".toString()) 
        }
        enable_ms_inject_api.call(msHttpPort) {
            respCode, body ->
            log.info("enable inject resp: ${body} ${respCode}".toString()) 
        }
        cluster.addBackend(10, "new_cluster")

        sql """admin set frontend config("cloud_tablet_rebalancer_interval_second"="3");"""

        cluster.restartBackends();

    }

}