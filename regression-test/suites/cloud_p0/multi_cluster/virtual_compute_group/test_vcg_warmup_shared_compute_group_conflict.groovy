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

import groovy.json.JsonOutput
import org.apache.doris.regression.suite.ClusterOptions

suite('test_vcg_warmup_shared_compute_group_conflict', 'docker') {
    def options = new ClusterOptions()
    options.feNum = 3
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'sys_log_verbose_modules=org',
    ]
    options.cloudMode = true

    docker(options) {
        def ms = cluster.getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort
        def instanceId = "default_instance_id"
        def clusterA = "vcg_share_a"
        def clusterB = "vcg_share_b"
        def clusterC = "vcg_share_c"

        cluster.addBackend(1, clusterA)
        cluster.addBackend(1, clusterB)
        cluster.addBackend(1, clusterC)

        def addClusterApi = { requestBody, Closure checkFunc ->
            httpTest {
                endpoint msHttpPort
                uri "/MetaService/http/add_cluster?token=$token"
                body requestBody
                check checkFunc
            }
        }

        def firstBody = JsonOutput.toJson([
                instance_id: instanceId,
                cluster: [
                        cluster_name   : "vcgShareFirst",
                        cluster_id     : "vcgShareFirstId",
                        type           : "VIRTUAL",
                        cluster_names  : [clusterA, clusterB],
                        cluster_policy : [type: "ActiveStandby", active_cluster_name: clusterA,
                                          standby_cluster_names: [clusterB]]
                ]
        ])
        addClusterApi(firstBody) { respCode, body ->
            def json = parseJson(body)
            assertTrue(json.code.equalsIgnoreCase("OK"))
        }

        def secondBody = JsonOutput.toJson([
                instance_id: instanceId,
                cluster: [
                        cluster_name   : "vcgShareSecond",
                        cluster_id     : "vcgShareSecondId",
                        type           : "VIRTUAL",
                        cluster_names  : [clusterA, clusterC],
                        cluster_policy : [type: "ActiveStandby", active_cluster_name: clusterA,
                                          standby_cluster_names: [clusterC]]
                ]
        ])
        addClusterApi(secondBody) { respCode, body ->
            def json = parseJson(body)
            assertTrue(!json.code.equalsIgnoreCase("OK"))
        }

        def reverseSharedBody = JsonOutput.toJson([
                instance_id: instanceId,
                cluster: [
                        cluster_name   : "vcgShareThird",
                        cluster_id     : "vcgShareThirdId",
                        type           : "VIRTUAL",
                        cluster_names  : [clusterC, clusterB],
                        cluster_policy : [type: "ActiveStandby", active_cluster_name: clusterC,
                                          standby_cluster_names: [clusterB]]
                ]
        ])
        addClusterApi(reverseSharedBody) { respCode, body ->
            def json = parseJson(body)
            assertTrue(!json.code.equalsIgnoreCase("OK"))
        }
    }
}
