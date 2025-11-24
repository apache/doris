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
import groovy.json.JsonOutput

suite('test_change_node_net', 'multi_cluster,docker') {
    def clusterOptions = [
        new ClusterOptions(),
        new ClusterOptions(),
    ]
    for (options in clusterOptions) {
        options.feConfigs += [
            'cloud_cluster_check_interval_second=5',
        ]
        options.feNum = 3
        options.cloudMode = true
    }
    clusterOptions[0].connectToFollower = true
    clusterOptions[1].connectToFollower = false

    def token = "greedisgood9999"
    def update_cluster_endpoint_api = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/update_cluster_endpoint?token=$token"
            body request_body
            check check_func
        }
    }

    def showClusterBackends = { clusterName ->
        // The new optimizer has a bug, all queries are forwarded to the master. Including show backend
        sql """set forward_to_master=false"""
        def bes = sql_return_maparray "show backends"
        def clusterBes = bes.findAll { be -> be.Tag.contains(clusterName) }
        def backendMap = clusterBes.collectEntries { be ->
            [(be.BackendId): be.Tag]
        }
        logger.info("Collected BackendId and Tag map: {}", backendMap)
        backendMap
    }
    for (options in clusterOptions) {
        docker(options) {
            def ms = cluster.getAllMetaservices().get(0)
            def msHttpPort = ms.host + ":" + ms.httpPort
            logger.info("ms addr={}, port={}, ms endpoint={}", ms.host, ms.httpPort, msHttpPort)

            def clusterName = "newcluster1"
            // 添加一个新的cluster add_new_cluster
            cluster.addBackend(3, clusterName)
        
            def result = sql """show clusters"""
            logger.info("show cluster1 : {}", result)

            def beforeBackendMap = showClusterBackends.call(clusterName)

            def tag = beforeBackendMap.entrySet().iterator().next().Value
            assertNotNull(tag)
            def jsonSlurper = new JsonSlurper()
            def jsonObject = jsonSlurper.parseText(tag)
            def cloudUniqueId = jsonObject.cloud_unique_id
            def clusterId = jsonObject.compute_group_id
            def before_public_endpoint = jsonObject.public_endpoint
            def after_private_endpoint = jsonObject.private_endpoint


            def changeCluster = [cluster_id: "${clusterId}", public_endpoint: "test_public_endpoint", private_endpoint: "test_private_endpoint"]
            def updateClusterEndpointBody = [cloud_unique_id: "${cloudUniqueId}", cluster: changeCluster]
            def jsonOutput = new JsonOutput()
            def updateClusterEndpointJson = jsonOutput.toJson(updateClusterEndpointBody)

            update_cluster_endpoint_api.call(msHttpPort, updateClusterEndpointJson) {
                respCode, body ->
                    def json = parseJson(body)
                    log.info("update cluster endpoint result: ${body} ${respCode} ${json}".toString())
            }

            def futrue = thread {
                // check 15s
                for (def i = 0; i < 15; i++) {
                    def afterBackendMap = showClusterBackends.call(clusterName)
                    if (i > 5) {
                        // cloud_cluster_check_interval_second = 5
                        afterBackendMap.each { key, value ->
                            assert value.contains("test_public_endpoint") : "Value for key ${key} does not contain 'test_public_endpoint'"
                            assert value.contains("test_private_endpoint") : "Value for key ${key} does not contain 'test_private_endpoint'"
                        }
                    }
                    // check beid not changed
                    assertEquals(afterBackendMap.keySet(), beforeBackendMap.keySet())
                    sleep(1 * 1000)
                }
            }
            futrue.get()
        }
    }
}
