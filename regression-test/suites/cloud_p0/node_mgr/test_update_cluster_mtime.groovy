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
import groovy.json.JsonSlurper
import org.apache.doris.regression.suite.ClusterOptions

suite('test_update_cluster_mtime', 'docker') {
    def options = new ClusterOptions()
    options.cloudMode = true

    def getClusterInfo = { clusterName ->
        def backends = sql "SHOW BACKENDS"
        def backend = backends.find {
            it[19].contains("\"compute_group_name\" : \"${clusterName}\"")
        }
        assertNotNull(backend, "No BE found for cluster: ${clusterName}")
        def tag = new JsonSlurper().parseText(backend[19])
        return [clusterId: tag.compute_group_id, uniqueId: tag.cloud_unique_id]
    }

    def getCluster = { ms, String uniqueId, String clusterId ->
        def responseHolder = []
        httpTest {
            endpoint ms.host + ':' + ms.httpPort
            uri '/MetaService/http/get_cluster_status?token=greedisgood9999'
            body JsonOutput.toJson([cloud_unique_ids: [uniqueId]])
            check { respCode, body ->
                assertEquals(200, respCode, "Failed to get cluster status: ${body}")
                def response = new JsonSlurper().parseText(body)
                assertTrue(response.code.equalsIgnoreCase('OK'), "Failed to get cluster status: ${body}")
                responseHolder << response.result.details[0].clusters.find { it.cluster_id == clusterId }
            }
        }
        assertFalse(responseHolder.isEmpty(), "Cluster ${clusterId} was not returned by MetaService")
        assertNotNull(responseHolder[0], "Cluster ${clusterId} was not returned by MetaService")
        return responseHolder[0]
    }

    def setClusterStatus = { ms, String uniqueId, String clusterId, String status ->
        def responseHolder = []
        httpTest {
            endpoint ms.host + ':' + ms.httpPort
            uri '/MetaService/http/set_cluster_status?token=greedisgood9999'
            body JsonOutput.toJson([
                cloud_unique_id: uniqueId,
                cluster: [cluster_id: clusterId, cluster_status: status]
            ])
            check { respCode, body ->
                assertEquals(200, respCode, "Failed to set cluster status: ${body}")
                def response = new JsonSlurper().parseText(body)
                assertTrue(response.code.equalsIgnoreCase('OK'), "Failed to set cluster status: ${body}")
                responseHolder << response
            }
        }
        assertFalse(responseHolder.isEmpty())
    }

    docker(options) {
        def clusterName = 'mtime_regression_cluster'
        cluster.addBackend(1, clusterName)
        def ms = cluster.getAllMetaservices().get(0)
        def clusterInfo = getClusterInfo(clusterName)

        def normalBefore = getCluster(ms, clusterInfo.uniqueId, clusterInfo.clusterId)
        sleep(1500)
        setClusterStatus(ms, clusterInfo.uniqueId, clusterInfo.clusterId, 'SUSPENDED')
        def suspended = getCluster(ms, clusterInfo.uniqueId, clusterInfo.clusterId)

        assertEquals('SUSPENDED', suspended.cluster_status)
        assertNotNull(suspended.mtime, 'Suspending a cluster must persist mtime')
        if (normalBefore.mtime != null) {
            assertTrue(suspended.mtime > normalBefore.mtime,
                    "Cluster mtime did not advance: before=${normalBefore.mtime}, after=${suspended.mtime}")
        }

        sleep(1500)
        setClusterStatus(ms, clusterInfo.uniqueId, clusterInfo.clusterId, 'NORMAL')
        def normalAfter = getCluster(ms, clusterInfo.uniqueId, clusterInfo.clusterId)
        assertEquals('NORMAL', normalAfter.cluster_status)
        assertNotNull(normalAfter.mtime, 'Resuming a cluster must persist mtime')
        assertTrue(normalAfter.mtime > suspended.mtime,
                "Cluster mtime did not advance on recovery: suspended=${suspended.mtime}, normal=${normalAfter.mtime}")
    }
}
