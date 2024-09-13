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
import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

suite('test_multi_followr_in_cloud', 'multi_cluster, docker') {
    if (!isCloudMode()) {
        return
    }
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'sys_log_verbose_modules=org',
        'heartbeat_interval_second=1'
    ]
    options.setFeNum(3)
    options.setBeNum(1)
    options.cloudMode = true
    options.connectToFollower = true
    options.useFollowersMode = true

    docker(options) {
        def f = "FOLLOWER"
        def o = "OBSERVER"
        def check = { int feNum, Closure checkFunc ->
            def ret = sql_return_maparray """SHOW FRONTENDS"""
            assertEquals(feNum, ret.size())
            Boolean hasMaster = false
            checkFunc.call(ret)
            ret.each { row ->
                if (Boolean.parseBoolean(row.IsMaster)) {
                    hasMaster = true
                }
            }
            assertTrue(hasMaster)
        }

        def transferType = { String old -> 
            if (old.contains(f)) {
                return "FE_FOLLOWER"
            } else if (old.contains(o)) {
                return "FE_OBSERVER"
            }
        }

        check(3) { ret ->
            ret.each {
                assertTrue(it.Role.contains(f))
            }
        }
        // get fe clusterName
        def result = sql_return_maparray """ADMIN SHOW FRONTEND CONFIG LIKE '%cloud_sql_server_cluster_name%'"""
        def feClusterName = result.Value[0]
        result = sql_return_maparray """ADMIN SHOW FRONTEND CONFIG LIKE '%cloud_sql_server_cluster_id%'"""
        def feClusterId = result.Value[0]
        log.info("fe clusterName: {}, clusterId: {} ", feClusterName, feClusterId)
        def toDropIP
        def toDropPort
        def toDropType
        def toDropUniqueId
        // add new follower
        cluster.addFrontend(1, true)
        dockerAwaitUntil(5) {
            def ret = sql """SHOW FRONTENDS"""
            log.info("show frontends: {}", ret)
            ret.size() == 4
        }
        check(4) { def ret ->
            ret.each {
                assertTrue(it.Role.contains(f))
                if (!Boolean.parseBoolean(it.IsMaster)) {
                    toDropIP = it.Host
                    toDropPort = it.EditLogPort
                    toDropType = transferType(it.Role)
                }
            }
        }
        log.info("ip: {}, port: {}, type: {}, uniqueId: {}", toDropIP, toDropPort, toDropType, toDropUniqueId)
        def ms = cluster.getAllMetaservices().get(0)
        logger.info("ms addr={}, port={}", ms.host, ms.httpPort)
        // drop a follwer
        def findToDropUniqueId = { clusterId, hostIP, metaServices ->
            ret = get_instance(metaServices)
            def toDropCluster = ret.clusters.find {
                it.cluster_id.contains(clusterId)
            }
            log.info("toDropCluster: {}", toDropCluster)
            def toDropNode = toDropCluster.nodes.find {
                it.ip.contains(hostIP)
            }
            log.info("toDropNode: {}", toDropNode)
            assertNotNull(toDropCluster)
            assertNotNull(toDropNode)
            toDropNode.cloud_unique_id
        }

        toDropUniqueId = findToDropUniqueId.call(feClusterId, toDropIP, ms)
        log.info("to Drop1 ip: {}, port: {}, type: {}, uniqueId: {}", toDropIP, toDropPort, toDropType, toDropUniqueId)

        drop_node(toDropUniqueId, toDropIP, 0,
                    toDropPort, toDropType, feClusterName, feClusterId, ms)

        dockerAwaitUntil(5) {
            def ret = sql """SHOW FRONTENDS"""
            log.info("show frontends: {}", ret)
            ret.size() == 3
        }

        check(3) { def ret ->
            ret.each {
                assertTrue(it.Role.contains(f))
            }
        }

        // add a observer
        cluster.addFrontend(1)
        check(4) { def ret ->
            int observerNum = 0
            int followerNum = 0
            ret.each {
                if (it.Role.contains(o)) {
                    observerNum++;
                    toDropIP = it.Host
                    toDropPort = it.EditLogPort
                    toDropType = transferType(it.Role)
                } else if (it.Role.contains(f)) {
                    followerNum++;
                }
            }
            assertEquals(1, observerNum)
            assertEquals(3, followerNum)
        }
        
        toDropUniqueId = findToDropUniqueId.call(feClusterId, toDropIP, ms)
        // drop observer
        drop_node(toDropUniqueId, toDropIP, 0,
                    toDropPort, toDropType, feClusterName, feClusterId, ms)

        dockerAwaitUntil(5) {
            def ret = sql """SHOW FRONTENDS"""
            log.info("show frontends: {}", ret)
            ret.size() == 3
        }

        check(3) { def ret ->
            int observerNum = 0
            int followerNum = 0
            ret.each {
                if (it.Role.contains(o)) {
                    observerNum++;
                } else if (it.Role.contains(f)){
                    followerNum++;
                }
            }
            assertEquals(0, observerNum)
            assertEquals(3, followerNum)
        }
    }
}
