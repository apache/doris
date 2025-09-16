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

// 1 create two physical cluster c1, c2, every cluster contains 1 be
// 2 create vcg, c1, c2 are sub compute group of vcg, adn c1 is active cg
// 3 use vcg
// 4 stop 1 bes of c1

suite('load_trigger_failover', 'multi_cluster,docker') {
    def options = new ClusterOptions()
    String tableName = "test_all_vcluster"
    String tbl = "test_virtual_compute_group_tbl"

    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'sys_log_verbose_modules=org',
    ]
    options.feNum = 3
    options.cloudMode = true

    def add_cluster_api = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/add_cluster?token=$token"
            body request_body
            check check_func
        }
    }

    def alter_cluster_info_api = { msHttpPort, request_body, check_func ->
        httpTest {
            endpoint msHttpPort
            uri "/MetaService/http/alter_vcluster_info?token=$token"
            body request_body
            check check_func
        }
    }

    options.connectToFollower = false

    for (def j = 0; j < 1; j++) {
        docker(options) {
            def ms = cluster.getAllMetaservices().get(0)
            def msHttpPort = ms.host + ":" + ms.httpPort
            logger.info("ms1 addr={}, port={}, ms endpoint={}", ms.host, ms.httpPort, msHttpPort)

            def clusterName1 = "newcluster1"
            // add cluster newcluster1
            cluster.addBackend(1, clusterName1)

            def clusterName2 = "newcluster2"
            // add cluster newcluster2
            cluster.addBackend(1, clusterName2)

            // add vcluster
            def normalVclusterName = "normalVirtualClusterName"
            def normalVclusterId = "normalVirtualClusterId"
            def vcgClusterNames = [clusterName1, clusterName2]
            def clusterPolicy = [type: "ActiveStandby", active_cluster_name: "${clusterName1}", standby_cluster_names: ["${clusterName2}"], failover_failure_threshold: 3]
            def clusterMap = [cluster_name: "${normalVclusterName}", cluster_id:"${normalVclusterId}", type:"VIRTUAL", cluster_names:vcgClusterNames, cluster_policy:clusterPolicy]
            def normalInstance = [instance_id: "${instance_id}", cluster: clusterMap]
            def jsonOutput = new JsonOutput()
            def normalVcgBody = jsonOutput.toJson(normalInstance)
            add_cluster_api.call(msHttpPort, normalVcgBody) {
                respCode, body ->
                    log.info("add normal vitural compute group http cli result: ${body} ${respCode}".toString())
                    def json = parseJson(body)
                    assertTrue(json.code.equalsIgnoreCase("OK"))
            }

            // show cluster
            sleep(5000)
            def showComputeGroup = sql_return_maparray """ SHOW COMPUTE GROUPS """
            log.info("show compute group {}", showComputeGroup)
            def vcgInShow = showComputeGroup.find { it.Name == normalVclusterName }
            assertNotNull(vcgInShow)
            assertTrue(vcgInShow.Policy.contains('"activeComputeGroup":"newcluster1","standbyComputeGroup":"newcluster2"'))
            assertTrue(vcgInShow.Policy.contains('"activeComputeGroup":"newcluster1","standbyComputeGroup":"newcluster2"'))

            def showResult = sql "show clusters"
            for (row : showResult) {
                println row
            }
            showResult = sql "show backends"
            for (row : showResult) {
                println row
            }

            // get be ip of clusterName1
            def jsonSlurper = new JsonSlurper()
            def cluster1Ips = showResult.findAll { entry ->
                def raw = entry[19]
                def info = (raw instanceof String) ? jsonSlurper.parseText(raw) : raw
                info.compute_group_name == clusterName1
            }.collect { entry ->
                entry[1]
            }
            log.info("backends of cluster1: ${clusterName1} ${cluster1Ips}".toString())

            def cluster2Ips = showResult.findAll { entry ->
                def raw = entry[19]
                def info = (raw instanceof String) ? jsonSlurper.parseText(raw) : raw
                info.compute_group_name == clusterName2
            }.collect { entry ->
                entry[1]
            }
            log.info("backends of cluster2: ${clusterName2} ${cluster2Ips}".toString())

            sql """use @${normalVclusterName}"""
            sql """ drop table if exists ${tableName} """

            sql """
                CREATE TABLE ${tbl} (
                  `k1` int(11) NULL,
                  `k2` char(5) NULL
                )
                DUPLICATE KEY(`k1`, `k2`)
                COMMENT 'OLAP'
                DISTRIBUTED BY HASH(`k1`) BUCKETS 1
                PROPERTIES (
                "replication_num"="1"
                )
            """

            cluster.stopBackends(4)
            sleep(60000)
            sql """
                insert into ${tbl} values (1, 'a');
            """

            sql """
                select * from ${tbl}
            """

            // show cluster
            showComputeGroup = sql_return_maparray """ SHOW COMPUTE GROUPS """
            log.info("show compute group {}", showComputeGroup)
            vcgInShow = showComputeGroup.find { it.Name == normalVclusterName }
            assertNotNull(vcgInShow)
            log.info("policy {}", vcgInShow.Policy)
            assertTrue(vcgInShow.Policy.contains('"activeComputeGroup":"newcluster2","standbyComputeGroup":"newcluster1"'))
        }
        // connect to follower, run again
        options.connectToFollower = true
    }
}
