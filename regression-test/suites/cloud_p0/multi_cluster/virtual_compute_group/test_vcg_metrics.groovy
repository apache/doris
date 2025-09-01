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

suite('test_vcg_metrics', 'multi_cluster,docker') {
    def options = new ClusterOptions()
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

    options.connectToFollower = false
    def tbl = "test_virtual_compute_group_metrics_tbl"

    for (def j = 0; j < 2; j++) {
        docker(options) {
            def ms = cluster.getAllMetaservices().get(0)
            def msHttpPort = ms.host + ":" + ms.httpPort
            logger.info("ms1 addr={}, port={}, ms endpoint={}", ms.host, ms.httpPort, msHttpPort)

            def clusterName1 = "newcluster1"
            // 添加一个新的cluster newcluster1
            cluster.addBackend(2, clusterName1)

            def clusterName2 = "newcluster2"
            // 添加一个新的cluster newcluster2
            cluster.addBackend(3, clusterName2) 

            // create test user
            def user = "regression_test_cloud_user"
            sql """create user ${user} identified by 'Cloud12345'"""
            sql """grant select_priv,load_priv on *.*.* to ${user}"""

            // add one vcg, test normal vcg
            // contain newCluster1, newCluster2
            def normalVclusterName = "normalVirtualClusterName"
            def normalVclusterId = "normalVirtualClusterId"
            def vcgClusterNames = [clusterName1, clusterName2]
            def clusterPolicy = [type: "ActiveStandby", active_cluster_name: "${clusterName1}", standby_cluster_names: ["${clusterName2}"]]
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

            // grant usage_priv on cluster vcg to user
            sql """GRANT USAGE_PRIV ON CLUSTER '${normalVclusterName}' TO '${user}'"""
            // show grant
            def result = sql_return_maparray """show grants for '${user}'"""
            log.info("show grant for ${user}, ret={}", result)
            def ret = result.find {it.CloudClusterPrivs == "normalVirtualClusterName: Cluster_usage_priv"}
            // use vcg
            
            sql """use @${normalVclusterName}"""
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
            );
            """

            sql """
                insert into ${tbl} (k1, k2) values (1, "10");
            """

            result = sql """select count(*) from ${tbl}"""
            log.info("result = {}", result)
            assertEquals(result.size(), 1)

            def db = context.dbName
            connectInDocker(user, 'Cloud12345') {
                sql """insert into ${db}.${tbl} (k1, k2) values (2, "20")"""
                result = sql """select * from ${db}.${tbl}"""
                assertEquals(result.size(), 2)
            }

            def fe = cluster.getFeByIndex(j + 1)

            httpTest {
                endpoint fe.host + ":" + fe.httpPort
                uri "/metrics"
                op "get"
                check { code, body ->
                    logger.debug("code:${code} body:${body}");
                    assertEquals(200, code)
                    assertTrue(body.contains("""doris_fe_query_total{cluster_id="normalVirtualClusterId", cluster_name="normalVirtualClusterName"} 1"""))
                    assertTrue(body.contains("""doris_fe_query_total{cluster_id="newcluster2_id", cluster_name="newcluster2"} 0"""))
                }
            }

            // stop clusterName1, switch active
            cluster.stopBackends(4, 5)

            // test warm up job destory and generate new jobs
            dockerAwaitUntil(50, 3) { 
                sql """USE @${normalVclusterName}"""
                sql """select count(*) from ${tbl}"""
                showComputeGroup = sql_return_maparray """ SHOW COMPUTE GROUPS """
                vcgInShow = showComputeGroup.find { it.Name == normalVclusterName }
                vcgInShow.Policy.contains('"activeComputeGroup":"newcluster2","standbyComputeGroup":"newcluster1"')
            }

            // check after switch metrics
            httpTest {
                endpoint fe.host + ":" + fe.httpPort
                uri "/metrics"
                op "get"
                check { code, body ->
                    logger.debug("code:${code} body:${body}");
                    assertEquals(200, code)
                    assertFalse(body.contains("""doris_fe_query_total{cluster_id="normalVirtualClusterId", cluster_name="normalVirtualClusterName"} 1"""))
                    assertFalse(body.contains("""doris_fe_query_total{cluster_id="newcluster2_id", cluster_name="newcluster2"} 0"""))
                }
            }
        }


        // connect to follower, run again
        options.connectToFollower = true 
        logger.info("Successfully run {} times", j + 1)
    }
}
