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

suite('test_rename_compute_group', 'docker, p0') {
    if (!isCloudMode()) {
        return;
    }
    def options = new ClusterOptions()
    options.feConfigs += [
        'sys_log_verbose_modules=org',
        'heartbeat_interval_second=1'
    ]
    options.setFeNum(2)
    options.setBeNum(3)
    options.cloudMode = true
    options.connectToFollower = true
    
    def user1 = "test_has_admin_auth_user"
    def user2 = "test_no_admin_auth_user"
    def table = "test_rename_compute_group_table"

    def get_instance_api = { msHttpPort, instance_id, check_func ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/get_instance?token=${token}&instance_id=${instance_id}"
            check check_func
        }
    }
    def findToDropUniqueId = { clusterId, hostIP, metaServices ->
            def ret = get_instance(metaServices)
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

    docker(options) {
        def clusterName = "newcluster1"
        // 添加一个新的cluster add_new_cluster
        cluster.addBackend(1, clusterName)
        def result = sql """SHOW COMPUTE GROUPS""";
        assertEquals(2, result.size())

        sql """CREATE USER $user1 IDENTIFIED BY 'Cloud123456' DEFAULT ROLE 'admin';"""
        sql """CREATE USER $user2 IDENTIFIED BY 'Cloud123456';"""
                // no cluster auth
        sql """GRANT SELECT_PRIV ON *.*.* TO ${user2}"""
        sql """CREATE TABLE $table (
            `k1` int(11) NULL,
            `k2` int(11) NULL
            )
            DUPLICATE KEY(`k1`, `k2`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_num"="1"
            );
        """

        // 1. test original compute group not exist in warehouse
        try {
            sql """ALTER SYSTEM RENAME COMPUTE GROUP notExistComputeGroup compute_cluster;"""
        } catch (Exception e) {
            logger.info("exception: {}", e.getMessage())
            assertTrue(e.getMessage().contains("compute group 'notExistComputeGroup' not found, unable to rename"))
        }
        
        // 2. test target compute group eq original compute group
        try {
            sql """ALTER SYSTEM RENAME COMPUTE GROUP compute_cluster compute_cluster;"""
        } catch (Exception e) {
            logger.info("exception: {}", e.getMessage())
            assertTrue(e.getMessage().contains("rename compute group original name eq new name"))
        }

        // 3. test target compute group exist in warehouse
        try {
            sql """ALTER SYSTEM RENAME COMPUTE GROUP compute_cluster newcluster1;"""
        } catch (Exception e) {
            logger.info("exception: {}", e.getMessage())
            assertTrue(e.getMessage().contains("compute group 'newcluster1' has existed in warehouse, unable to rename"))
        }
        // 4. test admin user can rename compute group
        connectInDocker(user1, 'Cloud123456') {
            sql """ALTER SYSTEM RENAME COMPUTE GROUP compute_cluster compute_cluster1;"""
            sql """sync"""
            result = sql_return_maparray """SHOW COMPUTE GROUPS;"""
            log.info("show compute group {}", result)

            assertTrue(result.stream().anyMatch(cluster -> cluster.Name == "compute_cluster1"))
            assertFalse(result.stream().anyMatch(cluster -> cluster.Name == "compute_cluster"))
            // use old compute group name
            try {
                sql """ use @compute_cluster"""
            } catch (Exception e) {
                logger.info("exception: {}", e.getMessage())
                assertTrue(e.getMessage().contains("Compute group (aka. Cloud cluster) compute_cluster not exist"))
            }

            sql """use @compute_cluster1"""

            // insert succ
            sql """
                insert into $table values (1, 1)
            """

            result = sql """
                select count(*) from $table
            """
            logger.info("select result {}", result)
            assertEquals(1, result[0][0])
        }

        // 5. test non admin user can't rename compute group
        connectInDocker(user2, 'Cloud123456') {
            try {
                sql """ALTER SYSTEM RENAME COMPUTE GROUP compute_cluster1 compute_cluster2;"""
            } catch (Exception e ) {
                logger.info("exception: {}", e.getMessage())
                assertTrue(e.getMessage().contains("Access denied; you need (at least one of) the (Node_priv,Admin_priv) privilege(s) for this operation")) 
            }
        }
        // 6. test target compute group is empty (no node), can succ, and old empty compute group will be drop
        // 调用http api 将add_new_cluster 下掉
        def tag = getCloudBeTagByName(clusterName)
        logger.info("tag = {}", tag)

        def jsonSlurper = new JsonSlurper()
        def jsonObject = jsonSlurper.parseText(tag)
        def cloudClusterId = jsonObject.compute_group_id
        def ms = cluster.getAllMetaservices().get(0)

        // tag = {"cloud_unique_id" : "compute_node_4", "compute_group_status" : "NORMAL", "private_endpoint" : "", "compute_group_name" : "newcluster1", "location" : "default", "public_endpoint" : "", "compute_group_id" : "newcluster1_id"}
        def toDropIP = cluster.getBeByIndex(4).host
        def toDropUniqueId = findToDropUniqueId.call(cloudClusterId, toDropIP, ms)
        drop_node(toDropUniqueId, toDropIP, 9050,
                0, "", clusterName, cloudClusterId, ms)
        // check have empty compute group
        def msHttpPort = ms.host + ":" + ms.httpPort
        def originalClusterId = ""
        get_instance_api(msHttpPort, "default_instance_id") {
            respCode, body ->
                log.info("before drop node get instance resp: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
                def clusters = json.result.clusters
                assertTrue(clusters.any { cluster -> 
                    cluster.cluster_name == clusterName && cluster.type == "COMPUTE"
                })
                def ret = clusters.find { cluster -> 
                    cluster.cluster_name == "compute_cluster1" && cluster.type == "COMPUTE"
                }
                originalClusterId = ret.cluster_id
                assertNotEquals("", originalClusterId)
        }
        Thread.sleep(11000)
        result = sql_return_maparray """SHOW COMPUTE GROUPS;"""
        logger.info("show compute group : {}", result)
        assertEquals(1, result.size())
        // after drop node, empty compute group not show
        assertFalse(result.stream().anyMatch(cluster -> cluster.Name == """$clusterName"""))

        sql """ALTER SYSTEM RENAME COMPUTE GROUP compute_cluster1 $clusterName;"""

        result = sql_return_maparray """SHOW COMPUTE GROUPS;"""
        logger.info("show compute group : {}", result)
        assertEquals(1, result.size())
        assertTrue(result.stream().anyMatch(cluster -> cluster.Name == """$clusterName"""))
        // check not have empty compute group
        get_instance_api(msHttpPort, "default_instance_id") {
            respCode, body ->
                log.info("after drop node get instance resp: ${body} ${respCode}".toString())
                def json = parseJson(body)
                assertTrue(json.code.equalsIgnoreCase("OK"))
                def clusters = json.result.clusters
                assertTrue(clusters.any { cluster -> 
                    cluster.cluster_name == clusterName && cluster.type == "COMPUTE"
                })
                def ret = clusters.find { cluster -> 
                    cluster.cluster_name == clusterName && cluster.type == "COMPUTE"
                }
                assertNotNull(ret)
                // after rename compute group id not changed
                assertEquals(originalClusterId, ret.cluster_id)
        }
    }
}
