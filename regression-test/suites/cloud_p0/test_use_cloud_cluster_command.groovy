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

suite('test_use_cloud_cluster_command') {
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
    
    def user = "test_use_cloud_cluster_command_user"
    def table = "test_use_cloud_cluster_command_table"

    def get_instance_api = { msHttpPort, instance_id, check_func ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/get_instance?token=${token}&instance_id=${instance_id}"
            check check_func
        }
    }
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

    docker(options) {
        def clusterName = "newcluster_usecloudcluster"
        cluster.addBackend(1, clusterName)
        def result = sql """SHOW COMPUTE GROUPS""";
        assertEquals(2, result.size())

        sql """CREATE USER $user IDENTIFIED BY 'Cloud123456' DEFAULT ROLE 'admin';"""
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

        try {
            sql """ALTER SYSTEM RENAME COMPUTE GROUP notExistComputeGroup compute_cluster;"""
        } catch (Exception e) {
            logger.info("exception: {}", e.getMessage())
            assertTrue(e.getMessage().contains("compute group 'notExistComputeGroup' not found, unable to rename"))
        }
        
        try {
            sql """ALTER SYSTEM RENAME COMPUTE GROUP compute_cluster compute_cluster;"""
        } catch (Exception e) {
            logger.info("exception: {}", e.getMessage())
            assertTrue(e.getMessage().contains("rename compute group original name eq new name"))
        }

        try {
            sql """ALTER SYSTEM RENAME COMPUTE GROUP compute_cluster newcluster_usecloudcluster;"""
        } catch (Exception e) {
            logger.info("exception: {}", e.getMessage())
            assertTrue(e.getMessage().contains("compute group 'newcluster_usecloudcluster' has existed in warehouse, unable to rename"))
        }

        connectInDocker(user, 'Cloud123456') {
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
    }
}
