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

suite('test_multi_default_compute_groups', 'multi_cluster, docker') {
    if (!isCloudMode()) {
        return
    }
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'sys_log_verbose_modules=org',
        'heartbeat_interval_second=1'
    ]
    options.setFeNum(2)
    options.setBeNum(1)
    options.cloudMode = true
    options.connectToFollower = true

    def getProperty = { user, property ->
        def result = sql_return_maparray """SHOW PROPERTY FOR ${user}""" 
        result.find {
            it.Key == property as String
        }
    }

    def setAndCheckDefaultCluster = { user, validCluster ->
        sql """set property for ${user} 'DEFAULT_CLOUD_CLUSTER' = '$validCluster'"""
        def ret1 = getProperty(user, "default_cloud_cluster")
        def ret2 = getProperty(user, "default_compute_group")
        assertEquals(ret1.Value as String, validCluster)
        assertEquals(ret1.Value as String, ret2.Value as String)
    }

    def getShowCluster = { clusterName ->
        def result = sql_return_maparray """show clusters"""
        result.find {
            it.cluster == clusterName as String
        }
    }

    docker(options) {
        cluster.addBackend(1, "clusterName1")
        cluster.addBackend(1, "clusterName2")
        
        def result = sql_return_maparray """show clusters"""
        assertEquals(3, result.size())
        logger.info("show cluster : {}", result) 
        
        def user = "testUser"
        sql """CREATE USER $user IDENTIFIED BY 'Cloud123456'"""
        sql """grant usage_priv on cluster clusterName1 to ${user};"""
        sql """grant usage_priv on cluster clusterName2 to ${user};"""
        sql """grant select_priv, load_priv on *.*.* to ${user};"""

        sql """CREATE TABLE `test` (
            `a` bigint NULL,
            `b` int NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`a`)
            DISTRIBUTED BY HASH(`a`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
        """

        // case1, enable_multi_default_compute_group, default value false
        setFeConfig('enable_multi_default_compute_group', 'false')
        test {
            sql """set property for ${user} 'default_compute_group' = 'not_exist1, compute_cluster';"""
            // in enable_multi_default_compute_group = false, but use ','. user will see exception 
            // in fe.log will see `not enable multi default compute group, but use ','`
            exception "Compute group (aka. Cloud cluster) not_exist1, compute_cluster not exist"
        }

        test {
            sql """set property for ${user} 'default_compute_group' = 'clusterName1, clusterName2';"""
            // in enable_multi_default_compute_group = false, but use ','. user will see exception
            // in fe.log will see `not enable multi default compute group, but use ','`
            exception "Compute group (aka. Cloud cluster) clusterName1, clusterName2 not exist"
        }

        // case2, enable_multi_default_compute_group = true
        setFeConfig('enable_multi_default_compute_group', 'true')
        test {
            sql """set property for ${user} 'default_compute_group' = 'not_exist1, compute_cluster';"""
            exception "Compute group (aka. Cloud cluster) not_exist1 not exist"
        }

        // check "," split format
        setAndCheckDefaultCluster(user, """,clusterName1,""")
        connectInDocker(user = user, password = 'Cloud123456') {
            assertTrue(getShowCluster("clusterName1").users.contains(user))
        }
        setAndCheckDefaultCluster(user, """,clusterName1 ,,, clusterName1""")
        connectInDocker(user = user, password = 'Cloud123456') {
            assertTrue(getShowCluster("clusterName1").users.contains(user))
        }
        setAndCheckDefaultCluster(user, """clusterName1, clusterName2""")
        // check show cluster
        connectInDocker(user = user, password = 'Cloud123456') {
            assertTrue(getShowCluster("clusterName1").users.contains(user))
            assertTrue(getShowCluster("clusterName2").users.contains(user))
        }

        // test clusterName1 all bes down, use clusterName2
        cluster.stopBackends(2)
        connectInDocker(user = user, password = 'Cloud123456') {
            sql """insert into test values (1, 1)"""
            sql """sync"""
            result = sql """select * from test"""
            assertEquals(1, result.size())
            assertTrue(getShowCluster("clusterName2").is_current.contains("TRUE"))
        } 
        // test clusterName1 and clusterName2 all bes down, read/write failed
        cluster.stopBackends(3)
        connectInDocker(user = user, password = 'Cloud123456') {
            test {
                sql """insert into test values (2, 2)"""
                exception "All the Backend nodes in the current compute group clusterName1 are in an abnormal state, and can't find backup default compute groups have alive bes"
            }
            test {
                sql """select * from test"""
                exception "All the Backend nodes in the current compute group clusterName1 are in an abnormal state, and can't find backup default compute groups have alive bes"
            }
        } 

        cluster.startBackends(2, 3)
        // case 3.
        // if enable_multi_default_compute_group = true, set default compute groups `cluster1, cluster2`
        // then change enable_multi_default_compute_group = fasle, in fe default compute group value eq `cluster1, cluster2`
        // but use `cluster1, cluster2` to find bes, will report error, cant find `cluster1, cluster2` as a cluster
        // so need set property for {user} 'default_compute_group' = 'cluster1', reconnect fe
        setFeConfig('enable_multi_default_compute_group', 'true') 
        setAndCheckDefaultCluster(user, """clusterName1, clusterName2""")
        setFeConfig('enable_multi_default_compute_group', 'false') 
        // read/write error
        connectInDocker(user = user, password = 'Cloud123456') {
            test {
                sql """insert into test values (2, 2)"""
                exception "The current compute group clusterName1, clusterName2 is not registered in the system"
            }
            test {
                sql """select * from test"""
                exception "The current compute group clusterName1, clusterName2 is not registered in the system"
            }
        } 
        setAndCheckDefaultCluster(user, """clusterName1""")
        // read/write ok
        connectInDocker(user = user, password = 'Cloud123456') {
            sql """insert into test values (2, 2)"""
            sql """sync"""
            result = sql """select * from test"""
            assertEquals(2, result.size())
            assertTrue(getShowCluster("clusterName1").is_current.contains("TRUE"))
        } 
    }
}
