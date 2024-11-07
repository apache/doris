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
import org.junit.Assert

suite("test_default_cluster", "docker") {
    def options = new ClusterOptions()
    options.cloudMode = true

    def getProperty = { property ->
        def result = null
        result = sql_return_maparray """SHOW PROPERTY""" 
        result.find {
            it.Key == property as String
        }
    }

    def setAndCheckDefaultCluster = { validCluster ->
        sql """set property 'DEFAULT_CLOUD_CLUSTER' = '$validCluster'"""
        def ret1 = getProperty("default_cloud_cluster")
        def ret2 = getProperty("default_compute_group")
        assertEquals(ret1.Value as String, validCluster)
        assertEquals(ret1.Value as String, ret2.Value as String)
    }

    docker(options) {
        // not admin
        def user1 = "default_user1"
        // admin role
        def user2 = "default_user2"

        sql """CREATE USER $user1 IDENTIFIED BY 'Cloud123456' DEFAULT ROLE 'admin'"""
        sql """CREATE USER $user2 IDENTIFIED BY 'Cloud123456'"""
        sql """GRANT SELECT_PRIV on *.*.* to ${user2}"""

        def clusters = sql " SHOW CLUSTERS "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]

        // admin set himself
        setAndCheckDefaultCluster validCluster

        // user1
        connectInDocker(user = user1, password = 'Cloud123456') {
            setAndCheckDefaultCluster validCluster
            def ret = sql """show grants"""
            log.info("ret = {}", ret)
        }

        connectInDocker(user = user2, password = 'Cloud123456') {
            //java.sql.SQLException: errCode = 2, detailMessage = set default compute group failed, user default_user2 has no permission to use compute group 'compute_cluster', please
            //grant use privilege first , ComputeGroupException: CURRENT_USER_NO_AUTH_TO_USE_COMPUTE_GROUP, you canuse SQL `GRANT USAGE_PRIV ON COMPUTE GROUP {compute_group_name} TO
            //{user}`
            try {
                sql """set property 'DEFAULT_CLOUD_CLUSTER' = '$validCluster'"""
            } catch (Exception e) {
                log.info(e.getMessage())
                assertTrue(e.getMessage().contains("CURRENT_USER_NO_AUTH_TO_USE_COMPUTE_GROUP"))
            }
        }

        try {
            // admin set user2, failed not give user2 cluster auth
            sql """set property for $user2 'DEFAULT_CLOUD_CLUSTER' = '$validCluster'"""
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("CURRENT_USER_NO_AUTH_TO_USE_COMPUTE_GROUP"))
        }
        sql """GRANT USAGE_PRIV ON COMPUTE GROUP $validCluster TO $user2""" 
        // succ
        setAndCheckDefaultCluster validCluster
        // admin clean
        sql """set property for $user2 'DEFAULT_CLOUD_CLUSTER' = '' """

        connectInDocker(user = user2, password = 'Cloud123456') {
            // user set himself
            setAndCheckDefaultCluster validCluster
            sql """set property 'DEFAULT_CLOUD_CLUSTER' = '' """
            def ret = getProperty("default_cloud_cluster")
            assertEquals(ret.Value as String, "")
        }
    }
}
