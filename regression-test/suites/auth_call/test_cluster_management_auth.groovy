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

import org.junit.Assert;

suite ("test_cluster_management_auth","nonConcurrent,p0,auth_call") {

    def follower_ip = ""
    def follower_host = ""
    def observer_ip = ""
    def observer_host = ""
    def backend_ip = ""
    def backend_host = ""
    def backend_id = ""

    def is_exists_follower = {
        def res = sql """show frontends;"""
        for (int i = 0; i < res.size(); i++) {
            if (res[i][7] == "FOLLOWER" && res[i][8] == "false" && res[i][11] == "true") {
                follower_ip = res[i][1]
                follower_host = res[i][2]
                return true
            }
        }
        return false;
    }
    def is_exists_observer = {
        def res = sql """show frontends;"""
        for (int i = 0; i < res.size(); i++) {
            if (res[i][7] == "OBSERVER" && res[i][8] == "false" && res[i][11] == "true") {
                observer_ip = res[i][1]
                observer_host = res[i][2]
                return true;
            }
        }
        return false;
    }
    def is_exists_backends = {
        def res = sql """show backends;"""
        assertTrue(res.size() > 0)
        backend_ip = res[0][1]
        backend_host = res[0][2]
        backend_id = res[0][0]
        return true
    }

    String user = 'test_cluster_management_auth_user'
    String pwd = 'C123_567p'

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }

    try_sql("DROP USER ${user}")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user}"""

    // pipeline can't support delete node, it can affect other case
    if (is_exists_follower()) {
        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            test {
                sql """show frontends"""
                exception "denied"
            }
            test {
                sql """ALTER SYSTEM add FOLLOWER '${follower_ip}:${follower_host}'"""
                exception "denied"
            }
            test {
                sql """ALTER SYSTEM DROP FOLLOWER '${follower_ip}:${follower_host}'"""
                exception "denied"
            }
        }
    }

    if (is_exists_observer()) {
        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            test {
                sql """show frontends"""
                exception "denied"
            }
            test {
                sql """ALTER SYSTEM add OBSERVER '${observer_ip}:${observer_host}'"""
                exception "denied"
            }
            test {
                sql """ALTER SYSTEM DROP OBSERVER '${observer_ip}:${observer_host}'"""
                exception "denied"
            }
        }
    }

    if (is_exists_backends()) {
        connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
            test {
                sql """show backends"""
                exception "denied"
            }
            test {
                sql """ALTER SYSTEM add backend '${backend_ip}:${backend_host}'"""
                exception "denied"
            }
            test {
                sql """ALTER SYSTEM MODIFY BACKEND "${backend_id}" SET ("tag.location" = "default");"""
                exception "denied"
            }
            test {
                sql """ALTER SYSTEM DECOMMISSION BACKEND '${backend_id}'"""
                exception "denied"
            }
            test {
                sql """ALTER SYSTEM DROP backend '${backend_ip}:${backend_host}'"""
                exception "denied"
            }
        }
    }

    try_sql("DROP USER ${user}")

}
