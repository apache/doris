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

suite("test_ddl_workload_group_auth","p0,auth_call") {
    String user = 'test_ddl_wg_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_ddl_wg_auth_db'
    String workloadGroupName = 'test_ddl_wg_auth_wg'

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }

    try_sql("DROP USER ${user}")
    try_sql """drop database if exists ${dbName}"""
    try_sql("""drop workload group if exists ${workloadGroupName};""")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user}"""
    sql """create database ${dbName}"""

    // ddl create,show,drop
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """CREATE WORKLOAD GROUP "${workloadGroupName}"
                    PROPERTIES (
                        "cpu_share"="10"
                    );"""
            exception "denied"
        }
        test {
            sql """alter workload group ${workloadGroupName}
                properties (
                    "cpu_share"="20"
                );"""
            exception "denied"
        }

        def res = sql """show workload groups like '${workloadGroupName}'"""
        assertTrue(res.size() == 0)

        test {
            sql """drop workload group if exists ${workloadGroupName};"""
            exception "denied"
        }
    }
    sql """grant admin_priv on *.*.* to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """CREATE WORKLOAD GROUP "${workloadGroupName}"
                PROPERTIES (
                    "cpu_share"="10"
                );"""
        def res = sql """show workload groups like '${workloadGroupName}'"""
        assertTrue(res.size() > 0)
        sql """alter workload group ${workloadGroupName}
                properties (
                    "cpu_share"="20"
                );"""
        sql """drop workload group if exists ${workloadGroupName};"""
        res = sql """show workload groups like '${workloadGroupName}'"""
        assertTrue(res.size() == 0)
    }

    try_sql("""drop workload group if exists ${workloadGroupName};""")
    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
