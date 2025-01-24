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

suite("test_show_no_auth","p0,auth_call") {
    String user = 'test_show_charset_auth_user'
    String user1 = 'test_show_charset_auth_user1'
    String pwd = 'C123_567p'

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }

    try_sql("DROP USER ${user}")
    try_sql("DROP USER ${user1}")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """CREATE USER '${user1}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user}"""

    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """SHOW CHARSET"""
        sql """SHOW DATA TYPES"""
        sql """SHOW ENGINES"""
        sql """show collation;"""
        sql """show variables;"""
        sql """SHOW PROPERTY;"""
        def res1 = sql """SHOW PROCESSLIST"""
        logger.info("res1: " + res1)
        assertTrue(res1.size() >= 1)

        test {
            sql """show PROPERTY for ${user1}"""
            exception "denied"
        }
        test {
            sql """SHOW TRASH;"""
            exception "denied"
        }
    }
    sql """grant grant_priv on *.*.* to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        def res = sql """show PROPERTY for ${user1}"""
        logger.info("res: " + res)
        assertTrue(res.size() > 0)

        def res1 = sql """SHOW PROCESSLIST"""
        logger.info("res1: " + res1)
        assertTrue(res1.size() == 1)
    }
    sql """revoke grant_priv on *.*.* from ${user}"""
    sql """grant admin_priv on *.*.* to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        def res = sql """SHOW TRASH;"""
        logger.info("res: " + res)
        assertTrue(res.size() >= 1)
    }

    try_sql("DROP USER ${user}")
    try_sql("DROP USER ${user1}")
}
