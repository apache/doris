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
import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_account_management_user_auth","p0,auth_call") {

    String user = 'test_account_management_user_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_account_management_user_auth_db'
    String user_derive = 'test_account_management_user_derive_role'

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }

    try_sql("DROP USER ${user}")
    try_sql("DROP USER ${user_derive}")
    try_sql """drop database if exists ${dbName}"""

    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user}"""
    sql """create database ${dbName}"""

    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """CREATE USER ${user_derive} IDENTIFIED BY '${pwd}';"""
            exception "denied"
        }
        test {
            sql """ALTER USER ${user_derive} IDENTIFIED BY "${pwd}";"""
            exception "denied"
        }
        test {
            sql """SET PASSWORD FOR '${user_derive}' = PASSWORD('${pwd}')"""
            exception "denied"
        }
        test {
            sql """SET PROPERTY FOR '${user_derive}' 'max_user_connections' = '1000';"""
            exception "denied"
        }
        test {
            sql """DROP user ${user_derive}"""
            exception "denied"
        }
        test {
            sql """SET LDAP_ADMIN_PASSWORD = PASSWORD('${pwd}')"""
            exception "denied"
        }
    }
    sql """grant grant_priv on *.*.* to '${user}'"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """CREATE USER ${user_derive} IDENTIFIED BY '${pwd}';"""
        sql """ALTER USER ${user_derive} IDENTIFIED BY "${pwd}";"""
        sql """SET PASSWORD FOR '${user_derive}' = PASSWORD('${pwd}')"""
        test {
            sql """SET PROPERTY FOR '${user_derive}' 'max_user_connections' = '1000';"""
            exception "denied"
        }
        sql """DROP user ${user_derive}"""
        test {
            sql """SET LDAP_ADMIN_PASSWORD = PASSWORD('${pwd}')"""
            exception "denied"
        }
    }
    sql """revoke grant_priv on *.*.* from '${user}'"""
    sql """grant admin_priv on *.*.* to '${user}'"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """CREATE USER ${user_derive} IDENTIFIED BY '${pwd}';"""
        sql """ALTER USER ${user_derive} IDENTIFIED BY "${pwd}";"""
        sql """SET PASSWORD FOR '${user_derive}' = PASSWORD('${pwd}')"""
        sql """SET PROPERTY FOR '${user_derive}' 'max_user_connections' = '1000';"""
        sql """DROP user ${user_derive}"""
        sql """SET LDAP_ADMIN_PASSWORD = PASSWORD('${pwd}')"""
    }

    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
    try_sql("DROP role ${user_derive}")
}
