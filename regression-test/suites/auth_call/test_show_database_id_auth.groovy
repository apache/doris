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

suite("test_show_database_id_auth","p0,auth_call") {
    String user = 'test_show_database_id_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_show_database_id_auth_db'
    String tableName = 'test_show_database_id_auth_tb'

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }

    try_sql("DROP USER ${user}")
    try_sql """drop database if exists ${dbName}"""
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user}"""
    sql """create database ${dbName}"""

    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """show database 1001"""
            exception "denied"
        }
        test {
            sql """show table 1001"""
            exception "denied"
        }
        test {
            sql """SHOW CATALOG RECYCLE BIN;"""
            exception "denied"
        }
    }
    sql """grant admin_priv on *.*.* to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """show database 1001"""
        sql """show table 1001"""
        sql """SHOW CATALOG RECYCLE BIN;"""
    }

    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
