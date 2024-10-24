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

suite("test_ddl_database_auth","p0,auth_call") {
    String user = 'test_ddl_database_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_ddl_database_auth_db'
    String dbNameNew = 'test_ddl_database_auth_db_new'
    String tableName = 'test_ddl_database_auth_tb'
    String tableNameNew = 'test_ddl_database_auth_tb_new'

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }

    try_sql("DROP USER ${user}")
    try_sql """drop database if exists ${dbName}"""
    try_sql """drop database if exists ${dbNameNew}"""
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user}"""

    // ddl create
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """create database ${dbName};"""
            exception "denied"
        }
        def db_res = sql """show databases;"""
        assertTrue(db_res.size() == 3 || db_res.size() == 1)
    }
    sql """create database ${dbName};"""
    sql """grant Create_priv on ${dbName}.* to ${user}"""
    sql """drop database ${dbName};"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """create database ${dbName};"""
        sql """show create database ${dbName}"""
        def db_res = sql """show databases;"""
        assertTrue(db_res.size() == 4 || db_res.size() == 2)
    }

    // ddl alter
    // user alter
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """ALTER database ${dbName} RENAME ${dbNameNew};"""
            exception "denied"
        }
    }
    sql """grant ALTER_PRIV on ${dbName}.* to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """ALTER database ${dbName} RENAME ${dbNameNew};"""
        test {
            sql """show create database ${dbNameNew}"""
            exception "denied"
        }
        def db_res = sql """show databases;"""
        assertTrue(db_res.size() == 3 || db_res.size() == 1)
    }
    // root alter
    sql """ALTER database ${dbNameNew} RENAME ${dbName};"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """show create database ${dbName}"""
        def db_res = sql """show databases;"""
        assertTrue(db_res.size() == 4 || db_res.size() == 2)
    }

    // ddl drop
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """drop database ${dbName};"""
            exception "denied"
        }
    }
    sql """grant DROP_PRIV on ${dbName}.* to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """drop database ${dbName};"""
        def ctl_res = sql """show databases;"""
        assertTrue(ctl_res.size() == 3 || ctl_res.size() == 1)
    }

    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
