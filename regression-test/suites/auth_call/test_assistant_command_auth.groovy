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

suite("test_assistant_command_auth","p0,auth_call") {

    String user = 'test_assistant_command_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_assistant_command_auth_db'
    String tableName = 'test_assistant_command_auth_tb'
    String catalogName = 'test_assistant_command_auth_catalog'

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

    sql """create table ${dbName}.${tableName} (
                id BIGINT,
                username VARCHAR(20)
            )
            PARTITION BY RANGE(id) ()
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );"""

    sql """alter table ${dbName}.${tableName} add partition p1 VALUES [("1"), ("2"));"""
    def insert_res = sql """insert into ${dbName}.${tableName} values (1, "111");"""
    logger.info("insert_res: " + insert_res)

    sql """create catalog if not exists ${catalogName} properties (
            'type'='hms'
        );"""


    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """use ${dbName}"""
            exception "denied"
        }

        test {
            sql """DESC ${dbName}.${tableName} ALL;"""
            exception "denied"
        }

        sql """switch internal;"""
        test {
            sql """REFRESH CATALOG ${catalogName};"""
            exception "denied"
        }

        sql """SYNC;"""
    }

    sql """grant select_PRIV on ${dbName}.${tableName} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """use ${dbName}"""
        sql """DESC ${dbName}.${tableName} ALL;"""
    }

    sql """grant select_PRIV on ${catalogName}.*.* to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """REFRESH CATALOG ${catalogName};"""
    }

    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
