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

suite("test_ddl_mv_auth","p0,auth_call") {
    String user = 'test_ddl_mv_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_ddl_mv_auth_db'
    String tableName = 'test_ddl_mv_auth_tb'
    String mvName = 'test_ddl_mv_auth_mv'
    String rollupName = 'test_ddl_mv_auth_rollup'

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
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1"
            );"""
    sql """
        insert into ${dbName}.`${tableName}` values 
        (1, "111"),
        (2, "222"),
        (3, "333");
        """

    // ddl create
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """create materialized view ${mvName} as select username from ${dbName}.${tableName};"""
            exception "denied"
        }
        test {
            sql """alter table ${dbName}.${tableName} add rollup ${rollupName}(username)"""
            exception "denied"
        }
    }
    sql """grant select_priv(username) on ${dbName}.${tableName} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """use ${dbName}"""
        test {
            sql """create materialized view ${mvName} as select username from ${dbName}.${tableName};"""
            exception "denied"
        }
        test {
            sql """alter table ${dbName}.${tableName} add rollup ${rollupName}(username)"""
            exception "denied"
        }
    }
    sql """grant alter_priv on ${dbName}.${tableName} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """use ${dbName}"""
        sql """create materialized view ${mvName} as select username from ${dbName}.${tableName};"""
        waitingMVTaskFinishedByMvName(dbName, tableName)
        sql """alter table ${dbName}.${tableName} add rollup ${rollupName}(username)"""
        waitingMVTaskFinishedByMvName(dbName, tableName)

        def mv_res = sql """desc ${dbName}.${tableName} all;"""
        logger.info("mv_res: " + mv_res)
        assertTrue(mv_res.size() == 6)
    }
    sql """revoke alter_priv on ${dbName}.${tableName} from ${user}"""
    sql """revoke select_priv(username) on ${dbName}.${tableName} from ${user}"""

    // ddl drop
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName} ON ${dbName}.${tableName};"""
            exception "denied"
        }
        test {
            sql """ALTER TABLE ${dbName}.${tableName} DROP ROLLUP ${rollupName};"""
            exception "denied"
        }
    }
    sql """grant alter_priv on ${dbName}.${tableName} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """use ${dbName}"""
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName} ON ${tableName};"""
        sql """ALTER TABLE ${dbName}.${tableName} DROP ROLLUP ${rollupName};"""
        def mv_res = sql """desc ${dbName}.${tableName} all;"""
        logger.info("mv_res: " + mv_res)
        assertTrue(mv_res.size() == 2)
    }

    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
