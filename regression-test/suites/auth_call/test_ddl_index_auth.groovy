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

suite("test_ddl_index_auth","p0,auth_call") {
    String user = 'test_ddl_index_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_ddl_index_auth_db'
    String tableName = 'test_ddl_index_auth_tb'
    String indexName = 'test_ddl_index_auth_index'

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
    sql """
        CREATE TABLE IF NOT EXISTS ${dbName}.${tableName} (
            id BIGINT,
            username1 VARCHAR(30),
            username2 VARCHAR(30),
            username3 VARCHAR(30)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    def index_res_tmp = sql """SHOW INDEX FROM ${dbName}.${tableName};"""
    assertTrue(index_res_tmp.size() == 0)

    // ddl create
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """CREATE INDEX IF NOT EXISTS ${indexName} ON ${dbName}.${tableName} (username3) USING INVERTED COMMENT 'balabala';"""
            exception "denied"
        }

        test {
            sql """DROP INDEX IF EXISTS ${indexName} ON ${dbName}.${tableName};"""
            exception "denied"
        }

        test {
            sql """show index FROM ${dbName}.${tableName};"""
            exception "denied"
        }
    }
    sql """grant ALTER_PRIV on ${dbName}.${tableName} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """use ${dbName}"""
        sql """CREATE INDEX IF NOT EXISTS ${indexName} ON ${dbName}.${tableName} (username3) USING INVERTED COMMENT 'balabala';"""
        sql """show create table ${tableName}"""
        def index_res = sql """SHOW INDEX FROM ${tableName};"""
        assertTrue(index_res.size() == 1)

        sql """DROP INDEX IF EXISTS ${indexName} ON ${dbName}.${tableName};"""
        index_res = sql """SHOW INDEX FROM ${tableName};"""
        assertTrue(index_res.size() == 0)

        def show_index_res = sql """show index FROM ${dbName}.${tableName};"""
        logger.info("show_index_res: " + show_index_res)
    }

    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
