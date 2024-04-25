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

suite("test_select_column_auth","p0,auth") {
    String user = 'test_select_column_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_select_column_auth_db'
    String tableName = 'test_select_column_auth_table'
    try_sql("drop user ${user}")
    try_sql """drop table if exists ${dbName}.${tableName}"""
    sql """drop database if exists ${dbName}"""

    sql """create user '${user}' IDENTIFIED by '${pwd}'"""

    sql """create database ${dbName}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${dbName}.`${tableName}` (
            id BIGINT,
            username VARCHAR(20)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql """create view ${dbName}.v1 as select * from ${dbName}.${tableName};"""
    sql """alter table ${dbName}.${tableName} add rollup rollup1(username)"""
    sleep(5 * 1000)
    sql """create materialized view mv1 as select username from ${dbName}.${tableName}"""
    sleep(5 * 1000)
    sql """CREATE MATERIALIZED VIEW ${dbName}.mtmv1 
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL 
        DISTRIBUTED BY RANDOM BUCKETS 1 
        PROPERTIES ('replication_num' = '1') 
        AS select username, sum(id) as sum_id from ${dbName}.${tableName} group by username"""
    sql """
        insert into ${dbName}.`${tableName}` values 
        (1, "111"),
        (2, "222"),
        (3, "333");
        """
    sql """refresh MATERIALIZED VIEW ${dbName}.mtmv1 auto"""
    sql """grant select_priv on regression_test to ${user}"""

    // table column
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        try {
            sql "select username from ${dbName}.${tableName}"
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains("Admin_priv,Select_priv"))
        }
    }
    sql """grant select_priv(username) on ${dbName}.${tableName} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql "select username from ${dbName}.${tableName}"
    }

    // view column
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        try {
            sql "select username from ${dbName}.v1"
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains("Admin_priv,Select_priv"))
        }
    }
    sql """grant select_priv(username) on ${dbName}.v1 to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql "select username from ${dbName}.v1"
    }

    // mtmv column
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        try {
            sql "select username from ${dbName}.mtmv1"
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains("Admin_priv,Select_priv"))
        }
    }
    sql """grant select_priv(username) on ${dbName}.mtmv1 to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql "select username from ${dbName}.mtmv1"
    }


    // mtmv hit
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql "SET enable_materialized_view_rewrite=true"
        try {
            sql "select username, sum(id) from ${dbName}.${tableName} group by username"
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains("Admin_priv,Select_priv"))
        }
    }
    sql """grant select_priv(username) on ${dbName}.mtmv1 to ${user}"""
    sql """grant select_priv(sum_id) on ${dbName}.mtmv1 to ${user}"""
    sql """grant select_priv(id) on ${dbName}.${tableName} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql "SET enable_materialized_view_rewrite=true"
        explain {
            sql("""select username, sum(id) from ${dbName}.${tableName} group by username""")
            contains "mtmv1(mtmv1)"
        }
    }

    try_sql("DROP USER ${user}")

}
