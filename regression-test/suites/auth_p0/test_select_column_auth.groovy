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
    String mv_name = 'test_select_column_auth_mv'
    String mtmv_name = 'test_select_column_auth_mtmv'
    String view_name = 'test_select_column_auth_view'
    String rollup_name = 'test_select_column_auth_rollup'
    String catalog_name = 'test_select_column_auth_catalog'
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

    sql """create view ${dbName}.${mv_name} as select * from ${dbName}.${tableName};"""
    sql """alter table ${dbName}.${tableName} add rollup ${rollup_name}(username)"""
    sleep(5 * 1000)
    sql """create materialized view ${mtmv_name} as select username from ${dbName}.${tableName}"""
    sleep(5 * 1000)
    sql """CREATE MATERIALIZED VIEW ${dbName}.${mtmv_name} 
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
    sql """refresh MATERIALIZED VIEW ${dbName}.${mtmv_name} auto"""
    sql """grant select_priv on regression_test to ${user}"""

    // table column
    connect(user, "${pwd}", context.config.jdbcUrl) {
        try {
            sql "select username from ${dbName}.${tableName}"
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains("denied"))
        }
    }
    sql """grant select_priv(username) on ${dbName}.${tableName} to ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        sql "select username from ${dbName}.${tableName}"
    }

    // view column
    connect(user, "${pwd}", context.config.jdbcUrl) {
        try {
            sql "select username from ${dbName}.${mv_name}"
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains("denied"))
        }
    }
    sql """grant select_priv(username) on ${dbName}.${mv_name} to ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        sql "select username from ${dbName}.${mv_name}"
    }

    // mtmv column
    connect(user, "${pwd}", context.config.jdbcUrl) {
        try {
            sql "select username from ${dbName}.${mtmv_name}"
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains("denied"))
        }
    }
    sql """grant select_priv(username) on ${dbName}.${mtmv_name} to ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        sql "select username from ${dbName}.${mtmv_name}"
    }


    // mtmv hit
    connect(user, "${pwd}", context.config.jdbcUrl) {
        sql "SET enable_materialized_view_rewrite=true"
        try {
            sql "select username, sum(id) from ${dbName}.${tableName} group by username"
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains("denied"))
        }
    }
    sql """grant select_priv(username) on ${dbName}.${mtmv_name} to ${user}"""
    sql """grant select_priv(sum_id) on ${dbName}.${mtmv_name} to ${user}"""
    sql """grant select_priv(id) on ${dbName}.${tableName} to ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        def show_grants = sql """show grants;"""
        logger.info("show grants:" + show_grants.toString())
        // If exec on fe follower, wait meta data is ready on follower
        Thread.sleep(2000)
        sql "SET enable_materialized_view_rewrite=true"
        explain {
            sql("""select username, sum(id) from ${dbName}.${tableName} group by username""")
            contains "${mtmv_name}(${mtmv_name})"
        }
    }

    try_sql("DROP USER ${user}")

}
