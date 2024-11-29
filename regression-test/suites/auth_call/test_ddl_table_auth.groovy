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

suite("test_ddl_table_auth","p0,auth_call") {
    String user = 'test_ddl_table_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_ddl_table_auth_db'
    String tableName = 'test_ddl_table_auth_tb'
    String tableNameNew = 'test_ddl_table_auth_tb_new'
    String cteLikeDstDb = 'test_ddl_table_cte_like_dst_db'
    String cteLikeDstTb = 'test_ddl_table_cte_like_dst_tb'
    String cteSelectDstDb = 'test_ddl_table_cte_select_dst_db'
    String cteSelectDstTb = 'test_ddl_table_cte_select_dst_tb'

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }

    try_sql("DROP USER ${user}")
    try_sql """drop database if exists ${dbName}"""
    try_sql """drop database if exists ${cteLikeDstDb}"""
    try_sql """drop database if exists ${cteSelectDstDb}"""
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user}"""
    sql """create database ${dbName}"""

    def waitingChangeTaskFinished = { def curDb ->
        Thread.sleep(2000)
        sql """use ${curDb}"""
        String showTasks = "SHOW ALTER TABLE COLUMN order by CreateTime;"
        String status = "NULL"
        List<List<Object>> result
        long startTime = System.currentTimeMillis()
        long timeoutTimestamp = startTime + 5 * 60 * 1000 // 5 min
        do {
            result = sql(showTasks)
            logger.info("result: " + result.toString())
            if (!result.isEmpty()) {
                status = result.last().get(9)
            }
            logger.info("The state of ${showTasks} is ${status}")
            Thread.sleep(1000);
        } while (timeoutTimestamp > System.currentTimeMillis() && (status != 'FINISHED'))
        if (status != "FINISHED") {
            logger.info("status is not success")
        }
        Assert.assertEquals("FINISHED", status)
    }

    // ddl create
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """create table ${dbName}.${tableName} (
                    id BIGINT,
                    username VARCHAR(20)
                )
                DISTRIBUTED BY HASH(id) BUCKETS 2
                PROPERTIES (
                    "replication_num" = "1"
                );"""
            exception "denied"
        }
        def res = sql """show query stats;"""
        logger.info("res:" + res)

        test {
            sql """SHOW FULL COLUMNS FROM ${dbName}.${tableName};"""
            exception "denied"
        }
    }
    sql """create table ${dbName}.${tableName} (
            id BIGINT,
            username VARCHAR(20)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );"""
    sql """grant Create_priv on ${dbName}.${tableName} to ${user}"""
    sql """drop table ${dbName}.${tableName};"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """create table ${dbName}.${tableName} (
                    id BIGINT,
                    username VARCHAR(20)
                )
                DISTRIBUTED BY HASH(id) BUCKETS 2
                PROPERTIES (
                    "replication_num" = "1"
                );"""
        sql """use ${dbName}"""
        sql """show create table ${tableName}"""
        def db_res = sql """show tables;"""
        assertTrue(db_res.size() == 1)

        def col_res = sql """SHOW FULL COLUMNS FROM ${dbName}.${tableName};"""
        logger.info("col_res: " + col_res)
        assertTrue(col_res.size() == 2)
    }

    // ddl alter
    // user alter
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """ALTER table ${tableName} RENAME ${tableNameNew};"""
            exception "denied"
        }
        test {
            sql """ALTER TABLE ${dbName}.${tableName} ADD COLUMN new_col INT KEY DEFAULT "0";"""
            exception "denied"
        }
        def res = sql """SHOW ALTER TABLE COLUMN;"""
        assertTrue(res.size() == 0)
    }
    sql """grant ALTER_PRIV on ${dbName}.${tableName} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """use ${dbName}"""
        sql """ALTER table ${tableName} RENAME ${tableNameNew};"""

        test {
            sql """show create table ${tableNameNew}"""
            exception "denied"
        }
        def tb_res = sql """show tables;"""
        assertTrue(tb_res.size() == 0)
    }
    // root alter
    sql """use ${dbName}"""
    sql """ALTER table ${tableNameNew} RENAME ${tableName};"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """use ${dbName}"""
        sql """show create table ${tableName}"""
        def db_res = sql """show tables;"""
        assertTrue(db_res.size() == 1)
    }

    // show
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """use ${dbName}"""
        sql """ALTER TABLE ${tableName} ADD COLUMN new_col INT KEY DEFAULT "0";"""
        def res = sql """SHOW ALTER TABLE COLUMN;"""
        assertTrue(res.size() == 1)
    }

    // dml select
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """select id from ${dbName}.${tableName}"""
            exception "denied"
        }
    }
    sql """grant select_priv(id) on ${dbName}.${tableName} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """select id from ${dbName}.${tableName}"""
    }
    sql """revoke select_priv(id) on ${dbName}.${tableName} from ${user}"""

    // ddl create table like
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """create table ${cteLikeDstDb}.${cteLikeDstTb} like ${dbName}.${tableName};"""
            exception "denied"
        }
    }
    sql """create database ${cteLikeDstDb}"""
    sql """create table ${cteLikeDstDb}.${cteLikeDstTb} like ${dbName}.${tableName};"""
    sql """grant Create_priv on ${cteLikeDstDb}.${cteLikeDstTb} to ${user}"""
    sql """drop table ${cteLikeDstDb}.${cteLikeDstTb};"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """create table ${cteLikeDstDb}.${cteLikeDstTb} like ${dbName}.${tableName};"""
            exception "denied"
        }
    }
    sql """grant SELECT_PRIV on ${dbName}.${tableName} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """create table ${cteLikeDstDb}.${cteLikeDstTb} like ${dbName}.${tableName};"""
    }
    sql """revoke SELECT_PRIV on ${dbName}.${tableName} from ${user}"""

    // ddl create table select
    sql """create database ${cteSelectDstDb}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """create table ${cteSelectDstDb}.${cteSelectDstTb}(username) PROPERTIES("replication_num" = "1") as select username from ${dbName}.${tableName};"""
            exception "denied"
        }
    }
    sql """create table ${cteSelectDstDb}.${cteSelectDstTb}(username) PROPERTIES("replication_num" = "1") as select username from ${dbName}.${tableName};"""
    sql """grant Create_priv on ${cteSelectDstDb}.${cteSelectDstTb} to ${user}"""
    sql """drop table ${cteSelectDstDb}.${cteSelectDstTb}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """create table ${cteSelectDstDb}.${cteSelectDstTb}(username) PROPERTIES("replication_num" = "1") as select username from ${dbName}.${tableName};"""
            exception "denied"
        }
    }
    sql """create table ${cteSelectDstDb}.${cteSelectDstTb}(username) PROPERTIES("replication_num" = "1") as select username from ${dbName}.${tableName};"""
    sql """grant LOAD_PRIV on ${cteSelectDstDb}.${cteSelectDstTb} to ${user}"""
    sql """drop table ${cteSelectDstDb}.${cteSelectDstTb}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """create table ${cteSelectDstDb}.${cteSelectDstTb}(username) PROPERTIES("replication_num" = "1") as select username from ${dbName}.${tableName};"""
            exception "denied"
        }
    }
    sql """grant select_priv(username) on ${dbName}.${tableName} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """create table ${cteSelectDstDb}.${cteSelectDstTb}(username) PROPERTIES("replication_num" = "1") as select username from ${dbName}.${tableName};"""
    }

    waitingChangeTaskFinished(dbName)
    // ddl truncate
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """use ${dbName}"""
            sql """truncate table ${tableName};"""
            exception "denied"
        }
    }
    sql """grant LOAD_PRIV on ${dbName}.${tableName} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """use ${dbName}"""
        sql """truncate table ${tableName};"""
    }

    // ddl drop
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """use ${dbName}"""
            sql """drop table ${tableName};"""
            exception "denied"
        }
    }
    sql """grant DROP_PRIV on ${dbName}.${tableName} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """use ${dbName}"""
        sql """drop table ${tableName};"""
        def ctl_res = sql """show tables;"""
        assertTrue(ctl_res.size() == 0)
    }


    sql """drop database if exists ${dbName}"""
    sql """drop database if exists ${cteLikeDstDb}"""
    sql """drop database if exists ${cteSelectDstDb}"""
    try_sql("DROP USER ${user}")
}
