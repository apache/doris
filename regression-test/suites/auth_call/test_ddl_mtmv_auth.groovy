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

suite("test_ddl_mtmv_auth","p0,auth_call") {
    String user = 'test_ddl_mtmv_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_ddl_mtmv_auth_db'
    String tableName = 'test_ddl_mtmv_auth_tb'
    String mtmvName = 'test_ddl_mtmv_auth_mtmv'
    String mtmvNameNew = 'test_ddl_mtmv_auth_mtmv_new'

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

    // ddl create
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """CREATE MATERIALIZED VIEW ${dbName}.${mtmvName} 
                BUILD IMMEDIATE REFRESH AUTO ON MANUAL 
                DISTRIBUTED BY RANDOM BUCKETS 1 
                PROPERTIES ('replication_num' = '1') 
                AS select username, sum(id) as sum_id from ${dbName}.${tableName} group by username"""
            exception "denied"
        }
    }
    sql """CREATE MATERIALIZED VIEW ${dbName}.${mtmvName} 
                BUILD IMMEDIATE REFRESH AUTO ON MANUAL 
                DISTRIBUTED BY RANDOM BUCKETS 1 
                PROPERTIES ('replication_num' = '1') 
                AS select username, sum(id) as sum_id from ${dbName}.${tableName} group by username"""
    sql """grant Create_priv on ${dbName}.${mtmvName} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """CREATE MATERIALIZED VIEW ${dbName}.${mtmvName} 
                BUILD IMMEDIATE REFRESH AUTO ON MANUAL 
                DISTRIBUTED BY RANDOM BUCKETS 1 
                PROPERTIES ('replication_num' = '1') 
                AS select username, sum(id) as sum_id from ${dbName}.${tableName} group by username"""
            exception "denied"
        }
    }
    sql """grant select_priv on ${dbName}.${tableName} to ${user}"""
    sql """drop MATERIALIZED VIEW ${dbName}.${mtmvName};"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """CREATE MATERIALIZED VIEW ${dbName}.${mtmvName} 
            BUILD IMMEDIATE REFRESH AUTO ON MANUAL 
            DISTRIBUTED BY RANDOM BUCKETS 1 
            PROPERTIES ('replication_num' = '1') 
            AS select username, sum(id) as sum_id from ${dbName}.${tableName} group by username"""
        sql """show create materialized view ${dbName}.${mtmvName}"""
        sql """use ${dbName}"""
        def tb_res = sql """show tables;"""
        assertTrue(tb_res.size() == 2)
    }
    sql """revoke select_priv on ${dbName}.${tableName} from ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """use ${dbName}"""
        sql """refresh MATERIALIZED VIEW ${mtmvName} auto;"""

        // insert and refresh mtmv
        def job_name = getJobName(dbName, mtmvName)

        // cancel
        def mv_tasks_res = sql """select * from tasks("type"="mv") where MvName="${mtmvName}";"""
        assertTrue(mv_tasks_res.size() > 0)
        try {
            sql """CANCEL MATERIALIZED VIEW TASK ${mv_tasks_res[0][0]} on ${mtmvName};"""
        } catch (Exception e) {
            log.info(e.getMessage())
            assertTrue(e.getMessage().contains("no running task"))
        }

        // pause
        def job_status = sql """select * from jobs("type"="mv") where Name="${job_name}";"""
        assertTrue(job_status[0][8] == "RUNNING")
        sql """PAUSE MATERIALIZED VIEW JOB ON ${mtmvName};"""
        job_status = sql """select * from jobs("type"="mv") where Name="${job_name}";"""
        assertTrue(job_status[0][8] == "PAUSED")

        // resume
        sql """RESUME MATERIALIZED VIEW JOB ON ${mtmvName};"""
        job_status = sql """select * from jobs("type"="mv") where Name="${job_name}";"""
        assertTrue(job_status[0][8] == "RUNNING")
    }
    sql """grant select_priv on ${dbName}.${tableName} to ${user}"""

    // ddl alter
    // user alter
    sql """revoke Create_priv on ${dbName}.${mtmvName} from ${user};"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """ALTER MATERIALIZED VIEW ${mtmvName} rename ${mtmvNameNew};"""
            exception "denied"
        }
        test {
            sql """show create materialized view ${dbName}.${mtmvName}"""
            exception "denied"
        }
    }
    sql """grant ALTER_PRIV on ${dbName}.${mtmvName} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """use ${dbName}"""
        sql """show create materialized view ${mtmvName}"""
        sql """ALTER MATERIALIZED VIEW ${mtmvName} rename ${mtmvNameNew};"""
        test {
            sql """show create materialized view ${mtmvNameNew}"""
            exception "denied"
        }
        def tb_res = sql """show tables;"""
        assertTrue(tb_res.size() == 1)
    }

    // root alter
    sql """use ${dbName}"""
    sql """ALTER MATERIALIZED VIEW ${mtmvNameNew} RENAME ${mtmvName};"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """use ${dbName}"""
        sql """show create materialized view ${mtmvName}"""
        def db_res = sql """show tables;"""
        assertTrue(db_res.size() == 2)
    }

    // dml select
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """select username from ${dbName}.${mtmvName}"""
            exception "denied"
        }
    }
    sql """grant select_priv(username) on ${dbName}.${mtmvName} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """select username from ${dbName}.${mtmvName}"""
    }
    sql """revoke select_priv(username) on ${dbName}.${mtmvName} from ${user}"""

    // ddl drop
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """use ${dbName}"""
            sql """drop materialized view ${mtmvName};"""
            exception "denied"
        }
    }
    sql """grant DROP_PRIV on ${dbName}.${mtmvName} to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """use ${dbName}"""
        sql """drop materialized view ${mtmvName};"""
        def ctl_res = sql """show tables;"""
        assertTrue(ctl_res.size() == 1)
    }

    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
