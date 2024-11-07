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

suite("test_ddl_job_auth","p0,auth_call") {
    String user = 'test_ddl_job_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_ddl_job_auth_db'
    String tableName = 'test_ddl_job_auth_tb'
    String tableNameDst = 'test_ddl_job_auth_tb_dst'
    String jobName = 'test_ddl_job_auth_job'

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO ${user}""";
    }

    try_sql("DROP USER ${user}")
    try_sql """drop database if exists ${dbName}"""
    try_sql("""DROP JOB where jobName='${jobName}';""")
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
    sql """create table ${dbName}.${tableNameDst} like ${dbName}.${tableName}"""

    // ddl create,show,drop
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        test {
            sql """CREATE JOB ${jobName} ON SCHEDULE AT '2020-01-01 00:00:00' DO INSERT INTO ${dbName}.${tableNameDst} SELECT * FROM ${dbName}.${tableName};"""
            exception "denied"
        }
        test {
            sql """PAUSE JOB where jobname='${jobName}';"""
            exception "denied"
        }
        test {
            sql """RESUME JOB where jobName= '${jobName}';"""
            exception "denied"
        }

        test {
            sql """DROP JOB where jobName='${jobName}';"""
            exception "denied"
        }

        test {
            sql """select * from jobs("type"="insert") where Name="${jobName}";"""
            exception "ADMIN priv"
        }
    }
    sql """grant admin_priv on *.*.* to ${user}"""
    connect(user=user, password="${pwd}", url=context.config.jdbcUrl) {
        sql """CREATE JOB ${jobName} ON SCHEDULE AT '2100-01-01 00:00:00' DO INSERT INTO ${dbName}.${tableNameDst} SELECT * FROM ${dbName}.${tableName};"""
        def res = sql """select * from jobs("type"="insert") where Name="${jobName}";"""
        assertTrue(res.size() == 1)

        sql """PAUSE JOB where jobname='${jobName}';"""
        res = sql """select * from jobs("type"="insert") where Name="${jobName}";"""
        assertTrue(res[0][5] == "PAUSED")

        sql """RESUME JOB where jobName= '${jobName}';"""
        res = sql """select * from jobs("type"="insert") where Name="${jobName}";"""
        assertTrue(res[0][5] == "RUNNING")

        sql """DROP JOB where jobName='${jobName}';"""
        res = sql """select * from jobs("type"="insert") where Name="${jobName}";"""
        assertTrue(res.size() == 0)
    }

    try_sql("""DROP JOB where jobName='${jobName}';""")
    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
