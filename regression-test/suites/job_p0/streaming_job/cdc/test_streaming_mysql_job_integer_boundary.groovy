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


import org.awaitility.Awaitility

import static java.util.concurrent.TimeUnit.SECONDS

// Guard MySQL integer-type boundaries that all_type does NOT cover:
//   1) UNSIGNED boundaries (esp. BIGINT UNSIGNED >= 2^63, which overflows Java long)
//   2) TINYINT(1) remains TINYINT and preserves non-boolean values
//
// Coverage is run twice: ids 1-5 cover the snapshot path, ids 101-105 repeat
// the same boundary themes through the binlog path. Plus UPDATEs that switch
// boundary values to validate UPDATE binlog parsing on UNSIGNED extremes.
//
// PG does not need a symmetric test: PG has no UNSIGNED and uses native
// boolean, so neither risk applies.
suite("test_streaming_mysql_job_integer_boundary", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_integer_boundary_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "streaming_integer_boundary_pk"
    def mysqlDb = "test_cdc_db"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

        // ===== Prepare MySQL side =====
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table1}"""
            sql """
            create table ${mysqlDb}.${table1} (
              `id` int primary key,
              `tag` varchar(64),
              `tinyint_u` tinyint unsigned,
              `smallint_u` smallint unsigned,
              `mediumint_u` mediumint unsigned,
              `int_u` int unsigned,
              `bigint_u` bigint unsigned,
              `tinyint_1` tinyint(1),
              `tinyint_s` tinyint
            ) engine=innodb default charset=utf8;
            """

            // ----- Snapshot rows: 5 boundary themes via JDBC path -----
            //                                           tinyint_u  smallint_u  mediumint_u   int_u           bigint_u                    tinyint(1)  tinyint_s
            sql """insert into ${mysqlDb}.${table1} values (1, 'all_zero',         0,         0,         0,            0,              0,                          0,           0)"""
            sql """insert into ${mysqlDb}.${table1} values (2, 'signed_max',       127,       32767,     8388607,      2147483647,     9223372036854775807,        1,           127)"""
            // signed_max+1: every column passes its signed boundary; bigint_u steps into Java long overflow range.
            sql """insert into ${mysqlDb}.${table1} values (3, 'signed_max_plus1', 128,       32768,     8388608,      2147483648,     9223372036854775808,        -1,          -128)"""
            // unsigned_max: each column at its unsigned upper bound. bigint_u is 2^64-1.
            sql """insert into ${mysqlDb}.${table1} values (4, 'unsigned_max',     255,       65535,     16777215,     4294967295,     18446744073709551615,       127,         -1)"""
            sql """insert into ${mysqlDb}.${table1} values (5, 'mid_value',        100,       30000,     8000000,      2000000000,     9000000000000000000,        4,           50)"""
        }

        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM MYSQL (
                    "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}?serverTimezone=UTC",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "com.mysql.cj.jdbc.Driver",
                    "user" = "root",
                    "password" = "123456",
                    "database" = "${mysqlDb}",
                    "include_tables" = "${table1}",
                    "offset" = "initial"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        // Wait for snapshot to land all 5 rows in Doris.
        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                        log.info("snapshot row count: " + cnt)
                        cnt.get(0).get(0) == 5
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex
        }

        // Verify type mapping in Doris (tinyint_u->smallint, bigint_u->largeint, etc.)
        qt_desc_integer_boundary """desc ${currentDb}.${table1};"""
        qt_select_snapshot """select id, tag, tinyint_u, smallint_u, mediumint_u, int_u, bigint_u, tinyint_1, tinyint_s from ${currentDb}.${table1} order by id;"""

        // ===== Binlog phase: repeat the SAME 5 boundary themes through binlog path =====
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """insert into ${mysqlDb}.${table1} values (101, 'all_zero',         0,         0,         0,            0,              0,                          0,           0)"""
            sql """insert into ${mysqlDb}.${table1} values (102, 'signed_max',       127,       32767,     8388607,      2147483647,     9223372036854775807,        1,           127)"""
            sql """insert into ${mysqlDb}.${table1} values (103, 'signed_max_plus1', 128,       32768,     8388608,      2147483648,     9223372036854775808,        -1,          -128)"""
            sql """insert into ${mysqlDb}.${table1} values (104, 'unsigned_max',     255,       65535,     16777215,     4294967295,     18446744073709551615,       127,         -1)"""
            sql """insert into ${mysqlDb}.${table1} values (105, 'mid_value',        100,       30000,     8000000,      2000000000,     9000000000000000000,        4,           50)"""

            sql """update ${mysqlDb}.${table1} set bigint_u=18446744073709551615 where id=1"""
            sql """update ${mysqlDb}.${table1} set tinyint_1=4 where id=2"""
            sql """update ${mysqlDb}.${table1} set int_u=4294967295 where id=5"""
        }

        // Wait until all 5 binlog inserts and 3 updates are visible.
        try {
            Awaitility.await().atMost(180, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                        def upd1 = sql """select cast(bigint_u as string) from ${currentDb}.${table1} where id=1"""
                        def upd2 = sql """select tinyint_1 from ${currentDb}.${table1} where id=2"""
                        def upd5 = sql """select int_u from ${currentDb}.${table1} where id=5"""
                        def b1 = upd1.get(0).get(0) == null ? '' : upd1.get(0).get(0).toString()
                        def t2 = upd2.get(0).get(0)
                        def b5 = upd5.get(0).get(0)
                        log.info("incr count=" + cnt + " id1.bigint_u=" + b1
                                + " id2.tinyint_1=" + t2 + " id5.int_u=" + b5)
                        cnt.get(0).get(0) == 10 &&
                                b1 == '18446744073709551615' &&
                                t2 != null && t2.toString() == '4' &&
                                b5 != null && b5.toString() == '4294967295'
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job (incr): " + showjob)
            log.info("show task (incr): " + showtask)
            throw ex
        }

        qt_select_after_incr """select id, tag, tinyint_u, smallint_u, mediumint_u, int_u, bigint_u, tinyint_1, tinyint_s from ${currentDb}.${table1} order by id;"""

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
