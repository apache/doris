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

// Recommended end-to-end tz configuration for data fidelity: set jdbc_url's
// serverTimezone to the SOURCE MySQL session/server tz, so cdc renders the
// TIMESTAMP instant back to the exact wall clock the source shows. Doris data
// then stays identical to MySQL, independent of Doris's own session tz.
//
// Source tz is Asia/Tokyo (+09, no DST), deliberately != Doris default +08, so
// the case proves the rendering follows the SOURCE tz (not Doris). MySQL docker
// has no tz table, so the source session uses the equivalent offset '+09:00'
// while jdbc_url uses the IANA name 'Asia/Tokyo' (cdc resolves it via ZoneId).
// Tokyo has no DST, so its offset is a constant +09 and the result is fully
// deterministic.
//
// Setup (source tz = Asia/Tokyo = +09):
//   source SET SESSION time_zone='+09:00', INSERT '2024-06-15 11:00:00'
//     ts0 (TIMESTAMP) -> source-internal UTC instant 2024-06-15 02:00:00Z
//     dt0 (DATETIME)  -> literal '2024-06-15 11:00:00'
//   jdbc_url serverTimezone aligned to the SOURCE tz (Asia/Tokyo)
//
// Expectation at Doris (independent of Doris session tz, since cdc renders
// with the source tz, not Doris's; .out has no dependency on Doris tz):
//   ts0 -> '2024-06-15T11:00'  (02:00Z rendered with Asia/Tokyo +09 = 11:00, == source)
//   dt0 -> '2024-06-15T11:00'  (DATETIME has no tz semantics, stored verbatim)
suite("test_streaming_mysql_job_jdbc_servertimezone", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_jdbc_servertimezone_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "streaming_mysql_jdbc_servertimezone"
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

        // jdbc serverTimezone is aligned to the SOURCE db tz (not Doris) so
        // Doris data matches the source wall clock. Log Doris tz only to show
        // the result is independent of it.
        def sourceTz = "+09:00"
        def jdbcTz = "Asia/Tokyo"
        log.info("Doris session time_zone = ${(sql "select @@time_zone")[0][0]}; cdc renders with source tz ${jdbcTz}.")

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table1}"""
            sql """
            create table ${mysqlDb}.${table1} (
                id int primary key,
                tag varchar(32),
                ts0 timestamp null,
                dt0 datetime null
            ) engine=innodb charset=utf8;
            """

            sql """SET SESSION time_zone = '${sourceTz}'"""
            sql """INSERT INTO ${mysqlDb}.${table1} VALUES (1, 'snapshot_tokyo',
                '2024-06-15 11:00:00', '2024-06-15 11:00:00')"""
        }

        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM MYSQL (
                    "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}?serverTimezone=${jdbcTz}&forceConnectionTimeZoneToSession=true",
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

        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                        log.info("snapshot row count: " + cnt)
                        cnt.get(0).get(0) == 1
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex
        }

        qt_desc """desc ${currentDb}.${table1};"""
        qt_select_snapshot """select * from ${currentDb}.${table1} order by id;"""

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """SET SESSION time_zone = '${sourceTz}'"""
            sql """INSERT INTO ${mysqlDb}.${table1} VALUES (2, 'binlog_tokyo',
                '2024-06-15 11:00:00', '2024-06-15 11:00:00')"""
        }

        Awaitility.await().atMost(180, SECONDS)
                .pollInterval(2, SECONDS).until(
                {
                    def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                    cnt.get(0).get(0) == 2
                }
        )

        qt_select_binlog """select * from ${currentDb}.${table1} order by id;"""

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name = '${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
