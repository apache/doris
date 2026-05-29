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

// PG counterpart of test_streaming_mysql_job_jdbc_servertimezone.
//
// Recommended end-to-end tz configuration for data fidelity: set jdbc_url's
// timezone to the SOURCE PG session/server tz, so cdc renders the timestamptz
// / timetz instant back to the exact wall clock the source shows. Doris data
// then stays identical to PG, independent of Doris's own session tz.
//
// Source tz is Asia/Tokyo (+09, no DST), deliberately != Doris default +08, so
// the case proves the rendering follows the SOURCE tz (not Doris). The source
// session uses the offset '+09:00' while jdbc_url uses the IANA name
// 'Asia/Tokyo' (cdc resolves it via ZoneId). Tokyo has no DST, so the offset is
// a constant +09 -- this also matters for timetz: cdc takes its offset at
// Instant.EPOCH, and with a no-DST zone that EPOCH offset equals the source
// offset, so timetz round-trips to the source wall clock too.
//
// Setup (source tz = Asia/Tokyo = +09):
//   source SET TIME ZONE INTERVAL '+09:00' HOUR TO MINUTE, INSERT '2024-06-15 11:00:00'
//     ts   (timestamp)   -> literal '2024-06-15 11:00:00'
//     tstz (timestamptz) -> source-internal UTC instant 2024-06-15 02:00:00Z
//     ttz  (timetz)      -> source-internal UTC time 02:00:00Z
//   jdbc_url timezone aligned to the SOURCE tz (Asia/Tokyo)
//
// Expectation at Doris (independent of Doris session tz, since cdc renders
// with the source tz, not Doris's; .out has no dependency on Doris tz):
//   ts   -> '2024-06-15T11:00'  (verbatim, no tz semantics)
//   tstz -> '2024-06-15T11:00'  (02:00Z rendered with Asia/Tokyo +09 = 11:00, == source)
//   ttz  -> '11:00'             (02:00Z time rendered with Asia/Tokyo +09 = 11:00, == source)
suite("test_streaming_postgres_job_jdbc_servertimezone", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_jdbc_servertimezone_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "streaming_pg_jdbc_servertimezone"
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port = context.config.otherConfigs.get("pg_14_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        // jdbc timezone is aligned to the SOURCE db tz (not Doris) so Doris
        // data matches the source wall clock. Log Doris tz only to show the
        // result is independent of it.
        def sourceTz = "+09:00"
        def jdbcTz = "Asia/Tokyo"
        log.info("Doris session time_zone = ${(sql "select @@time_zone")[0][0]}; cdc renders with source tz ${jdbcTz}.")

        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
            sql """
            create table ${pgDB}.${pgSchema}.${table1} (
                id   integer PRIMARY KEY,
                tag  varchar(32),
                ts   timestamp,
                tstz timestamp with time zone,
                ttz  time with time zone
            );
            """

            sql """SET TIME ZONE INTERVAL '+09:00' HOUR TO MINUTE"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (1, 'snapshot_tokyo',
                '2024-06-15 11:00:00', '2024-06-15 11:00:00', '11:00:00')"""
        }

        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM POSTGRES (
                    "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}?timezone=${jdbcTz}",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "org.postgresql.Driver",
                    "user" = "${pgUser}",
                    "password" = "${pgPassword}",
                    "database" = "${pgDB}",
                    "schema" = "${pgSchema}",
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

        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """SET TIME ZONE INTERVAL '+09:00' HOUR TO MINUTE"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (2, 'binlog_tokyo',
                '2024-06-15 11:00:00', '2024-06-15 11:00:00', '11:00:00')"""
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
