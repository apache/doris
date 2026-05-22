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

// Cover TIMESTAMP chunk-key path. TIMESTAMP is the epoch+tz column type,
// driver returns java.time.LocalDateTime by default; chunk-bound JSON
// round-trip exercises the LocalDateTime branch in
// AbstractCdcSourceReader.convertBound.
//
// Both source session and jdbc_url are pinned to UTC so the TIMESTAMP wall
// clock written by INSERT equals the value cdc renders, and the .out can
// be filled deterministically.
suite("test_streaming_mysql_job_timestamp_pk", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_timestamp_pk_name"
    def currentDb = (sql "select database()")[0][0]
    def tableTs = "events_mysql_timestamp_pk"
    def tableComposite = "events_mysql_timestamp_id_pk"
    def mysqlDb = "test_cdc_db"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${tableTs} force"""
    sql """drop table if exists ${currentDb}.${tableComposite} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}?serverTimezone=UTC") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """SET SESSION time_zone = '+00:00'"""

            sql """DROP TABLE IF EXISTS ${mysqlDb}.${tableTs}"""
            sql """CREATE TABLE ${mysqlDb}.${tableTs} (
                  `event_ts` timestamp(6) NOT NULL,
                  `payload` varchar(64),
                  PRIMARY KEY (`event_ts`)
                ) ENGINE=InnoDB"""
            sql """INSERT INTO ${mysqlDb}.${tableTs} VALUES ('2024-01-01 00:00:00.000000', 'A1')"""
            sql """INSERT INTO ${mysqlDb}.${tableTs} VALUES ('2024-06-15 12:00:00.123456', 'B1')"""
            sql """INSERT INTO ${mysqlDb}.${tableTs} VALUES ('2025-01-01 00:00:00.000000', 'C1')"""
            sql """INSERT INTO ${mysqlDb}.${tableTs} VALUES ('2025-06-15 12:34:56.999999', 'D1')"""
            sql """INSERT INTO ${mysqlDb}.${tableTs} VALUES ('2026-01-01 00:00:00.000000', 'E1')"""

            sql """DROP TABLE IF EXISTS ${mysqlDb}.${tableComposite}"""
            sql """CREATE TABLE ${mysqlDb}.${tableComposite} (
                  `event_ts` timestamp(6) NOT NULL,
                  `id` int NOT NULL,
                  `payload` varchar(64),
                  PRIMARY KEY (`event_ts`, `id`)
                ) ENGINE=InnoDB"""
            sql """INSERT INTO ${mysqlDb}.${tableComposite} VALUES ('2024-02-01 00:00:00.000000', 1, 'A2')"""
            sql """INSERT INTO ${mysqlDb}.${tableComposite} VALUES ('2024-02-01 00:00:00.000000', 2, 'B2')"""
            sql """INSERT INTO ${mysqlDb}.${tableComposite} VALUES ('2024-02-02 12:00:00.500000', 3, 'C2')"""
            sql """INSERT INTO ${mysqlDb}.${tableComposite} VALUES ('2024-02-03 23:59:59.999999', 4, 'D2')"""
            sql """INSERT INTO ${mysqlDb}.${tableComposite} VALUES ('2024-02-04 00:00:00.000000', 5, 'E2')"""
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
                    "include_tables" = "${tableTs},${tableComposite}",
                    "offset" = "initial",
                    "snapshot_split_size" = "2"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def c1 = sql """select count(1) from ${currentDb}.${tableTs}"""
                        def c2 = sql """select count(1) from ${currentDb}.${tableComposite}"""
                        log.info("snapshot row count ts=${c1} composite=${c2}")
                        c1.get(0).get(0) == 5 && c2.get(0).get(0) == 5
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex
        }

        qt_select_snapshot_timestamp_pk """select event_ts, payload from ${currentDb}.${tableTs} order by event_ts asc"""
        qt_select_snapshot_composite_pk """select event_ts, id, payload from ${currentDb}.${tableComposite} order by event_ts asc, id asc"""

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}?serverTimezone=UTC") {
            sql """SET SESSION time_zone = '+00:00'"""

            sql """INSERT INTO ${mysqlDb}.${tableTs} VALUES ('2026-06-01 00:00:00.000000', 'F2')"""
            sql """UPDATE ${mysqlDb}.${tableTs} SET payload='B2_upd' WHERE event_ts='2024-06-15 12:00:00.123456'"""
            sql """DELETE FROM ${mysqlDb}.${tableTs} WHERE event_ts='2025-06-15 12:34:56.999999'"""

            sql """INSERT INTO ${mysqlDb}.${tableComposite} VALUES ('2024-02-05 00:00:00.000000', 6, 'F3')"""
            sql """UPDATE ${mysqlDb}.${tableComposite} SET payload='C3_upd' WHERE event_ts='2024-02-02 12:00:00.500000' AND id=3"""
            sql """DELETE FROM ${mysqlDb}.${tableComposite} WHERE event_ts='2024-02-03 23:59:59.999999' AND id=4"""
        }

        try {
            Awaitility.await().atMost(180, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def c1 = sql """select count(1) from ${currentDb}.${tableTs}"""
                        def c2 = sql """select count(1) from ${currentDb}.${tableComposite}"""
                        def upd1 = sql """select payload from ${currentDb}.${tableTs} where event_ts='2024-06-15 12:00:00.123456'"""
                        def upd2 = sql """select payload from ${currentDb}.${tableComposite} where event_ts='2024-02-02 12:00:00.500000' and id=3"""
                        def del1 = sql """select count(1) from ${currentDb}.${tableTs} where event_ts='2025-06-15 12:34:56.999999'"""
                        def del2 = sql """select count(1) from ${currentDb}.${tableComposite} where event_ts='2024-02-03 23:59:59.999999' and id=4"""
                        def p1 = upd1.size() == 0 ? null : upd1.get(0).get(0)
                        def p2 = upd2.size() == 0 ? null : upd2.get(0).get(0)
                        log.info("incr ts=${c1} composite=${c2} ts_upd=${p1} comp_upd=${p2} ts_del=${del1} comp_del=${del2}")
                        c1.get(0).get(0) == 5 && c2.get(0).get(0) == 5 &&
                                p1 == 'B2_upd' && p2 == 'C3_upd' &&
                                del1.get(0).get(0) == 0 && del2.get(0).get(0) == 0
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job (incr): " + showjob)
            log.info("show task (incr): " + showtask)
            throw ex
        }

        qt_select_after_incr_timestamp_pk """select event_ts, payload from ${currentDb}.${tableTs} order by event_ts asc"""
        qt_select_after_incr_composite_pk """select event_ts, id, payload from ${currentDb}.${tableComposite} order by event_ts asc, id asc"""

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
