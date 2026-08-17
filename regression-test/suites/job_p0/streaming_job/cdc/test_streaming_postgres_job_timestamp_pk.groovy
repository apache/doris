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

// Cover PG timestamp / timestamptz chunk-key paths: timestamp (no tz,
// driver returns LocalDateTime), timestamptz (driver returns
// OffsetDateTime), plus a composite (timestamptz, id) PK to exercise
// multi-column locating.
//
// Source SET TIME ZONE = UTC and jdbc_url timezone=UTC so the timestamptz
// wall clock written by INSERT equals the value cdc renders, and the .out
// can be filled deterministically.
suite("test_streaming_postgres_job_timestamp_pk", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_timestamp_pk_name"
    def currentDb = (sql "select database()")[0][0]
    def tableTs = "events_pg_timestamp_pk"
    def tableTstz = "events_pg_timestamptz_pk"
    def tableComposite = "events_pg_timestamptz_id_pk"
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${tableTs} force"""
    sql """drop table if exists ${currentDb}.${tableTstz} force"""
    sql """drop table if exists ${currentDb}.${tableComposite} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port = context.config.otherConfigs.get("pg_14_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """SET TIME ZONE INTERVAL '+00:00' HOUR TO MINUTE"""

            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${tableTs}"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${tableTs} (
                  event_ts timestamp(6) NOT NULL,
                  payload  varchar(64),
                  PRIMARY KEY (event_ts)
                )"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableTs} VALUES ('2024-01-01 00:00:00.000000', 'A1')"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableTs} VALUES ('2024-06-15 12:00:00.123456', 'B1')"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableTs} VALUES ('2025-01-01 00:00:00.000000', 'C1')"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableTs} VALUES ('2025-06-15 12:34:56.999999', 'D1')"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableTs} VALUES ('2026-01-01 00:00:00.000000', 'E1')"""

            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${tableTstz}"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${tableTstz} (
                  event_ts timestamptz(6) NOT NULL,
                  payload  varchar(64),
                  PRIMARY KEY (event_ts)
                )"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableTstz} VALUES ('2024-01-01 00:00:00.000000', 'A1')"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableTstz} VALUES ('2024-06-15 12:00:00.123456', 'B1')"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableTstz} VALUES ('2025-01-01 00:00:00.000000', 'C1')"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableTstz} VALUES ('2025-06-15 12:34:56.999999', 'D1')"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableTstz} VALUES ('2026-01-01 00:00:00.000000', 'E1')"""

            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${tableComposite}"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${tableComposite} (
                  event_ts timestamptz(6) NOT NULL,
                  id       integer        NOT NULL,
                  payload  varchar(64),
                  PRIMARY KEY (event_ts, id)
                )"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableComposite} VALUES ('2024-02-01 00:00:00.000000', 1, 'A2')"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableComposite} VALUES ('2024-02-01 00:00:00.000000', 2, 'B2')"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableComposite} VALUES ('2024-02-02 12:00:00.500000', 3, 'C2')"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableComposite} VALUES ('2024-02-03 23:59:59.999999', 4, 'D2')"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableComposite} VALUES ('2024-02-04 00:00:00.000000', 5, 'E2')"""
        }

        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM POSTGRES (
                    "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}?timezone=UTC",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "org.postgresql.Driver",
                    "user" = "${pgUser}",
                    "password" = "${pgPassword}",
                    "database" = "${pgDB}",
                    "schema" = "${pgSchema}",
                    "include_tables" = "${tableTs},${tableTstz},${tableComposite}",
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
                        def c2 = sql """select count(1) from ${currentDb}.${tableTstz}"""
                        def c3 = sql """select count(1) from ${currentDb}.${tableComposite}"""
                        log.info("snapshot row count ts=${c1} tstz=${c2} composite=${c3}")
                        c1.get(0).get(0) == 5 && c2.get(0).get(0) == 5 && c3.get(0).get(0) == 5
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
        qt_select_snapshot_timestamptz_pk """select event_ts, payload from ${currentDb}.${tableTstz} order by event_ts asc"""
        qt_select_snapshot_composite_pk """select event_ts, id, payload from ${currentDb}.${tableComposite} order by event_ts asc, id asc"""

        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """SET TIME ZONE INTERVAL '+00:00' HOUR TO MINUTE"""

            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableTs} VALUES ('2026-06-01 00:00:00.000000', 'F2')"""
            sql """UPDATE ${pgDB}.${pgSchema}.${tableTs} SET payload='B2_upd' WHERE event_ts='2024-06-15 12:00:00.123456'"""
            sql """DELETE FROM ${pgDB}.${pgSchema}.${tableTs} WHERE event_ts='2025-06-15 12:34:56.999999'"""

            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableTstz} VALUES ('2026-06-01 00:00:00.000000', 'F2')"""
            sql """UPDATE ${pgDB}.${pgSchema}.${tableTstz} SET payload='B2_upd' WHERE event_ts='2024-06-15 12:00:00.123456'"""
            sql """DELETE FROM ${pgDB}.${pgSchema}.${tableTstz} WHERE event_ts='2025-06-15 12:34:56.999999'"""

            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableComposite} VALUES ('2024-02-05 00:00:00.000000', 6, 'F3')"""
            sql """UPDATE ${pgDB}.${pgSchema}.${tableComposite} SET payload='C3_upd' WHERE event_ts='2024-02-02 12:00:00.500000' AND id=3"""
            sql """DELETE FROM ${pgDB}.${pgSchema}.${tableComposite} WHERE event_ts='2024-02-03 23:59:59.999999' AND id=4"""
        }

        try {
            Awaitility.await().atMost(180, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def c1 = sql """select count(1) from ${currentDb}.${tableTs}"""
                        def c2 = sql """select count(1) from ${currentDb}.${tableTstz}"""
                        def c3 = sql """select count(1) from ${currentDb}.${tableComposite}"""
                        def upd1 = sql """select payload from ${currentDb}.${tableTs} where event_ts='2024-06-15 12:00:00.123456'"""
                        def upd2 = sql """select payload from ${currentDb}.${tableTstz} where event_ts='2024-06-15 12:00:00.123456'"""
                        def upd3 = sql """select payload from ${currentDb}.${tableComposite} where event_ts='2024-02-02 12:00:00.500000' and id=3"""
                        def del1 = sql """select count(1) from ${currentDb}.${tableTs} where event_ts='2025-06-15 12:34:56.999999'"""
                        def del2 = sql """select count(1) from ${currentDb}.${tableTstz} where event_ts='2025-06-15 12:34:56.999999'"""
                        def del3 = sql """select count(1) from ${currentDb}.${tableComposite} where event_ts='2024-02-03 23:59:59.999999' and id=4"""
                        def p1 = upd1.size() == 0 ? null : upd1.get(0).get(0)
                        def p2 = upd2.size() == 0 ? null : upd2.get(0).get(0)
                        def p3 = upd3.size() == 0 ? null : upd3.get(0).get(0)
                        log.info("incr ts=${c1} tstz=${c2} comp=${c3} ts_upd=${p1} tstz_upd=${p2} comp_upd=${p3} dels=${del1}/${del2}/${del3}")
                        c1.get(0).get(0) == 5 && c2.get(0).get(0) == 5 && c3.get(0).get(0) == 5 &&
                                p1 == 'B2_upd' && p2 == 'B2_upd' && p3 == 'C3_upd' &&
                                del1.get(0).get(0) == 0 && del2.get(0).get(0) == 0 && del3.get(0).get(0) == 0
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
        qt_select_after_incr_timestamptz_pk """select event_ts, payload from ${currentDb}.${tableTstz} order by event_ts asc"""
        qt_select_after_incr_composite_pk """select event_ts, id, payload from ${currentDb}.${tableComposite} order by event_ts asc, id asc"""

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
