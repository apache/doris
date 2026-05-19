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

// Guard PG temporal column boundaries across cdc snapshot + binlog paths.
//
// PG distinguishes:
//   timestamp        : "wall clock", no tz info ("local-ish" semantics)
//   timestamp with tz: normalized to UTC at write, displayed via session tz
//   time             : time of day, no tz
//   time with tz     : time of day with explicit tz offset
//   date             : day only
//   interval         : duration (years, months, days, hours, ...)
//
// _all_type only inserts one normal value per column. This suite covers
// the boundaries that actually break customer pipelines:
//   - timestamp vs timestamptz on the SAME input
//   - microsecond fractional precision (PG default = 6)
//   - the epoch and "far future" boundary values
//   - interval with multiple components (year + month + day + hours)
//   - SQL NULL across all temporal columns
//
// snapshot ids 1..5 then binlog ids 101..105 repeat the same themes.
// UPDATE rewrites a few rows to validate UPDATE binlog parsing.
//
// jdbc_url uses timezone=UTC so cdc reads a stable session tz; PG itself
// stores timestamptz as UTC anyway, so the UTC pin only affects how the
// connector renders timestamptz back to a string.
suite("test_streaming_postgres_job_time_types", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_time_types_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "streaming_pg_time_types_pk"
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

        // ===== Prepare PG side =====
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
            sql """
            create table ${pgDB}.${pgSchema}.${table1} (
                id          integer PRIMARY KEY,
                tag         varchar(64),
                ts          timestamp,
                tstz        timestamp with time zone,
                t           time,
                ttz         time with time zone,
                d           date,
                iv          interval
            );
            """

            // ----- Snapshot rows: 5 temporal boundary themes via JDBC path -----
            // epoch: the UNIX epoch second (and an interval of 1 day)
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (1, 'epoch',
                '1970-01-01 00:00:00',
                '1970-01-01 00:00:00+00',
                '00:00:00',
                '00:00:00+00',
                '1970-01-01',
                '1 day'
            )"""

            // microsecond: full 6-digit fractional precision (PG default)
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (2, 'microsecond',
                '2024-06-15 12:34:56.123456',
                '2024-06-15 12:34:56.123456+00',
                '12:34:56.123456',
                '12:34:56.123456+00',
                '2024-06-15',
                '1 year 2 months 3 days 04:05:06'
            )"""

            // distant_future: year 9999
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (3, 'distant_future',
                '9999-12-31 23:59:59',
                '9999-12-31 23:59:59+00',
                '23:59:59',
                '23:59:59+14',
                '9999-12-31',
                '999 years'
            )"""

            // tz_offset: timestamptz / timetz with a non-UTC offset to verify
            // wal2json/pgoutput normalize timestamptz to UTC but preserve timetz offset
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (4, 'tz_offset',
                '2024-06-15 12:00:00',
                '2024-06-15 12:00:00+08',
                '12:00:00',
                '12:00:00+08',
                '2024-06-15',
                '-1 day'
            )"""

            // sql_null: every temporal column is NULL
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, tag) VALUES (5, 'sql_null')"""
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

        qt_desc_time_types """desc ${currentDb}.${table1};"""
        qt_select_snapshot """select id, tag, ts, tstz, t, ttz, d, iv from ${currentDb}.${table1} order by id;"""

        // ===== Binlog phase: same themes through wal2json/pgoutput =====
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (101, 'epoch',
                '1970-01-01 00:00:01', '1970-01-01 00:00:01+00', '00:00:01', '00:00:01+00', '1970-01-02', '2 days')"""

            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (102, 'microsecond',
                '2024-12-31 23:59:59.999999', '2024-12-31 23:59:59.999999+00', '23:59:59.999999', '23:59:59.999999+00', '2024-12-31', '0 years 0 months 0 days 12:34:56.789')"""

            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (103, 'distant_future',
                '9999-01-01 00:00:00', '9999-01-01 00:00:00+00', '00:00:00', '00:00:00-12', '9999-01-01', '1000 years')"""

            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (104, 'tz_offset',
                '2025-01-15 06:00:00', '2025-01-15 06:00:00-05', '06:00:00', '06:00:00-05', '2025-01-15', '15 days')"""

            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, tag) VALUES (105, 'sql_null')"""

            // UPDATEs: validate UPDATE binlog parsing on temporal fields.
            //   id=1 (epoch) -> push timestamp to year 2024
            //   id=2 (microsecond) -> swap interval to a year+month+day form
            //   id=5 (sql_null) -> NULL -> non-null timestamptz
            sql """UPDATE ${pgDB}.${pgSchema}.${table1} SET ts='2024-06-15 12:00:00' WHERE id=1"""
            sql """UPDATE ${pgDB}.${pgSchema}.${table1} SET iv='5 years 6 months 7 days' WHERE id=2"""
            sql """UPDATE ${pgDB}.${pgSchema}.${table1} SET tstz='2024-06-15 12:00:00+00' WHERE id=5"""
        }

        // Wait until all 5 binlog inserts and 3 updates are visible.
        try {
            Awaitility.await().atMost(180, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                        def upd1 = sql """select cast(ts   as string) from ${currentDb}.${table1} where id=1"""
                        def upd2 = sql """select cast(iv   as string) from ${currentDb}.${table1} where id=2"""
                        def upd5 = sql """select cast(tstz as string) from ${currentDb}.${table1} where id=5"""
                        def t1 = upd1.get(0).get(0) == null ? '' : upd1.get(0).get(0).toString()
                        def i2 = upd2.get(0).get(0) == null ? '' : upd2.get(0).get(0).toString()
                        def z5 = upd5.get(0).get(0) == null ? '' : upd5.get(0).get(0).toString()
                        log.info("incr count=" + cnt + " id1.ts=" + t1 + " id2.iv=" + i2 + " id5.tstz=" + z5)
                        cnt.get(0).get(0) == 10 &&
                                t1.startsWith('2024-06-15') &&
                                i2.contains('5') &&
                                z5.startsWith('2024-06-15')
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job (incr): " + showjob)
            log.info("show task (incr): " + showtask)
            throw ex
        }

        qt_select_after_incr """select id, tag, ts, tstz, t, ttz, d, iv from ${currentDb}.${table1} order by id;"""

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
