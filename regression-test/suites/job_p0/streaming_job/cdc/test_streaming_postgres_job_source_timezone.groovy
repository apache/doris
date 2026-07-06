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

// PG counterpart of test_streaming_mysql_job_source_timezone.
//
// PG temporal semantics relevant to cdc tz handling:
//   timestamp   - wall clock, no tz; debezium emits epoch-style schema, cdc
//                 bypasses serverTimeZone.
//   timestamptz - normalized to UTC on write per session TimeZone; debezium
//                 emits ZonedTimestamp ISO string, cdc renders it using
//                 serverTimeZone.
//   timetz      - time-of-day with offset. Debezium emits ZonedTime, a
//                 UTC-normalized OffsetTime; cdc keeps it as-is (offset
//                 preserved) rather than rendering into a named zone, since a
//                 date-less time cannot resolve DST. Mirrors Debezium/PostgreSQL.
//   date        - no tz; literal day, must not drift across +08 boundary.
//
// Coverage:
//   * source session tz set to +08 / -05 / +00, same wall clock written ->
//     different UTC instants for timestamptz prove cdc honors source session.
//   * NULL row across every temporal column.
//   * epoch lower bound ('1970-01-01 00:00:01Z').
//   * Binlog path mirrors snapshot themes, plus an UPDATE on tstz0 under +08.
//
// jdbc_url uses timezone=UTC so cdc renders timestamptz back to UTC wall
// clock regardless of the source session offset.
suite("test_streaming_postgres_job_source_timezone", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_source_timezone_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "streaming_pg_source_timezone"
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

        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
            sql """
            create table ${pgDB}.${pgSchema}.${table1} (
                id    integer PRIMARY KEY,
                tag   varchar(32),
                ts    timestamp,
                tstz0 timestamptz(0),
                tstz3 timestamptz(3),
                tstz6 timestamptz(6),
                ttz   time with time zone,
                d     date
            );
            """

            // INTERVAL form avoids depending on the container's tzdata.
            // id=1: +08 baseline
            sql """SET TIME ZONE INTERVAL '+08:00' HOUR TO MINUTE"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (1, 'snapshot_plus08',
                '2024-06-15 20:00:00',
                '2024-06-15 20:00:00',
                '2024-06-15 20:00:00.123',
                '2024-06-15 20:00:00.123456',
                '20:00:00.123456',
                '2024-06-15')"""

            // id=2: -05 -> same wall clock crosses to next UTC day for timestamptz
            sql """SET TIME ZONE INTERVAL '-05:00' HOUR TO MINUTE"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (2, 'snapshot_minus05',
                '2024-06-15 20:00:00',
                '2024-06-15 20:00:00',
                '2024-06-15 20:00:00.123',
                '2024-06-15 20:00:00.123456',
                '20:00:00.123456',
                '2024-06-15')"""

            // id=3: +00 -> wall clock == UTC instant
            sql """SET TIME ZONE INTERVAL '+00:00' HOUR TO MINUTE"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (3, 'snapshot_utc',
                '2024-06-15 20:00:00',
                '2024-06-15 20:00:00',
                '2024-06-15 20:00:00.123',
                '2024-06-15 20:00:00.123456',
                '20:00:00.123456',
                '2024-06-15')"""

            // id=4: NULL across every nullable column
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, tag) VALUES (4, 'snapshot_null')"""

            // id=5: epoch lower bound under +08
            sql """SET TIME ZONE INTERVAL '+08:00' HOUR TO MINUTE"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (5, 'snapshot_epoch_plus08',
                '1970-01-01 08:00:01',
                '1970-01-01 08:00:01',
                '1970-01-01 08:00:01.123',
                '1970-01-01 08:00:01.123456',
                '08:00:01.123456',
                '1970-01-01')"""
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

        qt_desc """desc ${currentDb}.${table1};"""
        qt_select_snapshot """select * from ${currentDb}.${table1} order by id;"""

        // Binlog phase: same tz themes, plus an UPDATE that rewrites tstz0
        // on id=1 under +08 to confirm UPDATE follows the same tz codepath.
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """SET TIME ZONE INTERVAL '+08:00' HOUR TO MINUTE"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (101, 'binlog_plus08',
                '2024-06-15 20:00:00',
                '2024-06-15 20:00:00',
                '2024-06-15 20:00:00.123',
                '2024-06-15 20:00:00.123456',
                '20:00:00.123456',
                '2024-06-15')"""

            sql """SET TIME ZONE INTERVAL '-05:00' HOUR TO MINUTE"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (102, 'binlog_minus05',
                '2024-06-15 20:00:00',
                '2024-06-15 20:00:00',
                '2024-06-15 20:00:00.123',
                '2024-06-15 20:00:00.123456',
                '20:00:00.123456',
                '2024-06-15')"""

            sql """SET TIME ZONE INTERVAL '+00:00' HOUR TO MINUTE"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (103, 'binlog_utc',
                '2024-06-15 20:00:00',
                '2024-06-15 20:00:00',
                '2024-06-15 20:00:00.123',
                '2024-06-15 20:00:00.123456',
                '20:00:00.123456',
                '2024-06-15')"""

            sql """SET TIME ZONE INTERVAL '+08:00' HOUR TO MINUTE"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (105, 'binlog_epoch_plus08',
                '1970-01-01 08:00:01',
                '1970-01-01 08:00:01',
                '1970-01-01 08:00:01.123',
                '1970-01-01 08:00:01.123456',
                '08:00:01.123456',
                '1970-01-01')"""

            // UPDATE: id=1 tstz0 was '20:00 +08' (UTC 12:00). Push wall clock to
            // '22:00 +08' (UTC 14:00) so we can poll for completion.
            sql """UPDATE ${pgDB}.${pgSchema}.${table1} SET tstz0 = '2024-06-15 22:00:00' WHERE id = 1"""
        }

        // Wait for 4 binlog INSERTs + UPDATE on id=1 to settle.
        Awaitility.await().atMost(180, SECONDS)
                .pollInterval(2, SECONDS).until(
                {
                    def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                    if (cnt.get(0).get(0) != 9) return false
                    def updated = sql """select tstz0 from ${currentDb}.${table1} where id = 1"""
                    return updated.get(0).get(0).toString().startsWith('2024-06-15T14:00')
                }
        )

        qt_select_binlog """select * from ${currentDb}.${table1} order by id;"""

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name = '${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
