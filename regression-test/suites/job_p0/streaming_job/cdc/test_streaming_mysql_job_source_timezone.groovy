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

// Verify cdc tz handling when source session tz differs from cdc-client tz.
//
// MySQL TIMESTAMP stores a UTC instant; the session tz only affects how the
// wall clock is parsed on write and rendered on read. DATETIME / DATE are
// wall-clock verbatim, no tz transform.
//
// Coverage in one table:
//   * source session tz set to +08 / -05 / +00, same wall clock written ->
//     different UTC instants prove cdc honors source session tz (not a
//     hardcoded offset, not the cdc-client JVM tz).
//   * NULL row across every temporal column.
//   * MySQL TIMESTAMP epoch lower bound ('1970-01-01 00:00:01Z').
//   * Binlog path mirrors snapshot themes, plus an UPDATE that rewrites a
//     TIMESTAMP column under +08.
//
// jdbc_url uses serverTimezone=UTC so cdc renders TIMESTAMP back to UTC
// wall clock regardless of the source session offset.
suite("test_streaming_mysql_job_source_timezone", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_source_timezone_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "streaming_mysql_source_timezone"
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

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table1}"""
            sql """
            create table ${mysqlDb}.${table1} (
                id int primary key,
                tag varchar(32),
                ts0 timestamp null,
                ts3 timestamp(3) null,
                ts6 timestamp(6) null,
                dt0 datetime null,
                dt6 datetime(6) null,
                d date null
            ) engine=innodb charset=utf8;
            """

            // id=1: +08 baseline
            sql """SET SESSION time_zone = '+08:00'"""
            sql """INSERT INTO ${mysqlDb}.${table1} VALUES (1, 'snapshot_plus08',
                '2024-06-15 20:00:00',
                '2024-06-15 20:00:00.123',
                '2024-06-15 20:00:00.123456',
                '2024-06-15 20:00:00',
                '2024-06-15 20:00:00.123456',
                '2024-06-15')"""

            // id=2: -05 -> same wall clock crosses to next UTC day (20:00 -05 = 01:00 next day UTC)
            sql """SET SESSION time_zone = '-05:00'"""
            sql """INSERT INTO ${mysqlDb}.${table1} VALUES (2, 'snapshot_minus05',
                '2024-06-15 20:00:00',
                '2024-06-15 20:00:00.123',
                '2024-06-15 20:00:00.123456',
                '2024-06-15 20:00:00',
                '2024-06-15 20:00:00.123456',
                '2024-06-15')"""

            // id=3: +00 -> wall clock == UTC instant
            sql """SET SESSION time_zone = '+00:00'"""
            sql """INSERT INTO ${mysqlDb}.${table1} VALUES (3, 'snapshot_utc',
                '2024-06-15 20:00:00',
                '2024-06-15 20:00:00.123',
                '2024-06-15 20:00:00.123456',
                '2024-06-15 20:00:00',
                '2024-06-15 20:00:00.123456',
                '2024-06-15')"""

            // id=4: NULL across every nullable column
            sql """INSERT INTO ${mysqlDb}.${table1} (id, tag) VALUES (4, 'snapshot_null')"""

            // id=5: epoch lower bound. MySQL TIMESTAMP min is '1970-01-01 00:00:01' UTC,
            // so under +08 the smallest legal wall clock is '1970-01-01 08:00:01'.
            sql """SET SESSION time_zone = '+08:00'"""
            sql """INSERT INTO ${mysqlDb}.${table1} VALUES (5, 'snapshot_epoch_plus08',
                '1970-01-01 08:00:01',
                '1970-01-01 08:00:01.123',
                '1970-01-01 08:00:01.123456',
                '1970-01-01 08:00:01',
                '1970-01-01 08:00:01.123456',
                '1970-01-01')"""
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

        // Binlog phase: same tz themes through binlog, plus an UPDATE that
        // rewrites ts0 on id=1 under +08 to confirm UPDATE follows the same
        // tz codepath as INSERT.
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """SET SESSION time_zone = '+08:00'"""
            sql """INSERT INTO ${mysqlDb}.${table1} VALUES (101, 'binlog_plus08',
                '2024-06-15 20:00:00',
                '2024-06-15 20:00:00.123',
                '2024-06-15 20:00:00.123456',
                '2024-06-15 20:00:00',
                '2024-06-15 20:00:00.123456',
                '2024-06-15')"""

            sql """SET SESSION time_zone = '-05:00'"""
            sql """INSERT INTO ${mysqlDb}.${table1} VALUES (102, 'binlog_minus05',
                '2024-06-15 20:00:00',
                '2024-06-15 20:00:00.123',
                '2024-06-15 20:00:00.123456',
                '2024-06-15 20:00:00',
                '2024-06-15 20:00:00.123456',
                '2024-06-15')"""

            sql """SET SESSION time_zone = '+00:00'"""
            sql """INSERT INTO ${mysqlDb}.${table1} VALUES (103, 'binlog_utc',
                '2024-06-15 20:00:00',
                '2024-06-15 20:00:00.123',
                '2024-06-15 20:00:00.123456',
                '2024-06-15 20:00:00',
                '2024-06-15 20:00:00.123456',
                '2024-06-15')"""

            sql """SET SESSION time_zone = '+08:00'"""
            sql """INSERT INTO ${mysqlDb}.${table1} VALUES (105, 'binlog_epoch_plus08',
                '1970-01-01 08:00:01',
                '1970-01-01 08:00:01.123',
                '1970-01-01 08:00:01.123456',
                '1970-01-01 08:00:01',
                '1970-01-01 08:00:01.123456',
                '1970-01-01')"""

            // UPDATE: id=1 was '20:00 +08' (UTC 12:00). Push wall clock to '22:00 +08'
            // (UTC 14:00) so we can poll for completion.
            sql """UPDATE ${mysqlDb}.${table1} SET ts0 = '2024-06-15 22:00:00' WHERE id = 1"""
        }

        // Wait for 4 binlog INSERTs + UPDATE on id=1 to settle.
        Awaitility.await().atMost(180, SECONDS)
                .pollInterval(2, SECONDS).until(
                {
                    def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                    if (cnt.get(0).get(0) != 9) return false
                    def updated = sql """select ts0 from ${currentDb}.${table1} where id = 1"""
                    return updated.get(0).get(0).toString().startsWith('2024-06-15T14:00')
                }
        )

        qt_select_binlog """select * from ${currentDb}.${table1} order by id;"""

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name = '${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
