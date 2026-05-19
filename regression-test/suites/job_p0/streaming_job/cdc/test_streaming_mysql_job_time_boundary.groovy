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

// Guard MySQL time-type boundaries that all_type does NOT cover:
//   - DATE / DATETIME(6) extremes: 1000-01-01 ~ 9999-12-31
//   - TIME(6) negative bound: -838:59:59 ~ 838:59:59
//   - TIMESTAMP(6) UNIX-epoch bounds: 1970-01-01 00:00:01 UTC ~ 2038-01-19 03:14:07 UTC
//   - YEAR bounds: 1901 ~ 2155
//   - 0000-00-00 / 0000-00-00 00:00:00 (legacy non-strict mode rows)
//   - microsecond fractional seconds (3-arg and 6-arg precision)
//   - SQL NULL across all temporal columns
//
// Same boundary themes are run twice: ids 1-5 via JDBC/snapshot, ids 101-105
// via binlog. Plus UPDATEs that switch a row to another boundary value.
//
// Notes:
//   - sql_mode is cleared per session to allow zero dates.
//   - jdbc_url uses serverTimezone=UTC so TIMESTAMP comparisons stay stable.
//   - TIMESTAMP cannot store all-zero, so the zero_date row leaves it NULL.
suite("test_streaming_mysql_job_time_boundary", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_time_boundary_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "streaming_time_boundary_pk"
    def mysqlDb = "test_cdc_time_db"

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
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}?serverTimezone=UTC") {
            // allow zero dates and other lax-mode behaviors so we can insert 0000-00-00
            sql """SET SESSION sql_mode = ''"""

            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table1}"""
            sql """
            create table ${mysqlDb}.${table1} (
              `id` int primary key,
              `tag` varchar(64),
              `date_col` date,
              `datetime_col` datetime(6),
              `time_col` time(6),
              `timestamp_col` timestamp(6) null,
              `year_col` year
            ) engine=innodb default charset=utf8;
            """

            // ----- Snapshot rows: 5 boundary themes via JDBC path -----
            // all_min: lowest valid value for each temporal type. TIMESTAMP min is 1970-01-01 00:00:01 UTC.
            sql """insert into ${mysqlDb}.${table1} values (1, 'all_min',           '1000-01-01', '1000-01-01 00:00:00.000000', '-838:59:59.000000', '1970-01-01 00:00:01.000000', 1901)"""
            // all_max: highest valid value. TIMESTAMP max is 2038-01-19 03:14:07 UTC (UNIX epoch upper bound).
            sql """insert into ${mysqlDb}.${table1} values (2, 'all_max',           '9999-12-31', '9999-12-31 23:59:59.999999', '838:59:59.999999',  '2038-01-19 03:14:07.000000', 2155)"""
            // zero_date: legacy 0000-00-00 (NO_ZERO_DATE turned off). TIMESTAMP cannot be all-zero, so leave NULL.
            sql """insert into ${mysqlDb}.${table1} values (3, 'zero_date',         '0000-00-00', '0000-00-00 00:00:00.000000', '00:00:00.000000',  NULL,                           NULL)"""
            // microsecond precision: ensures 6-digit fractional seconds survive both paths.
            sql """insert into ${mysqlDb}.${table1} values (4, 'fractional_second', '2024-06-15', '2024-06-15 12:34:56.123456', '12:34:56.123456',  '2024-06-15 12:34:56.123456', 2024)"""
            // sql_null: NULL across every temporal column
            sql """insert into ${mysqlDb}.${table1} values (5, 'sql_null',          NULL,         NULL,                         NULL,               NULL,                           NULL)"""
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

        qt_desc_time_boundary """desc ${currentDb}.${table1};"""
        qt_select_snapshot """select id, tag, date_col, datetime_col, time_col, timestamp_col, year_col from ${currentDb}.${table1} order by id;"""

        // ===== Binlog phase: repeat the SAME 5 boundary themes through binlog path =====
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}?serverTimezone=UTC") {
            sql """SET SESSION sql_mode = ''"""

            sql """insert into ${mysqlDb}.${table1} values (101, 'all_min',           '1000-01-01', '1000-01-01 00:00:00.000000', '-838:59:59.000000', '1970-01-01 00:00:01.000000', 1901)"""
            sql """insert into ${mysqlDb}.${table1} values (102, 'all_max',           '9999-12-31', '9999-12-31 23:59:59.999999', '838:59:59.999999',  '2038-01-19 03:14:07.000000', 2155)"""
            sql """insert into ${mysqlDb}.${table1} values (103, 'zero_date',         '0000-00-00', '0000-00-00 00:00:00.000000', '00:00:00.000000',  NULL,                           NULL)"""
            sql """insert into ${mysqlDb}.${table1} values (104, 'fractional_second', '2024-06-15', '2024-06-15 12:34:56.123456', '12:34:56.123456',  '2024-06-15 12:34:56.123456', 2024)"""
            sql """insert into ${mysqlDb}.${table1} values (105, 'sql_null',          NULL,         NULL,                         NULL,               NULL,                           NULL)"""

            // UPDATEs: validate UPDATE binlog parsing on temporal extremes.
            //   id=1 (all_min) -> push date to 9999-12-31 via UPDATE
            //   id=3 (zero_date) -> recover timestamp_col from NULL to a valid 2038 value
            //   id=5 (sql_null) -> set datetime_col to microsecond-precision value
            sql """update ${mysqlDb}.${table1} set date_col='9999-12-31' where id=1"""
            sql """update ${mysqlDb}.${table1} set timestamp_col='2038-01-19 03:14:07.000000' where id=3"""
            sql """update ${mysqlDb}.${table1} set datetime_col='2024-06-15 12:34:56.123456' where id=5"""
        }

        // Wait until all 5 binlog inserts and 3 updates are visible.
        try {
            Awaitility.await().atMost(180, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                        def upd1  = sql """select cast(date_col as string) from ${currentDb}.${table1} where id=1"""
                        def upd3  = sql """select cast(timestamp_col as string) from ${currentDb}.${table1} where id=3"""
                        def upd5  = sql """select cast(datetime_col as string) from ${currentDb}.${table1} where id=5"""
                        def d1 = upd1.get(0).get(0) == null ? '' : upd1.get(0).get(0).toString()
                        def t3 = upd3.get(0).get(0) == null ? '' : upd3.get(0).get(0).toString()
                        def d5 = upd5.get(0).get(0) == null ? '' : upd5.get(0).get(0).toString()
                        log.info("incr count=" + cnt + " id1.date=" + d1 + " id3.timestamp=" + t3 + " id5.datetime=" + d5)
                        cnt.get(0).get(0) == 10 &&
                                d1.startsWith('9999-12-31') &&
                                t3.startsWith('2038-01-19') &&
                                d5.startsWith('2024-06-15')
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job (incr): " + showjob)
            log.info("show task (incr): " + showtask)
            throw ex
        }

        qt_select_after_incr """select id, tag, date_col, datetime_col, time_col, timestamp_col, year_col from ${currentDb}.${table1} order by id;"""

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
