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

suite("test_streaming_postgres_job_schema_change_ignore",
        "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def jobName = "test_streaming_pg_schema_change_ignore"
        def currentDb = (sql "select database()")[0][0]
        String pgPort = context.config.otherConfigs.get("pg_14_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String jdbcUrl = "jdbc:postgresql://${externalEnvIp}:${pgPort}/postgres"
        String driverUrl = "https://${getS3BucketName()}.${getS3Endpoint()}/regression/jdbc_driver/postgresql-42.5.0.jar"

        sql """DROP JOB IF EXISTS where jobname='${jobName}'"""
        sql "DROP TABLE IF EXISTS pg_schema_change_ignore FORCE"
        connect("postgres", "123456", jdbcUrl) {
            sql "DROP TABLE IF EXISTS cdc_test.pg_schema_change_ignore"
            sql """CREATE TABLE cdc_test.pg_schema_change_ignore (
                       id INT PRIMARY KEY, value_col INT, legacy INT)"""
            sql "INSERT INTO cdc_test.pg_schema_change_ignore VALUES (1, 10, 100)"
        }

        def waitForCommit = {
            long completed = (sql """select SucceedTaskCount from jobs("type"="insert")
                                       where Name='${jobName}'""")[0][0] as long
            Awaitility.await().atMost(180, SECONDS).pollInterval(1, SECONDS).until({
                (sql """select SucceedTaskCount from jobs("type"="insert")
                         where Name='${jobName}'""")[0][0] as long > completed
            })
        }
        def waitForRow = { int id ->
            Awaitility.await().atMost(180, SECONDS).pollInterval(1, SECONDS).until({
                (sql "SELECT COUNT(*) FROM pg_schema_change_ignore WHERE id=${id}")[0][0] as int == 1
            })
            // Do not change the source again until the row's schema and offset have committed.
            waitForCommit()
        }
        def waitForRunning = {
            Awaitility.await().atMost(120, SECONDS).pollInterval(1, SECONDS).until({
                (sql """select Status from jobs("type"="insert") where Name='${jobName}'""")[0][0] == 'RUNNING'
            })
        }

        sql """CREATE JOB ${jobName} ON STREAMING
            FROM POSTGRES (
                "jdbc_url"="${jdbcUrl}", "driver_url"="${driverUrl}",
                "driver_class"="org.postgresql.Driver", "user"="postgres", "password"="123456",
                "database"="postgres", "schema"="cdc_test", "include_tables"="pg_schema_change_ignore",
                "offset"="initial", "schema_change_behavior"="ignore"
            ) TO DATABASE ${currentDb} ("table.create.properties.replication_num"="1")"""
        waitForRow(1)
        Awaitility.await().atMost(300, SECONDS).pollInterval(1, SECONDS).until({
            (sql """select SucceedTaskCount from jobs("type"="insert")
                     where Name='${jobName}'""")[0][0] as long >= 2
        })
        waitForCommit()

        connect("postgres", "123456", jdbcUrl) {
            sql "ALTER TABLE cdc_test.pg_schema_change_ignore ADD COLUMN ignored_col TEXT"
            sql "INSERT INTO cdc_test.pg_schema_change_ignore VALUES (2, 20, 200, 'ignored')"
        }
        waitForRow(2)
        connect("postgres", "123456", jdbcUrl) {
            sql "ALTER TABLE cdc_test.pg_schema_change_ignore DROP COLUMN legacy"
            sql "INSERT INTO cdc_test.pg_schema_change_ignore VALUES (3, 30, 'ignored')"
        }
        waitForRow(3)
        connect("postgres", "123456", jdbcUrl) {
            // INT4 -> INT8 changes the native OID; small values remain writable to Doris INT.
            sql "ALTER TABLE cdc_test.pg_schema_change_ignore ALTER COLUMN value_col TYPE BIGINT"
            sql "INSERT INTO cdc_test.pg_schema_change_ignore VALUES (4, 40, 'ignored')"
        }
        waitForRow(4)

        // Rebuild the reader using the committed source baseline, while keeping ignore active.
        sql """PAUSE JOB where jobname='${jobName}'"""
        sql """RESUME JOB where jobname='${jobName}'"""
        waitForRunning()
        connect("postgres", "123456", jdbcUrl) {
            sql "INSERT INTO cdc_test.pg_schema_change_ignore VALUES (5, 50, 'after_rebuild')"
        }
        waitForRow(5)
        qt_ignore_rows "SELECT id, value_col, legacy FROM pg_schema_change_ignore ORDER BY id"
        qt_ignore_columns """SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.columns
            WHERE TABLE_SCHEMA='${currentDb}' AND TABLE_NAME='pg_schema_change_ignore' ORDER BY ORDINAL_POSITION"""

        sql """PAUSE JOB where jobname='${jobName}'"""
        sql """ALTER JOB ${jobName} FROM POSTGRES ("schema_change_behavior"="evolve")
            TO DATABASE ${currentDb}"""
        sql """RESUME JOB where jobname='${jobName}'"""
        waitForRunning()
        connect("postgres", "123456", jdbcUrl) {
            sql "ALTER TABLE cdc_test.pg_schema_change_ignore ADD COLUMN new_col INT"
            sql "INSERT INTO cdc_test.pg_schema_change_ignore VALUES (6, 60, 'still_ignored', 600)"
        }
        waitForRow(6)
        // Evolve handles only the new ADD; it must not replay ignored ADD/DROP/type changes.
        qt_evolve_rows "SELECT id, value_col, legacy, new_col FROM pg_schema_change_ignore ORDER BY id"
        qt_evolve_columns """SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.columns
            WHERE TABLE_SCHEMA='${currentDb}' AND TABLE_NAME='pg_schema_change_ignore' ORDER BY ORDINAL_POSITION"""
        qt_no_failures """select FailedTaskCount from jobs("type"="insert") where Name='${jobName}'"""
        sql """DROP JOB IF EXISTS where jobname='${jobName}'"""
    }
}
