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

suite("test_streaming_postgres_job_non_column_ddl",
        "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def jobName = "test_streaming_pg_non_column_ddl"
        def currentDb = (sql "select database()")[0][0]
        String pgPort = context.config.otherConfigs.get("pg_14_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String jdbcUrl = "jdbc:postgresql://${externalEnvIp}:${pgPort}/postgres"
        String driverUrl = "https://${getS3BucketName()}.${getS3Endpoint()}/regression/jdbc_driver/postgresql-42.5.0.jar"

        sql """DROP JOB IF EXISTS where jobname='${jobName}'"""
        sql "DROP TABLE IF EXISTS pg_non_column_ddl FORCE"
        sql "DROP TABLE IF EXISTS pg_unselected_ddl FORCE"
        connect("postgres", "123456", jdbcUrl) {
            sql "DROP TABLE IF EXISTS cdc_test.pg_non_column_ddl"
            sql "DROP TABLE IF EXISTS cdc_test.pg_unselected_ddl"
            sql "CREATE TABLE cdc_test.pg_non_column_ddl (id INT PRIMARY KEY, value_col INT)"
            sql "INSERT INTO cdc_test.pg_non_column_ddl VALUES (1, 10), (2, 20)"
        }

        long committedCount = 0L
        def waitForValue = { int id, int value ->
            Awaitility.await().atMost(180, SECONDS).pollInterval(1, SECONDS).until({
                def rows = sql "SELECT value_col FROM pg_non_column_ddl WHERE id=${id}"
                rows.size() == 1 && (rows[0][0] as int) == value
            })
            Awaitility.await().atMost(180, SECONDS).pollInterval(1, SECONDS).until({
                (sql """SELECT SucceedTaskCount FROM jobs("type"="insert")
                    WHERE Name='${jobName}'""")[0][0] as long > committedCount
            })
            committedCount = (sql """SELECT SucceedTaskCount FROM jobs("type"="insert")
                WHERE Name='${jobName}'""")[0][0] as long
        }

        // Default evolve must not treat table metadata or unrelated-table DDL as a column change.
        sql """CREATE JOB ${jobName} ON STREAMING
            FROM POSTGRES (
                "jdbc_url"="${jdbcUrl}", "driver_url"="${driverUrl}",
                "driver_class"="org.postgresql.Driver", "user"="postgres", "password"="123456",
                "database"="postgres", "schema"="cdc_test", "include_tables"="pg_non_column_ddl",
                "offset"="initial"
            ) TO DATABASE ${currentDb} ("table.create.properties.replication_num"="1")"""
        waitForValue(2, 20)

        connect("postgres", "123456", jdbcUrl) {
            sql "CREATE INDEX pg_non_column_value_idx ON cdc_test.pg_non_column_ddl (value_col)"
            sql "INSERT INTO cdc_test.pg_non_column_ddl VALUES (3, 30)"
        }
        waitForValue(3, 30)
        qt_ignored_index "SHOW INDEX FROM pg_non_column_ddl"
        connect("postgres", "123456", jdbcUrl) {
            sql "DROP INDEX cdc_test.pg_non_column_value_idx"
            sql "INSERT INTO cdc_test.pg_non_column_ddl VALUES (4, 40)"
        }
        waitForValue(4, 40)
        connect("postgres", "123456", jdbcUrl) {
            sql "COMMENT ON TABLE cdc_test.pg_non_column_ddl IS 'source-only comment'"
            sql "INSERT INTO cdc_test.pg_non_column_ddl VALUES (5, 50)"
        }
        waitForValue(5, 50)
        qt_metadata_ddl "SELECT id, value_col FROM pg_non_column_ddl ORDER BY id"

        connect("postgres", "123456", jdbcUrl) {
            sql "TRUNCATE TABLE cdc_test.pg_non_column_ddl"
            sql "INSERT INTO cdc_test.pg_non_column_ddl VALUES (6, 60)"
        }
        waitForValue(6, 60)
        // Source TRUNCATE does not delete historical target rows.
        qt_truncate_new_key "SELECT id, value_col FROM pg_non_column_ddl ORDER BY id"
        connect("postgres", "123456", jdbcUrl) {
            sql "INSERT INTO cdc_test.pg_non_column_ddl VALUES (1, 100)"
        }
        waitForValue(1, 100)
        qt_truncate_reused_key "SELECT id, value_col FROM pg_non_column_ddl ORDER BY id"

        connect("postgres", "123456", jdbcUrl) {
            sql "CREATE TABLE cdc_test.pg_unselected_ddl (id INT PRIMARY KEY, other_value INT)"
            sql "INSERT INTO cdc_test.pg_unselected_ddl VALUES (999, 999)"
            sql "INSERT INTO cdc_test.pg_non_column_ddl VALUES (7, 70)"
        }
        waitForValue(7, 70)
        connect("postgres", "123456", jdbcUrl) {
            sql "ALTER TABLE cdc_test.pg_unselected_ddl ADD COLUMN other_note TEXT"
            sql "INSERT INTO cdc_test.pg_unselected_ddl VALUES (1000, 1000, 'unselected')"
            sql "INSERT INTO cdc_test.pg_non_column_ddl VALUES (8, 80)"
        }
        waitForValue(8, 80)
        qt_unselected_table """SELECT COUNT(*) FROM information_schema.tables
            WHERE TABLE_SCHEMA='${currentDb}' AND TABLE_NAME='pg_unselected_ddl'"""

        sql """PAUSE JOB where jobname='${jobName}'"""
        sql """RESUME JOB where jobname='${jobName}'"""
        connect("postgres", "123456", jdbcUrl) {
            sql "ALTER TABLE cdc_test.pg_non_column_ddl ADD COLUMN note TEXT"
            sql "INSERT INTO cdc_test.pg_non_column_ddl VALUES (9, 90, 'after_resume')"
        }
        waitForValue(9, 90)
        qt_after_resume "SELECT id, value_col, note FROM pg_non_column_ddl ORDER BY id"
        qt_columns """SELECT COLUMN_NAME FROM information_schema.columns
            WHERE TABLE_SCHEMA='${currentDb}' AND TABLE_NAME='pg_non_column_ddl' ORDER BY ORDINAL_POSITION"""
        qt_no_failures """select FailedTaskCount from jobs("type"="insert") where Name='${jobName}'"""
        sql """DROP JOB IF EXISTS where jobname='${jobName}'"""
    }
}
