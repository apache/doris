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

// Verify that schema-change detection works with offset=latest (no snapshot, no JDBC baseline).
// The schema baseline is established solely from the first pgoutput Relation event; subsequent
// ADD/DROP are detected by diffing fresh Relation events against that baseline.
suite("test_streaming_postgres_job_sc_latest",
        "p0,external,pg,external_docker,external_docker_pg,nondatalake") {

    def jobName = "test_streaming_pg_sc_latest"
    def table1 = "test_streaming_pg_sc_latest_t"
    def currentDb = (sql "select database()")[0][0]
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """DROP TABLE IF EXISTS ${currentDb}.${table1} FORCE"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pgPort = context.config.otherConfigs.get("pg_14_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3Endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driverUrl = "https://${bucket}.${s3Endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        def waitForColumn = { String table, String column, boolean expected ->
            Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
                (sql "DESC ${table}").any { it[0] == column } == expected
            })
        }
        def waitForValue = { String table, int id, String column, String expected ->
            Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
                def rows = sql "SELECT ${column} FROM ${table} WHERE id=${id}"
                rows.size() == 1 && String.valueOf(rows[0][0]) == expected
            })
        }
        def waitForJobRunning = {
            Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
                def rows = sql """SELECT Status FROM jobs("type"="insert") WHERE Name='${jobName}'"""
                rows.size() == 1 && rows[0][0] == "RUNNING"
            })
        }

        // Phase 0: pre-create the PG table with data that must NOT be synced.
        connect(pgUser, pgPassword, "jdbc:postgresql://${externalEnvIp}:${pgPort}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgSchema}.${table1}"""
            sql """CREATE TABLE ${pgSchema}.${table1} (
                        id INT PRIMARY KEY,
                        value VARCHAR(200)
                    )"""
            sql """INSERT INTO ${pgSchema}.${table1} VALUES (1, 'before_latest')"""
        }

        // Phase 1: start job with offset=latest — no snapshot, no JDBC baseline.
        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM POSTGRES (
                    "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pgPort}/${pgDB}",
                    "driver_url" = "${driverUrl}",
                    "driver_class" = "org.postgresql.Driver",
                    "user" = "${pgUser}",
                    "password" = "${pgPassword}",
                    "database" = "${pgDB}",
                    "schema" = "${pgSchema}",
                    "include_tables" = "${table1}",
                    "offset" = "latest"
                )
                TO DATABASE ${currentDb} (
                    "table.create.properties.replication_num" = "1"
                )"""

        waitForJobRunning()

        // Confirm that pre-existing data was not synced.
        def preExisting = sql "SELECT COUNT(*) FROM ${table1} WHERE id=1"
        assert preExisting[0][0] as int == 0 : "pre-latest data must not be synced"

        // Phase 2: ADD column + INSERT. The first Relation establishes the baseline (no DDL);
        // the ADD Relation is diffed against it and emits the ADD DDL.
        connect(pgUser, pgPassword, "jdbc:postgresql://${externalEnvIp}:${pgPort}/${pgDB}") {
            sql """ALTER TABLE ${pgSchema}.${table1} ADD COLUMN extra VARCHAR(50)"""
            sql """INSERT INTO ${pgSchema}.${table1} VALUES (2, 'after_add', 'extra_val')"""
        }

        waitForColumn(table1, "extra", true)
        waitForValue(table1, 2, "extra", "extra_val")

        // Phase 3: DROP column + INSERT. The DROP Relation removes the column from Doris.
        connect(pgUser, pgPassword, "jdbc:postgresql://${externalEnvIp}:${pgPort}/${pgDB}") {
            sql """ALTER TABLE ${pgSchema}.${table1} DROP COLUMN extra"""
            sql """INSERT INTO ${pgSchema}.${table1} VALUES (3, 'after_drop')"""
        }

        waitForColumn(table1, "extra", false)
        waitForValue(table1, 3, "value", "after_drop")

        // Phase 4: ADD again (different column) to verify repeated ADD after DROP.
        connect(pgUser, pgPassword, "jdbc:postgresql://${externalEnvIp}:${pgPort}/${pgDB}") {
            sql """ALTER TABLE ${pgSchema}.${table1} ADD COLUMN score INT"""
            sql """INSERT INTO ${pgSchema}.${table1} VALUES (4, 'with_score', 99)"""
        }

        waitForColumn(table1, "score", true)
        waitForValue(table1, 4, "score", "99")

        order_qt_final """ SELECT id, value, score FROM ${table1} ORDER BY id """

        assert (sql """SELECT Status FROM jobs("type"="insert") WHERE Name='${jobName}'""")[0][0] == "RUNNING"

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    }
}
