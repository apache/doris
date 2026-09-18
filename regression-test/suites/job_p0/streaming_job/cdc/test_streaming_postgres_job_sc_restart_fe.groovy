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

import org.apache.doris.regression.suite.ClusterOptions
import org.awaitility.Awaitility

import static java.util.concurrent.TimeUnit.SECONDS

// Verifies that PostgreSQL schema baselines survive an FE restart, including an idempotent
// ADD-existing path, and that consecutive ADD/DROP changes continue from the restored baseline.
suite("test_streaming_postgres_job_sc_restart_fe",
        "docker,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_sc_restart_fe"
    def normalTable = "test_streaming_pg_sc_restart_normal"
    def idempotentTable = "test_streaming_pg_sc_restart_idempotent"
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"
    def options = new ClusterOptions()
    options.setFeNum(1)
    options.cloudMode = null

    docker(options) {
        def currentDb = (sql "select database()")[0][0]

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        sql """DROP TABLE IF EXISTS ${currentDb}.${normalTable} FORCE"""
        sql """DROP TABLE IF EXISTS ${currentDb}.${idempotentTable} FORCE"""

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
                Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                    def rows = sql """SELECT Status FROM jobs("type"="insert") WHERE Name='${jobName}'"""
                    rows.size() == 1 && rows[0][0] == "RUNNING"
                })
            }

            connect(pgUser, pgPassword, "jdbc:postgresql://${externalEnvIp}:${pgPort}/${pgDB}") {
                sql """DROP TABLE IF EXISTS ${pgSchema}.${normalTable}"""
                sql """DROP TABLE IF EXISTS ${pgSchema}.${idempotentTable}"""
                sql """CREATE TABLE ${pgSchema}.${normalTable} (
                            id INT PRIMARY KEY,
                            value VARCHAR(200)
                        )"""
                sql """CREATE TABLE ${pgSchema}.${idempotentTable} (
                            id INT PRIMARY KEY,
                            value VARCHAR(200)
                        )"""
                sql """INSERT INTO ${pgSchema}.${normalTable} VALUES (1, 'normal_snapshot')"""
                sql """INSERT INTO ${pgSchema}.${idempotentTable} VALUES (1, 'idempotent_snapshot')"""
            }

            // Pre-create the target-only column. A replayed ADD must treat the matching existing
            // column as success and still persist the new PostgreSQL schema baseline.
            sql """CREATE TABLE ${currentDb}.${idempotentTable} (
                        id INT NOT NULL,
                        value VARCHAR(65533) NULL,
                        c_existing VARCHAR(65533) NULL
                    ) UNIQUE KEY(id)
                    DISTRIBUTED BY HASH(id) BUCKETS 1
                    PROPERTIES (
                        "replication_num" = "1",
                        "enable_unique_key_merge_on_write" = "true"
                    )"""

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
                        "include_tables" = "${normalTable},${idempotentTable}",
                        "offset" = "initial"
                    )
                    TO DATABASE ${currentDb} (
                        "table.create.properties.replication_num" = "1"
                    )"""

            waitForValue(normalTable, 1, "value", "normal_snapshot")
            waitForValue(idempotentTable, 1, "value", "idempotent_snapshot")

            connect(pgUser, pgPassword, "jdbc:postgresql://${externalEnvIp}:${pgPort}/${pgDB}") {
                sql """ALTER TABLE ${pgSchema}.${normalTable} ADD COLUMN c1 VARCHAR(50)"""
                sql """INSERT INTO ${pgSchema}.${normalTable} VALUES (2, 'normal_before_restart', 'c1_value')"""
                sql """ALTER TABLE ${pgSchema}.${idempotentTable} ADD COLUMN c_existing VARCHAR(50)"""
                sql """INSERT INTO ${pgSchema}.${idempotentTable}
                        VALUES (2, 'idempotent_before_restart', 'existing_value')"""
            }

            waitForColumn(normalTable, "c1", true)
            waitForValue(normalTable, 2, "c1", "c1_value")
            waitForValue(idempotentTable, 2, "c_existing", "existing_value")

            cluster.restartFrontends()
            sleep(60000)
            context.reconnectFe()
            waitForJobRunning()

            // Both DROP operations require the pre-restart baselines to have been persisted. The
            // second one specifically verifies persistence after an idempotent ADD-existing result.
            connect(pgUser, pgPassword, "jdbc:postgresql://${externalEnvIp}:${pgPort}/${pgDB}") {
                sql """ALTER TABLE ${pgSchema}.${normalTable} DROP COLUMN c1"""
                sql """INSERT INTO ${pgSchema}.${normalTable} VALUES (3, 'normal_after_restart')"""
                sql """ALTER TABLE ${pgSchema}.${idempotentTable} DROP COLUMN c_existing"""
                sql """INSERT INTO ${pgSchema}.${idempotentTable} VALUES (3, 'idempotent_after_restart')"""
            }

            waitForColumn(normalTable, "c1", false)
            waitForColumn(idempotentTable, "c_existing", false)
            waitForValue(normalTable, 3, "value", "normal_after_restart")
            waitForValue(idempotentTable, 3, "value", "idempotent_after_restart")

            // Continue evolving the normal table to verify multiple Relation events use the latest
            // baseline after restart: ADD -> DROP -> ADD.
            connect(pgUser, pgPassword, "jdbc:postgresql://${externalEnvIp}:${pgPort}/${pgDB}") {
                sql """ALTER TABLE ${pgSchema}.${normalTable} ADD COLUMN c2 INT"""
                sql """INSERT INTO ${pgSchema}.${normalTable} VALUES (4, 'normal_c2', 42)"""
            }
            waitForColumn(normalTable, "c2", true)
            waitForValue(normalTable, 4, "c2", "42")

            connect(pgUser, pgPassword, "jdbc:postgresql://${externalEnvIp}:${pgPort}/${pgDB}") {
                sql """ALTER TABLE ${pgSchema}.${normalTable} DROP COLUMN c2"""
                sql """INSERT INTO ${pgSchema}.${normalTable} VALUES (5, 'normal_without_c2')"""
            }
            waitForColumn(normalTable, "c2", false)
            waitForValue(normalTable, 5, "value", "normal_without_c2")

            connect(pgUser, pgPassword, "jdbc:postgresql://${externalEnvIp}:${pgPort}/${pgDB}") {
                sql """ALTER TABLE ${pgSchema}.${normalTable} ADD COLUMN c3 VARCHAR(50)"""
                sql """INSERT INTO ${pgSchema}.${normalTable} VALUES (6, 'normal_c3', 'final_value')"""
            }
            waitForColumn(normalTable, "c3", true)
            waitForValue(normalTable, 6, "c3", "final_value")

            order_qt_normal_final "SELECT id, value, c3 FROM ${normalTable}"
            order_qt_idempotent_final "SELECT id, value FROM ${idempotentTable}"
            assert (sql """SELECT Status FROM jobs("type"="insert") WHERE Name='${jobName}'""")[0][0] == "RUNNING"

            sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        }
    }
}
