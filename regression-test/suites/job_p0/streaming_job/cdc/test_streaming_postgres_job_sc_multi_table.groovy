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

suite("test_streaming_postgres_job_sc_multi_table",
        "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_sc_multi_table"
    def tableA = "test_streaming_pg_sc_multi_a"
    def tableB = "test_streaming_pg_sc_multi_b"
    def currentDb = (sql "select database()")[0][0]
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """DROP TABLE IF EXISTS ${currentDb}.${tableA} FORCE"""
    sql """DROP TABLE IF EXISTS ${currentDb}.${tableB} FORCE"""

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

        connect(pgUser, pgPassword, "jdbc:postgresql://${externalEnvIp}:${pgPort}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgSchema}.${tableA}"""
            sql """DROP TABLE IF EXISTS ${pgSchema}.${tableB}"""
            sql """CREATE TABLE ${pgSchema}.${tableA} (
                        id INT PRIMARY KEY,
                        value VARCHAR(200)
                    )"""
            sql """CREATE TABLE ${pgSchema}.${tableB} (
                        id INT PRIMARY KEY,
                        value VARCHAR(200)
                    )"""
            sql """INSERT INTO ${pgSchema}.${tableA} VALUES (1, 'a_snapshot')"""
            sql """INSERT INTO ${pgSchema}.${tableB} VALUES (1, 'b_snapshot')"""
        }

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
                    "include_tables" = "${tableA},${tableB}",
                    "offset" = "initial"
                )
                TO DATABASE ${currentDb} (
                    "table.create.properties.replication_num" = "1"
                )"""

        waitForValue(tableA, 1, "value", "a_snapshot")
        waitForValue(tableB, 1, "value", "b_snapshot")

        // Interleave independent ADD events for both tables.
        connect(pgUser, pgPassword, "jdbc:postgresql://${externalEnvIp}:${pgPort}/${pgDB}") {
            sql """ALTER TABLE ${pgSchema}.${tableA} ADD COLUMN a_extra VARCHAR(50)"""
            sql """INSERT INTO ${pgSchema}.${tableA} VALUES (2, 'a_with_extra', 'a_extra_value')"""
            sql """ALTER TABLE ${pgSchema}.${tableB} ADD COLUMN b_extra INT"""
            sql """INSERT INTO ${pgSchema}.${tableB} VALUES (2, 'b_with_extra', 22)"""
        }

        waitForColumn(tableA, "a_extra", true)
        waitForColumn(tableB, "b_extra", true)
        waitForValue(tableA, 2, "a_extra", "a_extra_value")
        waitForValue(tableB, 2, "b_extra", "22")
        order_qt_b_before_drop "SELECT id, value, b_extra FROM ${tableB}"

        // Dropping A's column must not overwrite B's baseline; B continues writing b_extra.
        connect(pgUser, pgPassword, "jdbc:postgresql://${externalEnvIp}:${pgPort}/${pgDB}") {
            sql """ALTER TABLE ${pgSchema}.${tableA} DROP COLUMN a_extra"""
            sql """INSERT INTO ${pgSchema}.${tableA} VALUES (3, 'a_after_drop')"""
            sql """INSERT INTO ${pgSchema}.${tableB} VALUES (3, 'b_still_extra', 33)"""
        }

        waitForColumn(tableA, "a_extra", false)
        waitForValue(tableA, 3, "value", "a_after_drop")
        waitForValue(tableB, 3, "b_extra", "33")

        // Evolve both tables again in opposite directions. Their final schemas must reflect only
        // their own latest Relation events.
        connect(pgUser, pgPassword, "jdbc:postgresql://${externalEnvIp}:${pgPort}/${pgDB}") {
            sql """ALTER TABLE ${pgSchema}.${tableA} ADD COLUMN a_next INT"""
            sql """INSERT INTO ${pgSchema}.${tableA} VALUES (4, 'a_with_next', 44)"""
            sql """ALTER TABLE ${pgSchema}.${tableB} DROP COLUMN b_extra"""
            sql """INSERT INTO ${pgSchema}.${tableB} VALUES (4, 'b_after_drop')"""
        }

        waitForColumn(tableA, "a_next", true)
        waitForColumn(tableB, "b_extra", false)
        waitForValue(tableA, 4, "a_next", "44")
        waitForValue(tableB, 4, "value", "b_after_drop")

        order_qt_table_a_final "SELECT id, value, a_next FROM ${tableA}"
        order_qt_table_b_final "SELECT id, value FROM ${tableB}"
        assert (sql """SELECT Status FROM jobs("type"="insert") WHERE Name='${jobName}'""")[0][0] == "RUNNING"

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    }
}
