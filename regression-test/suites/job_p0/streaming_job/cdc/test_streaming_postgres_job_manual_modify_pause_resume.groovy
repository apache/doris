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

suite("test_streaming_postgres_job_manual_modify_pause_resume",
        "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_manual_modify_pause_resume"
    def currentDb = (sql "select database()")[0][0]
    def tableName = "pg_manual_modify_pause_resume"
    def pgDb = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """DROP TABLE IF EXISTS ${currentDb}.${tableName} FORCE"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pgPort = context.config.otherConfigs.get("pg_14_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3Endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driverUrl =
                "https://${bucket}.${s3Endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        def dumpJobState = {
            log.info("jobs: " + sql("""select * from jobs("type"="insert") where Name='${jobName}'"""))
            log.info("tasks: " + sql("""select * from tasks("type"="insert") where JobName='${jobName}'"""))
        }

        def waitForRow = { String id ->
            Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
                def rows = sql """SELECT COUNT(*) FROM ${tableName} WHERE id='${id}'"""
                rows[0][0] as int == 1
            })
        }

        connect("${pgUser}", "${pgPassword}",
                "jdbc:postgresql://${externalEnvIp}:${pgPort}/${pgDb}") {
            sql """DROP TABLE IF EXISTS ${pgSchema}.${tableName}"""
            sql """CREATE TABLE ${pgSchema}.${tableName} (
                       id VARCHAR(32) PRIMARY KEY,
                       value_col INT4
                   )"""
            sql """INSERT INTO ${pgSchema}.${tableName} VALUES ('before_modify', 100)"""
        }

        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM POSTGRES (
                    "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pgPort}/${pgDb}",
                    "driver_url" = "${driverUrl}",
                    "driver_class" = "org.postgresql.Driver",
                    "user" = "${pgUser}",
                    "password" = "${pgPassword}",
                    "database" = "${pgDb}",
                    "schema" = "${pgSchema}",
                    "include_tables" = "${tableName}",
                    "offset" = "initial"
                )
                TO DATABASE ${currentDb} (
                    "table.create.properties.replication_num" = "1"
                )"""

        try {
            waitForRow("before_modify")
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        sql """PAUSE JOB where jobname = '${jobName}'"""
        Awaitility.await().atMost(60, SECONDS).pollInterval(1, SECONDS).until({
            def status = sql """select Status from jobs("type"="insert") where Name='${jobName}'"""
            status.size() == 1 && status[0][0] == "PAUSED"
        })
        Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
            def running = sql """select count(*) from tasks("type"="insert")
                                  where JobName='${jobName}' and Status='RUNNING'"""
            running[0][0] as int == 0
        })

        connect("${pgUser}", "${pgPassword}",
                "jdbc:postgresql://${externalEnvIp}:${pgPort}/${pgDb}") {
            sql """ALTER TABLE ${pgSchema}.${tableName} ALTER COLUMN value_col TYPE INT8"""
        }
        sql """ALTER TABLE ${tableName} MODIFY COLUMN value_col BIGINT NULL"""
        Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
            def desc = sql """DESC ${tableName}"""
            desc.find { it[0] == "value_col" }[1] == "bigint"
        })

        sql """RESUME JOB where jobname = '${jobName}'"""
        Awaitility.await().atMost(60, SECONDS).pollInterval(1, SECONDS).until({
            def status = sql """select Status from jobs("type"="insert") where Name='${jobName}'"""
            status.size() == 1 && status[0][0] == "RUNNING"
        })

        connect("${pgUser}", "${pgPassword}",
                "jdbc:postgresql://${externalEnvIp}:${pgPort}/${pgDb}") {
            sql """INSERT INTO ${pgSchema}.${tableName}
                    VALUES ('after_modify', 3000000000)"""
        }
        try {
            waitForRow("after_modify")
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }
        order_qt_modify_resume """SELECT id, value_col FROM ${tableName}"""

        // Verify that a supported schema change still works after the manual MODIFY workflow.
        connect("${pgUser}", "${pgPassword}",
                "jdbc:postgresql://${externalEnvIp}:${pgPort}/${pgDb}") {
            sql """ALTER TABLE ${pgSchema}.${tableName} ADD COLUMN note VARCHAR(64)"""
            sql """INSERT INTO ${pgSchema}.${tableName}
                    VALUES ('after_add', 4000000000, 'baseline_updated')"""
        }
        Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
            def desc = sql """DESC ${tableName}"""
            desc.any { it[0] == "note" }
        })
        try {
            waitForRow("after_add")
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }
        order_qt_add_after_resume """SELECT id, value_col, note FROM ${tableName}"""

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    }
}
