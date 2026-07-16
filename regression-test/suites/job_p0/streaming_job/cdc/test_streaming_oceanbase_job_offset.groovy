// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import org.awaitility.Awaitility

import static java.util.concurrent.TimeUnit.SECONDS

suite("test_streaming_oceanbase_job_offset",
        "p2,external,oceanbase,external_docker,external_docker_oceanbase,nondatalake") {
    def currentDb = (sql "SELECT DATABASE()")[0][0]
    def sourceDb = "test_oceanbase_streaming_db"
    def suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 8)
    def latestJob = "test_streaming_oceanbase_offset_latest"
    def earliestJob = "test_streaming_oceanbase_offset_earliest"
    def specificJob = "test_streaming_oceanbase_offset_specific"
    def latestTable = "oceanbase_offset_latest_${suffix}"
    def earliestTable = "oceanbase_offset_earliest_${suffix}"
    def specificTable = "oceanbase_offset_specific_${suffix}"

    [latestJob, earliestJob, specificJob].each {
        sql """DROP JOB IF EXISTS WHERE jobname='${it}'"""
    }
    [latestTable, earliestTable, specificTable].each {
        sql """DROP TABLE IF EXISTS ${currentDb}.${it} FORCE"""
    }

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String oceanbaseCdcPort = context.config.otherConfigs.get("oceanbase_cdc_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3Endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driverUrl =
                "https://${bucket}.${s3Endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"
        String sourceUrl = "jdbc:mysql://${externalEnvIp}:${oceanbaseCdcPort}"

        def waitForJobReady = { String jobName ->
            Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
                def rows = sql """SELECT Status, SucceedTaskCount FROM jobs("type"="insert")
                                   WHERE Name='${jobName}'"""
                rows.size() == 1 && rows[0][0] == "RUNNING" && (rows[0][1] as int) >= 1
            })
        }
        def waitForRows = { String tableName, int count ->
            Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                def rows = sql """SELECT COUNT(*) FROM ${currentDb}.${tableName}"""
                rows.size() == 1 && rows[0][0] == count
            })
        }
        def dumpJobState = { String jobName ->
            log.info("jobs: " + sql("""SELECT * FROM jobs("type"="insert") WHERE Name='${jobName}'"""))
            log.info("tasks: " + sql("""SELECT * FROM tasks("type"="insert") WHERE JobName='${jobName}'"""))
        }

        connect("root@test", "123456", sourceUrl) {
            sql """CREATE DATABASE IF NOT EXISTS ${sourceDb}"""
            sql """DROP TABLE IF EXISTS ${sourceDb}.${latestTable}"""
            sql """CREATE TABLE ${sourceDb}.${latestTable} (
                        id INT NOT NULL,
                        name VARCHAR(100),
                        PRIMARY KEY (id)
                    ) ENGINE=InnoDB"""
            sql """INSERT INTO ${sourceDb}.${latestTable} VALUES (1, 'before_latest')"""
        }

        sql """CREATE JOB ${latestJob}
                ON STREAMING
                FROM OCEANBASE (
                    "jdbc_url" = "${sourceUrl}",
                    "driver_url" = "${driverUrl}",
                    "driver_class" = "com.mysql.cj.jdbc.Driver",
                    "user" = "root@test",
                    "password" = "123456",
                    "database" = "${sourceDb}",
                    "include_tables" = "${latestTable}",
                    "offset" = "latest"
                )
                TO DATABASE ${currentDb} (
                    "table.create.properties.replication_num" = "1"
                )"""

        try {
            waitForJobReady(latestJob)
            connect("root@test", "123456", sourceUrl) {
                sql """INSERT INTO ${sourceDb}.${latestTable} VALUES (2, 'after_latest')"""
            }
            waitForRows(latestTable, 1)
        } catch (Exception ex) {
            dumpJobState(latestJob)
            throw ex
        }

        order_qt_oceanbase_offset_latest """
            SELECT id, name FROM ${currentDb}.${latestTable} ORDER BY id
        """
        sql """DROP JOB IF EXISTS WHERE jobname='${latestJob}'"""

        connect("root@test", "123456", sourceUrl) {
            sql """DROP TABLE IF EXISTS ${sourceDb}.${earliestTable}"""
            sql """CREATE TABLE ${sourceDb}.${earliestTable} (
                        id INT NOT NULL,
                        name VARCHAR(100),
                        PRIMARY KEY (id)
                    ) ENGINE=InnoDB"""
            sql """INSERT INTO ${sourceDb}.${earliestTable} VALUES
                        (1, 'earliest_one'),
                        (2, 'earliest_two')"""
        }

        sql """CREATE JOB ${earliestJob}
                ON STREAMING
                FROM OCEANBASE (
                    "jdbc_url" = "${sourceUrl}",
                    "driver_url" = "${driverUrl}",
                    "driver_class" = "com.mysql.cj.jdbc.Driver",
                    "user" = "root@test",
                    "password" = "123456",
                    "database" = "${sourceDb}",
                    "include_tables" = "${earliestTable}",
                    "offset" = "earliest"
                )
                TO DATABASE ${currentDb} (
                    "table.create.properties.replication_num" = "1"
                )"""

        try {
            waitForRows(earliestTable, 2)
        } catch (Exception ex) {
            dumpJobState(earliestJob)
            throw ex
        }

        order_qt_oceanbase_offset_earliest """
            SELECT id, name FROM ${currentDb}.${earliestTable} ORDER BY id
        """
        sql """DROP JOB IF EXISTS WHERE jobname='${earliestJob}'"""

        def binlogFile = ""
        def binlogPosition = ""
        connect("root@test", "123456", sourceUrl) {
            sql """DROP TABLE IF EXISTS ${sourceDb}.${specificTable}"""
            sql """CREATE TABLE ${sourceDb}.${specificTable} (
                        id INT NOT NULL,
                        name VARCHAR(100),
                        PRIMARY KEY (id)
                    ) ENGINE=InnoDB"""
            def masterStatus = sql """SHOW MASTER STATUS"""
            binlogFile = masterStatus[0][0]
            binlogPosition = masterStatus[0][1].toString()
            sql """INSERT INTO ${sourceDb}.${specificTable} VALUES
                        (10, 'specific_one'),
                        (11, 'specific_two')"""
        }
        def offsetJson = """{"file":"${binlogFile}","pos":"${binlogPosition}"}"""

        sql """CREATE JOB ${specificJob}
                ON STREAMING
                FROM OCEANBASE (
                    "jdbc_url" = "${sourceUrl}",
                    "driver_url" = "${driverUrl}",
                    "driver_class" = "com.mysql.cj.jdbc.Driver",
                    "user" = "root@test",
                    "password" = "123456",
                    "database" = "${sourceDb}",
                    "include_tables" = "${specificTable}",
                    "offset" = '${offsetJson}'
                )
                TO DATABASE ${currentDb} (
                    "table.create.properties.replication_num" = "1"
                )"""

        try {
            waitForRows(specificTable, 2)
        } catch (Exception ex) {
            dumpJobState(specificJob)
            throw ex
        }

        order_qt_oceanbase_offset_specific """
            SELECT id, name FROM ${currentDb}.${specificTable} ORDER BY id
        """
        sql """DROP JOB IF EXISTS WHERE jobname='${specificJob}'"""
    }
}
