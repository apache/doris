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

suite("test_streaming_oceanbase_job_schema_change",
        "p2,external,oceanbase,external_docker,external_docker_oceanbase,nondatalake") {
    def jobName = "test_streaming_oceanbase_job_schema_change"
    def currentDb = (sql "SELECT DATABASE()")[0][0]
    def sourceDb = "test_oceanbase_streaming_db"
    def table1 = "oceanbase_streaming_schema_change"

    sql """DROP JOB IF EXISTS WHERE jobname='${jobName}'"""
    sql """DROP TABLE IF EXISTS ${currentDb}.${table1} FORCE"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String oceanbaseCdcPort = context.config.otherConfigs.get("oceanbase_cdc_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3Endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driverUrl =
                "https://${bucket}.${s3Endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

        def waitForColumn = { String column, boolean expected ->
            Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
                (sql "DESC ${currentDb}.${table1}").any { it[0] == column } == expected
            })
        }
        def waitForValue = { int id, String column, String expected ->
            Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
                def rows = sql "SELECT ${column} FROM ${currentDb}.${table1} WHERE id=${id}"
                rows.size() == 1 && String.valueOf(rows[0][0]) == expected
            })
        }
        def dumpJobState = {
            log.info("jobs: " + sql("""SELECT * FROM jobs("type"="insert") WHERE Name='${jobName}'"""))
            log.info("tasks: " + sql("""SELECT * FROM tasks("type"="insert") WHERE JobName='${jobName}'"""))
        }

        connect("root@test", "123456", "jdbc:mysql://${externalEnvIp}:${oceanbaseCdcPort}") {
            sql """CREATE DATABASE IF NOT EXISTS ${sourceDb}"""
            sql """DROP TABLE IF EXISTS ${sourceDb}.${table1}"""
            sql """CREATE TABLE ${sourceDb}.${table1} (
                        id INT NOT NULL,
                        name VARCHAR(100),
                        PRIMARY KEY (id)
                    ) ENGINE=InnoDB"""
            sql """INSERT INTO ${sourceDb}.${table1} VALUES (1, 'snapshot')"""
        }

        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM OCEANBASE (
                    "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${oceanbaseCdcPort}",
                    "driver_url" = "${driverUrl}",
                    "driver_class" = "com.mysql.cj.jdbc.Driver",
                    "user" = "root@test",
                    "password" = "123456",
                    "database" = "${sourceDb}",
                    "include_tables" = "${table1}",
                    "offset" = "initial"
                )
                TO DATABASE ${currentDb} (
                    "table.create.properties.replication_num" = "1"
                )"""

        try {
            waitForValue(1, "name", "snapshot")
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        connect("root@test", "123456", "jdbc:mysql://${externalEnvIp}:${oceanbaseCdcPort}") {
            sql """ALTER TABLE ${sourceDb}.${table1} ADD COLUMN city VARCHAR(50)"""
            sql """INSERT INTO ${sourceDb}.${table1} VALUES (2, 'after_add', 'Hangzhou')"""
        }

        try {
            waitForColumn("city", true)
            waitForValue(2, "city", "Hangzhou")
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        order_qt_oceanbase_after_add """
            SELECT id, name, city FROM ${currentDb}.${table1} ORDER BY id
        """

        connect("root@test", "123456", "jdbc:mysql://${externalEnvIp}:${oceanbaseCdcPort}") {
            sql """UPDATE ${sourceDb}.${table1} SET city='Shanghai' WHERE id=2"""
        }

        try {
            waitForValue(2, "city", "Shanghai")
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        connect("root@test", "123456", "jdbc:mysql://${externalEnvIp}:${oceanbaseCdcPort}") {
            sql """ALTER TABLE ${sourceDb}.${table1} DROP COLUMN city"""
            sql """INSERT INTO ${sourceDb}.${table1} VALUES (3, 'after_drop')"""
        }

        try {
            waitForColumn("city", false)
            waitForValue(3, "name", "after_drop")
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        order_qt_oceanbase_after_drop """
            SELECT id, name FROM ${currentDb}.${table1} ORDER BY id
        """

        def status = sql """SELECT Status FROM jobs("type"="insert") WHERE Name='${jobName}'"""
        assert status.size() == 1 && status[0][0] == "RUNNING"
        sql """DROP JOB IF EXISTS WHERE jobname='${jobName}'"""
    }
}
