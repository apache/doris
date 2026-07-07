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

suite("test_streaming_mysql_job_sc", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_sc"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "test_streaming_mysql_sc_t"
    def mysqlDb = "test_cdc_db"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """DROP TABLE IF EXISTS ${currentDb}.${table1} FORCE"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysqlPort = context.config.otherConfigs.get("mysql_57_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3Endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driverUrl = "https://${bucket}.${s3Endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

        def waitForColumn = { String column, boolean expected ->
            Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
                (sql "DESC ${table1}").any { it[0] == column } == expected
            })
        }
        def waitForRow = { String name ->
            Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
                (sql "SELECT COUNT(*) FROM ${table1} WHERE name='${name}'")[0][0] as int > 0
            })
        }
        def waitForRowGone = { String name ->
            Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
                (sql "SELECT COUNT(*) FROM ${table1} WHERE name='${name}'")[0][0] as int == 0
            })
        }
        def waitForValue = { String name, String column, Object expected ->
            Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
                def rows = sql "SELECT ${column} FROM ${table1} WHERE name='${name}'"
                rows.size() == 1 && String.valueOf(rows[0][0]) == String.valueOf(expected)
            })
        }
        def dumpJobState = {
            log.info("jobs  : " + sql("""SELECT * FROM jobs("type"="insert") WHERE Name='${jobName}'"""))
            log.info("tasks : " + sql("""SELECT * FROM tasks("type"="insert") WHERE JobName='${jobName}'"""))
        }

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysqlPort}") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table1}"""
            sql """CREATE TABLE ${mysqlDb}.${table1} (
                       name VARCHAR(200) NOT NULL,
                       age INT DEFAULT NULL,
                       PRIMARY KEY (name)
                   ) ENGINE=InnoDB"""
            sql """INSERT INTO ${mysqlDb}.${table1} (name, age) VALUES ('A1', 1)"""
            sql """INSERT INTO ${mysqlDb}.${table1} (name, age) VALUES ('B1', 2)"""
        }

        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM MYSQL (
                    "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysqlPort}",
                    "driver_url" = "${driverUrl}",
                    "driver_class" = "com.mysql.cj.jdbc.Driver",
                    "user" = "root",
                    "password" = "123456",
                    "database" = "${mysqlDb}",
                    "include_tables" = "${table1}",
                    "offset" = "initial"
                )
                TO DATABASE ${currentDb} (
                    "table.create.properties.replication_num" = "1"
                )"""

        try {
            Awaitility.await().atMost(300, SECONDS).pollInterval(1, SECONDS).until({
                def cnt = sql """SELECT SucceedTaskCount FROM jobs("type"="insert")
                                  WHERE Name='${jobName}' AND ExecuteType='STREAMING'"""
                cnt.size() == 1 && cnt[0][0] as int >= 2
            })
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        qt_snapshot """ SELECT name, age FROM ${table1} ORDER BY name """

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysqlPort}") {
            sql """ALTER TABLE ${mysqlDb}.${table1} ADD COLUMN c1 VARCHAR(50)"""
            sql """INSERT INTO ${mysqlDb}.${table1} (name, age, c1) VALUES ('C1', 10, 'hello')"""
        }

        try {
            waitForColumn("c1", true)
            waitForRow("C1")
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        assert (sql "SELECT c1 FROM ${table1} WHERE name='A1'")[0][0] == null
        assert (sql "SELECT c1 FROM ${table1} WHERE name='B1'")[0][0] == null
        qt_add_column """ SELECT name, age, c1 FROM ${table1} ORDER BY name """

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysqlPort}") {
            sql """UPDATE ${mysqlDb}.${table1} SET c1='world' WHERE name='C1'"""
            sql """UPDATE ${mysqlDb}.${table1} SET age=99, c1='updated' WHERE name='B1'"""
            sql """DELETE FROM ${mysqlDb}.${table1} WHERE name='A1'"""
        }

        try {
            waitForRowGone("A1")
            waitForValue("B1", "age", 99)
            waitForValue("C1", "c1", "world")
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        qt_add_column_dml """ SELECT name, age, c1 FROM ${table1} ORDER BY name """

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysqlPort}") {
            sql """ALTER TABLE ${mysqlDb}.${table1} DROP COLUMN c1"""
            sql """INSERT INTO ${mysqlDb}.${table1} (name, age) VALUES ('D1', 20)"""
        }

        try {
            waitForColumn("c1", false)
            waitForRow("D1")
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        qt_drop_column """ SELECT name, age FROM ${table1} ORDER BY name """

        assert (sql """SELECT Status FROM jobs("type"="insert") WHERE Name='${jobName}'""")[0][0] == "RUNNING"

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    }
}
