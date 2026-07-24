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

suite("test_streaming_mysql_job_sc_restart_fe", "docker,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_sc_restart_fe"
    def table1 = "test_streaming_mysql_sc_restart_t"
    def mysqlDb = "test_cdc_db"
    def options = new ClusterOptions()
    options.setFeNum(1)
    options.cloudMode = null

    docker(options) {
        def currentDb = (sql "select database()")[0][0]

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
            def waitForValue = { int id, String column, String expected ->
                Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
                    def rows = sql "SELECT ${column} FROM ${table1} WHERE id=${id}"
                    rows.size() == 1 && String.valueOf(rows[0][0]) == expected
                })
            }
            def waitForJobRunning = {
                Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                    def rows = sql """SELECT Status FROM jobs("type"="insert") WHERE Name='${jobName}'"""
                    rows.size() == 1 && rows[0][0] == "RUNNING"
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
                           id INT NOT NULL,
                           value VARCHAR(100) DEFAULT NULL,
                           PRIMARY KEY (id)
                       ) ENGINE=InnoDB"""
                sql """INSERT INTO ${mysqlDb}.${table1} VALUES (1, 'snapshot')"""
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
                waitForValue(1, "value", "snapshot")
            } catch (Exception ex) {
                dumpJobState()
                throw ex
            }

            connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysqlPort}") {
                sql """ALTER TABLE ${mysqlDb}.${table1} ADD COLUMN c1 VARCHAR(50)"""
                sql """INSERT INTO ${mysqlDb}.${table1} VALUES (2, 'before_restart', 'c1_value')"""
            }

            try {
                waitForColumn("c1", true)
                waitForValue(2, "c1", "c1_value")
            } catch (Exception ex) {
                dumpJobState()
                throw ex
            }

            cluster.restartFrontends()
            sleep(60000)
            context.reconnectFe()
            waitForJobRunning()

            connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysqlPort}") {
                sql """INSERT INTO ${mysqlDb}.${table1} VALUES (3, 'after_restart', 'after_restart_c1')"""
                sql """UPDATE ${mysqlDb}.${table1} SET c1='updated_after_restart' WHERE id=2"""
            }

            try {
                waitForValue(3, "c1", "after_restart_c1")
                waitForValue(2, "c1", "updated_after_restart")
            } catch (Exception ex) {
                dumpJobState()
                throw ex
            }

            order_qt_after_restart_dml """ SELECT id, value, c1 FROM ${table1} ORDER BY id """

            connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysqlPort}") {
                sql """ALTER TABLE ${mysqlDb}.${table1} DROP COLUMN c1"""
                sql """INSERT INTO ${mysqlDb}.${table1} VALUES (4, 'after_drop')"""
            }

            try {
                waitForColumn("c1", false)
                waitForValue(4, "value", "after_drop")
            } catch (Exception ex) {
                dumpJobState()
                throw ex
            }

            order_qt_final """ SELECT id, value FROM ${table1} ORDER BY id """
            assert (sql """SELECT Status FROM jobs("type"="insert") WHERE Name='${jobName}'""")[0][0] == "RUNNING"

            sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        }
    }
}
