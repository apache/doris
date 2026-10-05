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

// Verify ALTER JOB on source credentials reaches JdbcSourceOffsetProvider at runtime
// (no FE restart). Without the fix, provider keeps a stale copy seeded at init time
// and would continue connecting upstream with the original password.
suite("test_streaming_mysql_job_alter_cred",
        "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_alter_cred"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "alter_cred_user_info"
    def mysqlDb = "test_cdc_db"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table1}"""
            sql """CREATE TABLE ${mysqlDb}.${table1} (
                    `name` varchar(200) NOT NULL,
                    `age` int DEFAULT NULL,
                    PRIMARY KEY (`name`)
                ) ENGINE=InnoDB"""
            sql """INSERT INTO ${mysqlDb}.${table1} (name, age) VALUES ('A1', 1)"""
            sql """INSERT INTO ${mysqlDb}.${table1} (name, age) VALUES ('B1', 2)"""
        }

        // Create with correct credentials.
        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM MYSQL (
                    "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "com.mysql.cj.jdbc.Driver",
                    "user" = "root",
                    "password" = "123456",
                    "database" = "${mysqlDb}",
                    "include_tables" = "${table1}",
                    "offset" = "initial"
                )
                TO DATABASE ${currentDb} (
                    "table.create.properties.replication_num" = "1"
                )
            """

        try {
            Awaitility.await().atMost(300, SECONDS).pollInterval(1, SECONDS).until({
                def count = sql """SELECT count(1) FROM ${currentDb}.${table1}"""
                log.info("initial row count: " + count)
                (count.get(0).get(0) as int) == 2
            })
        } catch (Exception ex) {
            log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            log.info("tasks: " + (sql """select * from tasks("type"="insert") where JobName='${jobName}'"""))
            throw ex
        }

        sql """PAUSE JOB where jobname = '${jobName}'"""
        Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).until({
            def s = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
            s.size() == 1 && s.get(0).get(0) == "PAUSED"
        })

        // ALTER to a wrong password. If the provider still holds the original credentials
        // (Bug A), resume will succeed; with the fix, provider picks up the bad password
        // and fetchMeta / task RPC fail, auto-pausing the job.
        sql """ALTER JOB ${jobName}
                FROM MYSQL (
                    "password" = "wrong_password_for_test"
                )
                TO DATABASE ${currentDb}
            """

        sql """RESUME JOB where jobname = '${jobName}'"""

        // Auto-resume cycles PAUSED→PENDING→RUNNING→PAUSED, so the PAUSED state is transient.
        // FailureReason is only cleared by onStreamTaskSuccess, so ErrorMsg is sticky across cycles.
        try {
            Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                def r = sql """select ErrorMsg from jobs("type"="insert") where Name='${jobName}'"""
                String msg = r.get(0).get(0)
                log.info("ErrorMsg after wrong cred resume: " + msg)
                return msg != null && !msg.isEmpty()
            })
        } catch (Exception ex) {
            log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            throw ex
        }

        // ALTER back to the correct password and verify recovery.
        sql """ALTER JOB ${jobName}
                FROM MYSQL (
                    "password" = "123456"
                )
                TO DATABASE ${currentDb}
            """

        sql """RESUME JOB where jobname = '${jobName}'"""

        try {
            Awaitility.await().atMost(60, SECONDS).pollInterval(1, SECONDS).until({
                def s = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
                log.info("status after correct cred resume: " + s)
                s.size() == 1 && s.get(0).get(0) == "RUNNING"
            })
        } catch (Exception ex) {
            log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            throw ex
        }

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """INSERT INTO ${mysqlDb}.${table1} (name, age) VALUES ('C1', 3)"""
        }

        try {
            Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                def count = sql """SELECT count(1) FROM ${currentDb}.${table1}"""
                log.info("row count after recovery: " + count)
                (count.get(0).get(0) as int) == 3
            })
        } catch (Exception ex) {
            log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            log.info("tasks: " + (sql """select * from tasks("type"="insert") where JobName='${jobName}'"""))
            throw ex
        }

        qt_final_data """SELECT * FROM ${currentDb}.${table1} ORDER BY name"""

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        sql """drop table if exists ${currentDb}.${table1} force"""
    }
}
