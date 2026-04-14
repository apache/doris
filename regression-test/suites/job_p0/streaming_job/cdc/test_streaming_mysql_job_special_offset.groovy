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

suite("test_streaming_mysql_job_special_offset", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_special_offset"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "special_offset_mysql_tbl"
    def mysqlDb = "test_cdc_db"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

        // prepare MySQL source table
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table1}"""
            sql """CREATE TABLE ${mysqlDb}.${table1} (
                  `id` int NOT NULL,
                  `name` varchar(100) DEFAULT NULL,
                  PRIMARY KEY (`id`)
                ) ENGINE=InnoDB"""
            sql """INSERT INTO ${mysqlDb}.${table1} VALUES (1, 'alice'), (2, 'bob')"""
        }

        // ===== Test 1: offset = earliest, verify data synced =====
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
                    "offset" = "earliest"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """
        Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
            def result = sql """SELECT count(*) FROM ${currentDb}.${table1}"""
            return result[0][0] >= 2
        })
        def rows = sql """SELECT * FROM ${currentDb}.${table1} order by id"""
        assert rows.size() == 2
        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        sql """drop table if exists ${currentDb}.${table1} force"""

        // ===== Test 2: offset = latest, then insert new data, verify synced =====
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
                    "offset" = "latest"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """
        // wait for job to be running
        Awaitility.await().atMost(60, SECONDS).pollInterval(2, SECONDS).until({
            def jobStatus = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
            return jobStatus.size() == 1 && jobStatus[0][0] == "RUNNING"
        })
        // insert new data after job started with latest offset
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """INSERT INTO ${mysqlDb}.${table1} VALUES (3, 'charlie')"""
        }
        Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
            def result = sql """SELECT count(*) FROM ${currentDb}.${table1}"""
            return result[0][0] >= 1
        })
        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        sql """drop table if exists ${currentDb}.${table1} force"""

        // ===== Test 3: ALTER JOB to change offset =====
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
                    "offset" = "latest"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """
        Awaitility.await().atMost(60, SECONDS).pollInterval(2, SECONDS).until({
            def jobStatus = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
            return jobStatus.size() == 1 && jobStatus[0][0] == "RUNNING"
        })
        // pause, then alter offset to initial
        sql "PAUSE JOB where jobname = '${jobName}'"
        Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).until({
            def jobStatus = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
            return jobStatus[0][0] == "PAUSED"
        })
        sql """ALTER JOB ${jobName}
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
        sql "RESUME JOB where jobname = '${jobName}'"
        // after alter to initial, snapshot data should sync
        Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
            def result = sql """SELECT count(*) FROM ${currentDb}.${table1}"""
            return result[0][0] >= 3
        })
        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        sql """drop table if exists ${currentDb}.${table1} force"""

        // ===== Test 4: invalid offset format =====
        test {
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
                    "offset" = "not_valid_offset"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """
            exception "Invalid value for key 'offset'"
        }

        // ===== Test 5: JSON binlog offset, verify data synced =====
        // Get current binlog position, insert data after it, then create job from that position
        def binlogFile = ""
        def binlogPos = ""
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            def masterStatus = sql """SHOW MASTER STATUS"""
            binlogFile = masterStatus[0][0]
            binlogPos = masterStatus[0][1].toString()
            log.info("Current binlog position: file=${binlogFile}, pos=${binlogPos}")
            // insert data after this binlog position
            sql """INSERT INTO ${mysqlDb}.${table1} VALUES (10, 'specific1')"""
            sql """INSERT INTO ${mysqlDb}.${table1} VALUES (11, 'specific2')"""
        }
        def offsetJson = """{"file":"${binlogFile}","pos":"${binlogPos}"}"""
        log.info("Using JSON offset: ${offsetJson}")
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
                    "offset" = '${offsetJson}'
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """
        Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
            def result = sql """SELECT count(*) FROM ${currentDb}.${table1}"""
            return result[0][0] >= 2
        })
        def specificRows = sql """SELECT * FROM ${currentDb}.${table1} WHERE id IN (10, 11) order by id"""
        log.info("specificRows: " + specificRows)
        assert specificRows.size() == 2
        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        sql """drop table if exists ${currentDb}.${table1} force"""

        // cleanup MySQL source table
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table1}"""
        }
    }
}
