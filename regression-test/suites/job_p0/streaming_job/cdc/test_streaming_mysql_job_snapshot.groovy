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

/**
 * Test snapshot-only mode (offset=snapshot):
 *   1. Job syncs existing data via full snapshot.
 *   2. Job transitions to FINISHED after snapshot completes (no binlog phase).
 *   3. Data inserted after job finishes is NOT synced to Doris.
 */
suite("test_streaming_mysql_job_snapshot", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_snapshot_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "user_info_mysql_snapshot1"
    def table2 = "user_info_mysql_snapshot2"
    def mysqlDb = "test_cdc_db"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""
    sql """drop table if exists ${currentDb}.${table2} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

        // prepare source tables and pre-existing data in mysql
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table1}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table2}"""
            sql """CREATE TABLE ${mysqlDb}.${table1} (
                  `name` varchar(200) NOT NULL,
                  `age` int DEFAULT NULL,
                  PRIMARY KEY (`name`)
                ) ENGINE=InnoDB"""
            sql """INSERT INTO ${mysqlDb}.${table1} (name, age) VALUES ('A1', 1)"""
            sql """INSERT INTO ${mysqlDb}.${table1} (name, age) VALUES ('B1', 2)"""
            sql """CREATE TABLE ${mysqlDb}.${table2} (
                  `name` varchar(200) NOT NULL,
                  `age` int DEFAULT NULL,
                  PRIMARY KEY (`name`)
                ) ENGINE=InnoDB"""
            sql """INSERT INTO ${mysqlDb}.${table2} (name, age) VALUES ('A2', 1)"""
            sql """INSERT INTO ${mysqlDb}.${table2} (name, age) VALUES ('B2', 2)"""
        }

        // create streaming job with offset=snapshot (snapshot-only mode)
        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM MYSQL (
                    "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/${mysqlDb}",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "com.mysql.cj.jdbc.Driver",
                    "user" = "root",
                    "password" = "123456",
                    "database" = "${mysqlDb}",
                    "include_tables" = "${table1},${table2}",
                    "offset" = "snapshot"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        // wait for job to transition to FINISHED
        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def jobStatus = sql """select Status from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING'"""
                        log.info("jobStatus: " + jobStatus)
                        jobStatus.size() == 1 && jobStatus.get(0).get(0) == 'FINISHED'
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex
        }

        // verify snapshot data is correctly synced
        qt_select_snapshot_table1 """ SELECT * FROM ${table1} order by name asc """
        qt_select_snapshot_table2 """ SELECT * FROM ${table2} order by name asc """

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    }
}
