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

suite("test_streaming_mysql_job_snapshot_finished_restart_fe",
        "docker,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_snapshot_finished_restart_fe"
    def tableName = "snapshot_finished_restart_fe"
    def mysqlDb = "test_cdc_db"
    def totalRows = 5
    def options = new ClusterOptions()
    options.setFeNum(1)
    options.cloudMode = null

    docker(options) {
        def currentDb = (sql "select database()")[0][0]

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        sql """DROP TABLE IF EXISTS ${currentDb}.${tableName} FORCE"""

        String enabled = context.config.otherConfigs.get("enableJdbcTest")
        if (enabled != null && enabled.equalsIgnoreCase("true")) {
            String mysqlPort = context.config.otherConfigs.get("mysql_57_port")
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String s3Endpoint = getS3Endpoint()
            String bucket = getS3BucketName()
            String driverUrl =
                    "https://${bucket}.${s3Endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

            connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysqlPort}") {
                sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
                sql """DROP TABLE IF EXISTS ${mysqlDb}.${tableName}"""
                sql """CREATE TABLE ${mysqlDb}.${tableName} (
                    `id` int NOT NULL,
                    `name` varchar(200),
                    PRIMARY KEY (`id`)
                ) ENGINE=InnoDB"""
                sql """INSERT INTO ${mysqlDb}.${tableName} (id, name) VALUES
                    (1, 'name_1'),
                    (2, 'name_2'),
                    (3, 'name_3'),
                    (4, 'name_4'),
                    (5, 'name_5')"""
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
                        "include_tables" = "${tableName}",
                        "offset" = "snapshot",
                        "snapshot_split_size" = "1",
                        "snapshot_parallelism" = "1"
                    )
                    TO DATABASE ${currentDb} (
                        "table.create.properties.replication_num" = "1"
                    )
                """

            try {
                Awaitility.await().atMost(300, SECONDS)
                        .pollInterval(2, SECONDS).until(
                        {
                            def jobStatus = sql """
                                SELECT Status
                                FROM jobs("type"="insert")
                                WHERE Name='${jobName}' AND ExecuteType='STREAMING'
                            """
                            log.info("jobStatus before FE restart: " + jobStatus)
                            jobStatus.size() == 1 && jobStatus.get(0).get(0) == "FINISHED"
                        }
                )

                def jobIdRows = sql """
                    SELECT Id
                    FROM jobs("type"="insert")
                    WHERE Name='${jobName}' AND ExecuteType='STREAMING'
                """
                assert jobIdRows.size() == 1
                def jobId = jobIdRows.get(0).get(0).toString()

                def rowsBeforeRestart = sql """
                    SELECT COUNT(*), COUNT(DISTINCT id)
                    FROM ${currentDb}.${tableName}
                """
                assert rowsBeforeRestart.size() == 1
                assert rowsBeforeRestart.get(0).get(0) == totalRows
                assert rowsBeforeRestart.get(0).get(1) == totalRows

                cluster.restartFrontends()
                sleep(60000)
                context.reconnectFe()

                // A terminal job must survive replay with its finish time. Otherwise
                // the scheduler may treat it as an expired job and remove it.
                Awaitility.await().atMost(120, SECONDS)
                        .pollInterval(2, SECONDS).until(
                        {
                            def jobAfterRestart = sql """
                                SELECT Id, Status
                                FROM jobs("type"="insert")
                                WHERE Name='${jobName}' AND ExecuteType='STREAMING'
                            """
                            log.info("job after FE restart: " + jobAfterRestart)
                            jobAfterRestart.size() == 1
                                    && jobAfterRestart.get(0).get(0).toString() == jobId
                                    && jobAfterRestart.get(0).get(1) == "FINISHED"
                        }
                )

                def rowsAfterRestart = sql """
                    SELECT COUNT(*), COUNT(DISTINCT id)
                    FROM ${currentDb}.${tableName}
                """
                assert rowsAfterRestart.size() == 1
                assert rowsAfterRestart.get(0).get(0) == totalRows
                assert rowsAfterRestart.get(0).get(1) == totalRows
            } catch (Exception ex) {
                def showJob = sql """
                    SELECT *
                    FROM jobs("type"="insert")
                    WHERE Name='${jobName}'
                """
                def showTask = sql """
                    SELECT *
                    FROM tasks("type"="insert")
                    WHERE JobName='${jobName}'
                """
                log.info("show job: " + showJob)
                log.info("show task: " + showTask)
                throw ex
            } finally {
                sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
            }
        }
    }
}
