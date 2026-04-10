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

suite("test_streaming_mysql_job_restart_fe_with_props", "docker,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_restart_fe_with_props"
    def options = new ClusterOptions()
    options.setFeNum(1)
    options.cloudMode = null

    docker(options) {
        def currentDb = (sql "select database()")[0][0]
        def table1 = "restart_props_user_info"
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

            connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
                sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
                sql """DROP TABLE IF EXISTS ${mysqlDb}.${table1}"""
                sql """CREATE TABLE ${mysqlDb}.${table1} (
                    `name` varchar(200) NOT NULL,
                    `age` int DEFAULT NULL,
                    PRIMARY KEY (`name`)
                    ) ENGINE=InnoDB"""
                sql """INSERT INTO ${mysqlDb}.${table1} (name, age) VALUES ('A1', 1);"""
                sql """INSERT INTO ${mysqlDb}.${table1} (name, age) VALUES ('B1', 2);"""
            }

            // Create job with explicit max_interval property.
            // Before the fix, after FE restart the max_interval would not be parsed
            // from properties in the constructor, causing timeoutMs=0 and every task
            // to timeout immediately.
            sql """CREATE JOB ${jobName}
                    PROPERTIES("max_interval" = "5")
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

            // Wait for snapshot data to be loaded
            try {
                Awaitility.await().atMost(300, SECONDS)
                        .pollInterval(1, SECONDS).until(
                        {
                            def jobSucceedCount = sql """ select SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                            log.info("jobSucceedCount: " + jobSucceedCount)
                            jobSucceedCount.size() == 1 && '2' <= jobSucceedCount.get(0).get(0)
                        }
                )
            } catch (Exception ex) {
                def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
                def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
                log.info("show job: " + showjob)
                log.info("show task: " + showtask)
                throw ex;
            }

            def jobInfoBeforeRestart = sql """
                select loadStatistic, status, currentOffset from jobs("type"="insert") where Name='${jobName}'
            """
            log.info("jobInfoBeforeRestart: " + jobInfoBeforeRestart)
            def loadStatBefore = parseJson(jobInfoBeforeRestart.get(0).get(0))
            assert loadStatBefore.scannedRows == 2
            assert jobInfoBeforeRestart.get(0).get(1) == "RUNNING"

            // Restart FE
            cluster.restartFrontends()
            sleep(60000)
            context.reconnectFe()

            // Insert new data after restart
            connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
                sql """INSERT INTO ${mysqlDb}.${table1} (name, age) VALUES ('C1', 3);"""
            }

            // Verify new data gets consumed after restart.
            // Before the fix, timeoutMs=0 caused every task to timeout immediately,
            // so the job would be stuck in PAUSED and never consume new data.
            try {
                Awaitility.await().atMost(120, SECONDS)
                        .pollInterval(2, SECONDS).until(
                        {
                            def loadStat = sql """ select loadStatistic from jobs("type"="insert") where Name = '${jobName}' """
                            def stat = parseJson(loadStat.get(0).get(0))
                            log.info("scannedRows after restart: " + stat.scannedRows)
                            stat.scannedRows == 3
                        }
                )
            } catch (Exception ex) {
                def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
                def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
                log.info("show job after restart insert: " + showjob)
                log.info("show task after restart insert: " + showtask)
                throw ex;
            }

            def result = sql """select * from ${currentDb}.${table1} order by name"""
            log.info("final result: " + result)
            assert result.size() == 3

            sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        }
    }
}
