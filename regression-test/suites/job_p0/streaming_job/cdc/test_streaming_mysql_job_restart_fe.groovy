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

suite("test_streaming_mysql_job_restart_fe", "docker,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_restart_fe"
    def options = new ClusterOptions()
    options.setFeNum(1)
    // run in cloud and not cloud
    options.cloudMode = null

    docker(options) {
        def currentDb = (sql "select database()")[0][0]
        def table1 = "restart_user_info"
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

            // create test
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
            // check table created
            def showTables = sql """ show tables from ${currentDb} like '${table1}'; """
            assert showTables.size() == 1

            // check job running
            try {
                Awaitility.await().atMost(300, SECONDS)
                        .pollInterval(1, SECONDS).until(
                        {
                            def jobSuccendCount = sql """ select SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                            log.info("jobSuccendCount: " + jobSuccendCount)
                            // check job status and succeed task count larger than 2
                            jobSuccendCount.size() == 1 && '2' <= jobSuccendCount.get(0).get(0)
                        }
                )
            } catch (Exception ex){
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
            assert loadStatBefore.loadBytes == 95
            assert jobInfoBeforeRestart.get(0).get(1) == "RUNNING"

            // Restart FE
            cluster.restartFrontends()
            sleep(60000)
            context.reconnectFe()

            // check is it consistent after restart
            def jobAfterRestart = sql """
                select loadStatistic, status, currentOffset from jobs("type"="insert") where Name='${jobName}'
            """
            log.info("jobAfterRestart: " + jobAfterRestart)
            def loadStatAfter = parseJson(jobAfterRestart.get(0).get(0))
            assert loadStatAfter.scannedRows == 2
            assert loadStatAfter.loadBytes == 95
            assert jobAfterRestart.get(0).get(1) == "RUNNING"
            assert jobAfterRestart.get(0).get(2) == jobInfoBeforeRestart.get(0).get(2)

            sql """
                DROP JOB IF EXISTS where jobname =  '${jobName}'
            """
            def jobCountRsp = sql """select count(1) from jobs("type"="insert")  where Name ='${jobName}'"""
            assert jobCountRsp.get(0).get(0) == 0
        }
    }
}
