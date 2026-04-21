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

/**
 * Verify that ALTERed source/target properties survive FE restart.
 *
 * replayOnUpdated() currently syncs properties (job props), executeSql,
 * offsetProviderPersist, etc., but does NOT sync sourceProperties and
 * targetProperties. After FE restart, the CDC job reverts to the original
 * source/target properties written at CREATE time.
 *
 * This test:
 *   1. Creates a CDC job with only "replication_num" in target properties
 *   2. ALTERs target properties to add "load.max_filter_ratio"
 *   3. ALTERs source properties to change "user" to a different value
 *   4. Restarts FE
 *   5. Asserts that ExecuteSql (built from sourceProperties/targetProperties)
 *      still contains the ALTERed values
 */
suite("test_streaming_mysql_job_alter_props_restart_fe", "docker,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_alter_props_restart_fe"
    def options = new ClusterOptions()
    options.setFeNum(1)
    options.cloudMode = null

    docker(options) {
        def currentDb = (sql "select database()")[0][0]
        def table1 = "alter_props_restart_user"
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

            // Prepare upstream MySQL data
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

            // Step 1: Create CDC job with initial target properties
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

            // Step 2: Wait for initial data to be consumed
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

            // Step 3: PAUSE the job
            sql """PAUSE JOB where jobname = '${jobName}'"""
            Awaitility.await().atMost(30, SECONDS)
                    .pollInterval(1, SECONDS).until(
                    {
                        def st = sql """ select status from jobs("type"="insert") where Name = '${jobName}' """
                        st.get(0).get(0) == "PAUSED"
                    }
            )

            // Step 4: ALTER source properties (change user to a non-existent value)
            //         and target properties (add load.max_filter_ratio).
            // Using a distinct user value gives the assertion discriminating power: the
            // original CREATE also sets user=root, so same-value ALTER would not prove
            // that replayOnUpdated actually propagated the new map.
            sql """ALTER JOB ${jobName}
                    FROM MYSQL (
                        "user" = "alter_restart_probe_user"
                    )
                    TO DATABASE ${currentDb} (
                        "load.max_filter_ratio" = "0.5"
                    )
                """

            // Step 5: Verify ALTER took effect before restart
            def execSqlBeforeRestart = sql """
                select ExecuteSql from jobs("type"="insert") where Name='${jobName}'
            """
            log.info("ExecuteSql before restart: " + execSqlBeforeRestart.get(0).get(0))
            assert execSqlBeforeRestart.get(0).get(0).contains("max_filter_ratio") :
                "target properties should contain max_filter_ratio after ALTER"
            assert execSqlBeforeRestart.get(0).get(0).contains("alter_restart_probe_user") :
                "source properties should contain the altered user after ALTER"

            // Step 6: Restart FE
            cluster.restartFrontends()
            sleep(60000)
            context.reconnectFe()

            // Step 7: Verify properties survived restart
            def jobInfoAfterRestart = sql """
                select status, ExecuteSql from jobs("type"="insert") where Name='${jobName}'
            """
            log.info("jobInfoAfterRestart: " + jobInfoAfterRestart)
            assert jobInfoAfterRestart.get(0).get(0) == "PAUSED"

            def execSqlAfterRestart = jobInfoAfterRestart.get(0).get(1)
            log.info("ExecuteSql after restart: " + execSqlAfterRestart)
            // If replayOnUpdated does not sync targetProperties, max_filter_ratio will be lost
            assert execSqlAfterRestart.contains("max_filter_ratio") :
                "targetProperties lost after FE restart — replayOnUpdated did not sync targetProperties"
            // If replayOnUpdated does not sync sourceProperties, the altered user will be lost
            assert execSqlAfterRestart.contains("alter_restart_probe_user") :
                "sourceProperties lost after FE restart — replayOnUpdated did not sync sourceProperties"

            // Step 8: ALTER user back so RESUME can authenticate upstream
            sql """ALTER JOB ${jobName}
                    FROM MYSQL (
                        "user" = "root"
                    )
                    TO DATABASE ${currentDb}
                """

            // Step 9: RESUME and verify job can still run with new data
            connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
                sql """INSERT INTO ${mysqlDb}.${table1} (name, age) VALUES ('C1', 3);"""
            }

            sql """RESUME JOB where jobname = '${jobName}'"""

            try {
                Awaitility.await().atMost(120, SECONDS)
                        .pollInterval(2, SECONDS).until(
                        {
                            def loadStat = sql """ select loadStatistic from jobs("type"="insert") where Name = '${jobName}' """
                            def stat = parseJson(loadStat.get(0).get(0))
                            log.info("scannedRows after restart resume: " + stat.scannedRows)
                            stat.scannedRows == 3
                        }
                )
            } catch (Exception ex) {
                def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
                def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
                log.info("show job after restart resume: " + showjob)
                log.info("show task after restart resume: " + showtask)
                throw ex;
            }

            def result = sql """select * from ${currentDb}.${table1} order by name"""
            log.info("final result: " + result)
            assert result.size() == 3

            sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        }
    }
}
