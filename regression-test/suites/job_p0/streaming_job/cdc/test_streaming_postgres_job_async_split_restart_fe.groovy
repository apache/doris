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

// Verifies async chunk splitting survives FE restart mid-snapshot: cdcSplitProgress
// resumes from the system table on next tick, no chunks are re-cut, no rows lost.
suite("test_streaming_postgres_job_async_split_restart_fe",
        "docker,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_async_split_restart_fe"
    def options = new ClusterOptions()
    options.setFeNum(1)
    options.cloudMode = null

    docker(options) {
        def currentDb = (sql "select database()")[0][0]
        def table1 = "user_info_pg_async_split_restart"
        def pgDB = "postgres"
        def pgSchema = "cdc_test"
        def pgUser = "postgres"
        def pgPassword = "123456"
        def totalRows = 500

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        sql """drop table if exists ${currentDb}.${table1} force"""

        String enabled = context.config.otherConfigs.get("enableJdbcTest")
        if (enabled != null && enabled.equalsIgnoreCase("true")) {
            String pg_port = context.config.otherConfigs.get("pg_14_port");
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String s3_endpoint = getS3Endpoint()
            String bucket = getS3BucketName()
            String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

            // 500 rows + split_size=10 -> 50 splits; parallelism=1 keeps consumption slow
            // enough that we can restart while still mid-snapshot.
            connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
                sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
                sql """CREATE TABLE ${pgDB}.${pgSchema}.${table1} (
                      "id" int4 PRIMARY KEY,
                      "name" varchar(200)
                    )"""
                StringBuilder sb = new StringBuilder()
                sb.append("INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, name) VALUES ")
                for (int i = 1; i <= totalRows; i++) {
                    if (i > 1) sb.append(", ")
                    sb.append("(${i}, 'name_${i}')")
                }
                sql sb.toString()
            }

            sql """CREATE JOB ${jobName}
                    ON STREAMING
                    FROM POSTGRES (
                        "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}",
                        "driver_url" = "${driver_url}",
                        "driver_class" = "org.postgresql.Driver",
                        "user" = "${pgUser}",
                        "password" = "${pgPassword}",
                        "database" = "${pgDB}",
                        "schema" = "${pgSchema}",
                        "include_tables" = "${table1}",
                        "offset" = "initial",
                        "snapshot_split_size" = "10",
                        "snapshot_parallelism" = "1"
                    )
                    TO DATABASE ${currentDb} (
                      "table.create.properties.replication_num" = "1"
                    )
                """

            // Wait until at least 3 tasks succeeded (≈30% snapshot progress) before restart,
            // so cdcSplitProgress is mid-table when FE goes down.
            try {
                Awaitility.await().atMost(120, SECONDS)
                        .pollInterval(1, SECONDS).until(
                        {
                            def succeed = sql """select SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}'"""
                            log.info("SucceedTaskCount before restart: ${succeed}")
                            succeed.size() == 1 && Integer.parseInt(succeed.get(0).get(0).toString()) >= 3
                        }
                )
            } catch (Exception ex) {
                def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
                log.info("show job: " + showjob)
                throw ex
            }

            // Capture state before restart so we can confirm progress survives editlog replay.
            def rowsBefore = sql """SELECT COUNT(*) FROM ${currentDb}.${table1}"""
            def progressBefore = sql """select SucceedTaskCount from jobs("type"="insert") where Name='${jobName}'"""
            log.info("before restart: rows=${rowsBefore} succeedTasks=${progressBefore}")
            assert rowsBefore.get(0).get(0) < totalRows :
                    "snapshot finished too fast (${rowsBefore.get(0).get(0)} rows) — restart wouldn't be mid-snapshot"

            cluster.restartFrontends()
            sleep(30000)
            context.reconnectFe()

            // After restart, snapshot must complete; final count = totalRows, no duplicates.
            try {
                Awaitility.await().atMost(600, SECONDS)
                        .pollInterval(2, SECONDS).until(
                        {
                            def cnt = sql """SELECT COUNT(*) FROM ${currentDb}.${table1}"""
                            log.info("doris row count after restart: ${cnt}")
                            cnt.size() == 1 && cnt.get(0).get(0) == totalRows
                        }
                )
            } catch (Exception ex) {
                def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
                def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
                log.info("show job: " + showjob)
                log.info("show task: " + showtask)
                throw ex
            }

            def distinctCnt = sql """SELECT COUNT(DISTINCT id) FROM ${currentDb}.${table1}"""
            assert distinctCnt.get(0).get(0) == totalRows :
                    "row count matches but PK distinct count differs — duplicates from re-cutting split chunks"

            // Job should reach binlog phase (RUNNING, snapshot transitioned cleanly).
            def status = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
            assert status.get(0).get(0) == "RUNNING"

            sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        }
    }
}
