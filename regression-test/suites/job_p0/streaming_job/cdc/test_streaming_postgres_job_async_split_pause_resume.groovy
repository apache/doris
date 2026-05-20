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

// PAUSE mid-snapshot must stopAsyncWork the splitter thread (SucceedTaskCount stops
// growing); RESUME must startAsyncWorkIfNeed and continue from cdcSplitProgress
// without re-cutting committed chunks. Uses uneven path so the snapshot is slow
// enough to catch mid-flight.
suite("test_streaming_postgres_job_async_split_pause_resume",
        "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_async_split_pause_resume_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "user_info_pg_async_split_pause_resume"
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"
    def totalRows = 200

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port = context.config.otherConfigs.get("pg_14_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${table1} (
                  "id" varchar(20) PRIMARY KEY,
                  "name" varchar(200)
                )"""
            StringBuilder sb = new StringBuilder()
            sb.append("INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, name) VALUES ")
            for (int i = 1; i <= totalRows; i++) {
                if (i > 1) sb.append(", ")
                String key = "k_" + String.format("%05d", i)
                sb.append("('${key}', 'name_${i}')")
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
                    "snapshot_split_size" = "5",
                    "snapshot_parallelism" = "1"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        // Catch the job mid-snapshot — wait for some progress, but not finished.
        try {
            Awaitility.await().atMost(180, SECONDS)
                    .pollInterval(1, SECONDS).until(
                    {
                        def succeed = sql """select SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}'"""
                        def cnt = sql """SELECT COUNT(*) FROM ${currentDb}.${table1}"""
                        log.info("pre-pause succeed=${succeed} rows=${cnt}")
                        succeed.size() == 1 && Integer.parseInt(succeed.get(0).get(0).toString()) >= 2
                                && cnt.get(0).get(0) < totalRows
                    }
            )
        } catch (Exception ex) {
            log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            throw ex
        }

        def rowsBeforePause = sql("""SELECT COUNT(*) FROM ${currentDb}.${table1}""").get(0).get(0) as int
        assert rowsBeforePause < totalRows : "snapshot finished before we could pause"

        // PAUSE — splitter thread should stop within a few ticks.
        sql """PAUSE JOB where jobname = '${jobName}'"""
        try {
            Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).until({
                def status = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
                status.size() == 1 && status.get(0).get(0) == "PAUSED"
            })
        } catch (Exception ex) {
            log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            throw ex
        }

        // Capture state, sleep, recapture — succeedTaskCount must not grow while paused.
        def succeedAtPause = sql("""select SucceedTaskCount from jobs("type"="insert") where Name='${jobName}'""")
                .get(0).get(0).toString()
        def rowsAtPause = sql("""SELECT COUNT(*) FROM ${currentDb}.${table1}""").get(0).get(0) as int
        sleep(15000)
        def succeedAfterSleep = sql("""select SucceedTaskCount from jobs("type"="insert") where Name='${jobName}'""")
                .get(0).get(0).toString()
        def rowsAfterSleep = sql("""SELECT COUNT(*) FROM ${currentDb}.${table1}""").get(0).get(0) as int
        log.info("paused: succeed ${succeedAtPause}->${succeedAfterSleep} rows ${rowsAtPause}->${rowsAfterSleep}")
        assert succeedAfterSleep == succeedAtPause :
                "SucceedTaskCount grew while paused (${succeedAtPause} -> ${succeedAfterSleep}) — splitter not stopped"
        assert rowsAfterSleep == rowsAtPause :
                "row count grew while paused (${rowsAtPause} -> ${rowsAfterSleep}) — tasks still running"

        def pausedStatus = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
        assert pausedStatus.get(0).get(0) == "PAUSED" : "job didn't stay PAUSED"

        // RESUME — splitter thread should restart from cdcSplitProgress.
        sql """RESUME JOB where jobname = '${jobName}'"""
        try {
            Awaitility.await().atMost(60, SECONDS).pollInterval(1, SECONDS).until({
                def status = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
                status.size() == 1 && status.get(0).get(0) == "RUNNING"
            })
        } catch (Exception ex) {
            log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            throw ex
        }

        try {
            Awaitility.await().atMost(600, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """SELECT COUNT(*) FROM ${currentDb}.${table1}"""
                        log.info("post-resume rows: ${cnt}")
                        cnt.size() == 1 && cnt.get(0).get(0) == totalRows
                    }
            )
        } catch (Exception ex) {
            log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            log.info("tasks: " + (sql """select * from tasks("type"="insert") where JobName='${jobName}'"""))
            throw ex
        }

        // DISTINCT must equal totalRows — resume must not re-cut committed chunks.
        def distinctCnt = sql """SELECT COUNT(DISTINCT id) FROM ${currentDb}.${table1}"""
        assert distinctCnt.get(0).get(0) == totalRows :
                "DISTINCT count ${distinctCnt.get(0).get(0)} != ${totalRows} — resume re-cut chunks"

        def jobInfo = sql """select status, FailedTaskCount, ErrorMsg from jobs("type"="insert") where Name='${jobName}'"""
        assert jobInfo.get(0).get(0) == "RUNNING"
        assert (jobInfo.get(0).get(1) as int) == 0 : "FailedTaskCount should be 0 after resume"

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    }
}
