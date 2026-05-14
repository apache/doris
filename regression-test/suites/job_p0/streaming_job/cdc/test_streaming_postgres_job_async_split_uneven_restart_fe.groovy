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

// Restarts FE while the uneven splitter is mid-snapshot. Combines the slow uneven
// path (VARCHAR PK -> splitOneUnevenlySizedChunk) with editlog replay to verify
// cdcSplitProgress resumes from the system table — no rows lost, no duplicates
// from re-cut chunks. batch_size is shrunk so the splitter must make multiple
// RPCs, otherwise the default would let cdc_client finish all chunks in one shot
// and the restart would only test task-dispatch resume, not split resume.
suite("test_streaming_postgres_job_async_split_uneven_restart_fe",
        "docker,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_async_split_uneven_restart_fe"
    def options = new ClusterOptions()
    options.setFeNum(1)
    options.cloudMode = null

    docker(options) {
        def currentDb = (sql "select database()")[0][0]
        def table1 = "user_info_pg_async_split_uneven_restart"
        def pgDB = "postgres"
        def pgSchema = "cdc_test"
        def pgUser = "postgres"
        def pgPassword = "123456"
        def totalRows = 200
        int expectedChunks = (int) Math.ceil(totalRows / 5.0)   // 100

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        sql """drop table if exists ${currentDb}.${table1} force"""

        String enabled = context.config.otherConfigs.get("enableJdbcTest")
        if (enabled != null && enabled.equalsIgnoreCase("true")) {
            String pg_port = context.config.otherConfigs.get("pg_14_port");
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String s3_endpoint = getS3Endpoint()
            String bucket = getS3BucketName()
            String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

            // Shrink batch_size so 100 chunks need ~20 RPCs — required to keep the
            // splitter mid-flight when restart fires. With default 100, cdc_client
            // returns all chunks in one RPC and the restart only validates task
            // dispatch resume, not cdcSplitProgress replay.
            def origBatchSize = sql("""ADMIN SHOW FRONTEND CONFIG LIKE 'streaming_cdc_fetch_splits_batch_size'""")
                    .get(0).get(1)
            sql """ADMIN SET FRONTEND CONFIG ('streaming_cdc_fetch_splits_batch_size' = '5')"""

            try {
            // VARCHAR PK forces the uneven splitter; split_size=5 + parallelism=1 keeps
            // consumption slow enough that we can restart while still mid-snapshot.
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

            def jobIdRow = sql """select Id from jobs("type"="insert") where Name='${jobName}'"""
            assert jobIdRow.size() == 1
            def jobId = jobIdRow.get(0).get(0).toString()

            // Wait until at least 3 tasks succeeded AND splitter is still mid-flight
            // (chunk_list < expectedChunks). Both conditions: task dispatch has made
            // observable progress AND cdcSplitProgress has work left to replay.
            try {
                Awaitility.await().atMost(180, SECONDS)
                        .pollInterval(1, SECONDS).until(
                        {
                            def succeed = sql """select SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}'"""
                            int chunkLen = -1
                            try {
                                def chunkRow = sql """SELECT json_length(chunk_list)
                                                      FROM internal.__internal_schema.streaming_job_meta
                                                      WHERE job_id='${jobId}'"""
                                if (chunkRow.size() > 0 && chunkRow.get(0).get(0) != null) {
                                    chunkLen = chunkRow.get(0).get(0) as int
                                }
                            } catch (Throwable ignored) { /* meta table is lazy-created on first UPSERT */ }
                            log.info("pre-restart succeed=${succeed} chunkLen=${chunkLen}")
                            succeed.size() == 1
                                    && Integer.parseInt(succeed.get(0).get(0).toString()) >= 3
                                    && chunkLen > 0 && chunkLen < expectedChunks
                        }
                )
            } catch (Exception ex) {
                def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
                log.info("show job: " + showjob)
                throw ex
            }

            def rowsBefore = sql """SELECT COUNT(*) FROM ${currentDb}.${table1}"""
            def chunkLenBefore = sql """SELECT json_length(chunk_list)
                                        FROM internal.__internal_schema.streaming_job_meta
                                        WHERE job_id='${jobId}'""".get(0).get(0) as int
            log.info("before restart: rows=${rowsBefore} chunkLen=${chunkLenBefore}")
            assert rowsBefore.get(0).get(0) < totalRows :
                    "uneven snapshot finished too fast (${rowsBefore.get(0).get(0)} rows) — restart wouldn't be mid-snapshot"
            assert chunkLenBefore < expectedChunks :
                    "splitter already finished (chunk_list=${chunkLenBefore}/${expectedChunks}) — restart can't exercise cdcSplitProgress replay"

            cluster.restartFrontends()
            sleep(30000)
            context.reconnectFe()

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
                    "row count matches but DISTINCT differs — chunks re-cut on resume"

            // Spot-check both ends of the PK range; if any chunk was lost on the boundary
            // the missing rows would cluster at one end.
            def firstRow = sql """SELECT name FROM ${currentDb}.${table1} WHERE id='k_00001'"""
            def lastRow  = sql """SELECT name FROM ${currentDb}.${table1} WHERE id='k_00500'"""
            assert firstRow.size() == 1 && firstRow.get(0).get(0) == 'name_1'
            assert lastRow.size()  == 1 && lastRow.get(0).get(0) == 'name_500'

            def status = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
            assert status.get(0).get(0) == "RUNNING"

            // Binlog phase after resume.
            connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
                sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, name) VALUES ('k_99999', 'post_restart');"""
            }
            try {
                Awaitility.await().atMost(120, SECONDS)
                        .pollInterval(2, SECONDS).until(
                        {
                            def r = sql """SELECT count(*) FROM ${currentDb}.${table1} WHERE id='k_99999'"""
                            r.size() == 1 && r.get(0).get(0) == 1
                        }
                )
            } catch (Exception ex) {
                log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
                throw ex
            }

            sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
            } finally {
                sql """ADMIN SET FRONTEND CONFIG ('streaming_cdc_fetch_splits_batch_size' = '${origBatchSize}')"""
            }
        }
    }
}
