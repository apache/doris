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

// TVF (cdc_stream) variant of the uneven-restart-fe case. The TVF path has its own
// JdbcTvfSourceOffsetProvider.replayIfNeed; this case verifies that after a mid-snapshot
// FE restart, cachedSyncTables is restored, restored remainingSplits are consumed, AND
// advanceSplitsIfNeed continues to fetch the next batch (i.e. snapshot is not truncated
// to just the chunks already in the meta table).
suite("test_streaming_job_cdc_stream_postgres_async_split_restart_fe",
        "docker,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_job_cdc_stream_postgres_async_split_restart_fe"
    def options = new ClusterOptions()
    options.setFeNum(1)
    options.cloudMode = null

    docker(options) {
        def currentDb = (sql "select database()")[0][0]
        def dorisTable = "streaming_job_cdc_stream_postgres_async_split_restart_fe_tbl"
        def pgDB = "postgres"
        def pgSchema = "cdc_test"
        def pgUser = "postgres"
        def pgPassword = "123456"
        def pgTable = "streaming_job_cdc_stream_postgres_async_split_restart_fe_src"
        def totalRows = 200
        int expectedChunks = (int) Math.ceil(totalRows / 5.0)   // 100

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        sql """drop table if exists ${currentDb}.${dorisTable} force"""

        sql """
            CREATE TABLE IF NOT EXISTS ${currentDb}.${dorisTable} (
                `id`   varchar(20) NULL,
                `name` varchar(200) NULL,
                `age`  int NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS AUTO
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """

        String enabled = context.config.otherConfigs.get("enableJdbcTest")
        if (enabled != null && enabled.equalsIgnoreCase("true")) {
            String pg_port = context.config.otherConfigs.get("pg_14_port");
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String s3_endpoint = getS3Endpoint()
            String bucket = getS3BucketName()
            String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

            // Shrink batch_size so 100 chunks need ~20 RPCs — required so cdcSplitProgress
            // is genuinely mid-flight when restart fires (default 100 = one-shot split).
            def origBatchSize = sql("""ADMIN SHOW FRONTEND CONFIG LIKE 'streaming_cdc_fetch_splits_batch_size'""")
                    .get(0).get(1)
            sql """ADMIN SET FRONTEND CONFIG ('streaming_cdc_fetch_splits_batch_size' = '5')"""

            try {
                connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
                    sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${pgTable}"""
                    sql """CREATE TABLE ${pgDB}.${pgSchema}.${pgTable} (
                          "id" varchar(20) PRIMARY KEY,
                          "name" varchar(200),
                          "age" int4
                        )"""
                    StringBuilder sb = new StringBuilder()
                    sb.append("INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (id, name, age) VALUES ")
                    for (int i = 1; i <= totalRows; i++) {
                        if (i > 1) sb.append(", ")
                        String key = "k_" + String.format("%05d", i)
                        sb.append("('${key}', 'name_${i}', ${i})")
                    }
                    sql sb.toString()
                }

                sql """
                    CREATE JOB ${jobName}
                    ON STREAMING DO INSERT INTO ${currentDb}.${dorisTable} (id, name, age)
                    SELECT id, name, age FROM cdc_stream(
                        "type"               = "postgres",
                        "jdbc_url"           = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}",
                        "driver_url"         = "${driver_url}",
                        "driver_class"       = "org.postgresql.Driver",
                        "user"               = "${pgUser}",
                        "password"           = "${pgPassword}",
                        "database"           = "${pgDB}",
                        "schema"             = "${pgSchema}",
                        "table"              = "${pgTable}",
                        "offset"             = "initial",
                        "snapshot_split_size"  = "5",
                        "snapshot_parallelism" = "1"
                    )
                """

                def jobIdRow = sql """select Id from jobs("type"="insert") where Name='${jobName}'"""
                assert jobIdRow.size() == 1
                def jobId = jobIdRow.get(0).get(0).toString()

                // Wait until tasks have committed AND splitter is still mid-flight.
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

                def rowsBefore = sql """SELECT COUNT(*) FROM ${currentDb}.${dorisTable}"""
                def chunkLenBefore = (sql """SELECT json_length(chunk_list)
                                            FROM internal.__internal_schema.streaming_job_meta
                                            WHERE job_id='${jobId}'""").get(0).get(0) as int
                log.info("before restart: rows=${rowsBefore} chunkLen=${chunkLenBefore}")
                assert rowsBefore.get(0).get(0) < totalRows :
                        "TVF snapshot finished too fast (${rowsBefore.get(0).get(0)} rows) — restart wouldn't be mid-snapshot"
                assert chunkLenBefore < expectedChunks :
                        "splitter already finished (chunk_list=${chunkLenBefore}/${expectedChunks}) — restart can't exercise replay"

                cluster.restartFrontends()
                sleep(30000)
                context.reconnectFe()

                // After restart, snapshot must complete — verifies cachedSyncTables was restored
                // by replayIfNeed and advanceSplitsIfNeed continues fetching beyond the chunks
                // already in the meta table at restart time.
                try {
                    Awaitility.await().atMost(600, SECONDS)
                            .pollInterval(2, SECONDS).until(
                            {
                                def cnt = sql """SELECT COUNT(*) FROM ${currentDb}.${dorisTable}"""
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

                def distinctCnt = sql """SELECT COUNT(DISTINCT id) FROM ${currentDb}.${dorisTable}"""
                assert distinctCnt.get(0).get(0) == totalRows :
                        "row count matches but DISTINCT differs — chunks re-cut on resume"

                // Final chunk_list must have grown past pre-restart size — direct evidence that
                // advanceSplits continued fetching after restart (not truncated to meta snapshot).
                def chunkLenAfter = (sql """SELECT json_length(chunk_list)
                                           FROM internal.__internal_schema.streaming_job_meta
                                           WHERE job_id='${jobId}'""").get(0).get(0) as int
                log.info("after restart: chunkLen=${chunkLenAfter}")
                // Uneven sampler re-runs after restart, so total chunk count is non-deterministic.
                assert chunkLenAfter > chunkLenBefore :
                        "chunk_list did not grow after restart (${chunkLenBefore} -> ${chunkLenAfter}) — " +
                        "snapshot was truncated to pre-restart state"

                def status = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
                assert status.get(0).get(0) == "RUNNING"

                // Binlog phase after resume must still work.
                connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
                    sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (id, name, age) VALUES ('k_99999', 'post_restart', 999);"""
                }
                try {
                    Awaitility.await().atMost(120, SECONDS)
                            .pollInterval(2, SECONDS).until(
                            {
                                def r = sql """SELECT count(*) FROM ${currentDb}.${dorisTable} WHERE id='k_99999'"""
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
