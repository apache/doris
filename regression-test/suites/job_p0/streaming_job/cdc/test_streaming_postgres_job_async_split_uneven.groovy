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

import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.atomic.AtomicBoolean

import static java.util.concurrent.TimeUnit.SECONDS

// Exercises the uneven splitter path (VARCHAR PK bypasses isEvenlySplitColumn() and
// goes through splitOneUnevenlySizedChunk -> per-chunk MAX(...) probe SQL) AND verifies
// that splitting actually happens in batches: with batch_size shrunk to 5, the
// streaming_job_meta.chunk_list must grow incrementally (multiple distinct lengths
// observed during snapshot) — a single-shot synchronous splitter would write the
// full list in one UPSERT and we'd only ever see one length.
suite("test_streaming_postgres_job_async_split_uneven", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_async_split_uneven_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "user_info_pg_async_split_uneven"
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"
    def totalRows = 200
    int expectedChunks = (int) Math.ceil(totalRows / 5.0)   // 100 chunks at split_size=5

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port = context.config.otherConfigs.get("pg_14_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        // Shrink the FE-side batch size so 100 chunks need ~20 RPCs instead of 1 — needed
        // to make the "in batches" assertion observable. Default 100 would otherwise let
        // cdc_client return all 100 chunks in a single RPC even on the uneven path.
        def origBatchSize = sql("""ADMIN SHOW FRONTEND CONFIG LIKE 'streaming_cdc_fetch_splits_batch_size'""")
                .get(0).get(1)
        sql """ADMIN SET FRONTEND CONFIG ('streaming_cdc_fetch_splits_batch_size' = '5')"""

        try {
            connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
                sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
                sql """CREATE TABLE ${pgDB}.${pgSchema}.${table1} (
                      "id" varchar(20) PRIMARY KEY,
                      "name" varchar(200),
                      "age" int4
                    )"""
                StringBuilder sb = new StringBuilder()
                sb.append("INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, name, age) VALUES ")
                for (int i = 1; i <= totalRows; i++) {
                    if (i > 1) sb.append(", ")
                    String key = "k_" + String.format("%05d", i)
                    sb.append("('${key}', 'name_${i}', ${i})")
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
                        "snapshot_parallelism" = "2"
                    )
                    TO DATABASE ${currentDb} (
                      "table.create.properties.replication_num" = "1"
                    )
                """

            def jobIdRow = sql """select Id from jobs("type"="insert") where Name='${jobName}'"""
            assert jobIdRow.size() == 1
            def jobId = jobIdRow.get(0).get(0).toString()

            // Background sampler: poll streaming_job_meta.chunk_list length every 150ms while
            // the splitter runs. With batch_size=5 and ~20 RPCs the chunk_list grows in steps,
            // so we should observe several distinct lengths.
            def lengthsSeen = new CopyOnWriteArraySet<Integer>()
            def samplerStop = new AtomicBoolean(false)
            def sampler = Thread.start {
                while (!samplerStop.get()) {
                    try {
                        def r = sql """SELECT json_length(chunk_list)
                                       FROM internal.__internal_schema.streaming_job_meta
                                       WHERE job_id='${jobId}'"""
                        if (r.size() > 0 && r.get(0).get(0) != null) {
                            int len = r.get(0).get(0) as int
                            log.info("sampler: chunk_list length=${len}")
                            lengthsSeen.add(len)
                        }
                    } catch (Throwable ignored) { /* table may not exist yet; retry */ }
                    try { Thread.sleep(150) } catch (InterruptedException ie) { break }
                }
            }

            try {
                Awaitility.await().atMost(600, SECONDS)
                        .pollInterval(2, SECONDS).until(
                        {
                            def cnt = sql """SELECT COUNT(*) FROM ${currentDb}.${table1}"""
                            log.info("doris row count: ${cnt}")
                            cnt.size() == 1 && cnt.get(0).get(0) == totalRows
                        }
                )
            } catch (Exception ex) {
                samplerStop.set(true)
                def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
                def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
                log.info("show job: " + showjob)
                log.info("show task: " + showtask)
                throw ex
            }
            samplerStop.set(true)
            sampler.join(5000)
            log.info("chunk_list lengths observed during snapshot: ${lengthsSeen.toSorted()}")

            // >=3 distinct lengths = chunk_list grew in batched UPSERTs; one-shot would show only one.
            assert lengthsSeen.size() >= 3 :
                    "uneven splitting should produce chunk_list in batches (>=3 distinct lengths)," +
                    " saw only ${lengthsSeen.toSorted()} — implies one-shot splitting"

            // DISTINCT count guards against duplicates from chunk re-cut / RPC retry.
            def doneDistinct = sql """SELECT COUNT(DISTINCT id) FROM ${currentDb}.${table1}"""
            assert doneDistinct.get(0).get(0) == totalRows :
                    "DISTINCT pk count ${doneDistinct.get(0).get(0)} != ${totalRows} — chunks may have re-cut"

            def loadStat0 = parseJson(sql("""
                select loadStatistic from jobs("type"="insert") where Name='${jobName}'
            """).get(0).get(0))
            log.info("loadStat after uneven snapshot: ${loadStat0}")
            assert loadStat0.scannedRows == totalRows

            // Verify binlog phase still drains after uneven snapshot.
            connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
                sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, name, age) VALUES ('k_99999', 'incr', 999);"""
                sql """UPDATE ${pgDB}.${pgSchema}.${table1} SET age = 8888 WHERE id = 'k_00001';"""
                sql """DELETE FROM ${pgDB}.${pgSchema}.${table1} WHERE id = 'k_00002';"""
            }

            try {
                Awaitility.await().atMost(120, SECONDS)
                        .pollInterval(2, SECONDS).until(
                        {
                            def res = sql """SELECT
                                    COUNT(*),
                                    SUM(CASE WHEN id='k_00001' AND age=8888 THEN 1 ELSE 0 END),
                                    SUM(CASE WHEN id='k_99999' THEN 1 ELSE 0 END),
                                    SUM(CASE WHEN id='k_00002' THEN 1 ELSE 0 END)
                                FROM ${currentDb}.${table1}"""
                            log.info("binlog state: ${res}")
                            res.size() == 1
                                    && res.get(0).get(0) == totalRows
                                    && res.get(0).get(1) == 1
                                    && res.get(0).get(2) == 1
                                    && res.get(0).get(3) == 0
                        }
                )
            } catch (Exception ex) {
                def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
                log.info("show job: " + showjob)
                throw ex
            }

            def jobInfo = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
            assert jobInfo.get(0).get(0) == "RUNNING"

            sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
            def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name='${jobName}'"""
            assert jobCountRsp.get(0).get(0) == 0
        } finally {
            sql """ADMIN SET FRONTEND CONFIG ('streaming_cdc_fetch_splits_batch_size' = '${origBatchSize}')"""
        }
    }
}
