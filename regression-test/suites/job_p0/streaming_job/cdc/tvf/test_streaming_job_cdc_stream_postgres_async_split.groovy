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

// cdc_stream TVF path + uneven splitter (VARCHAR PK). With batch_size shrunk so
// the splitter is forced to multiple RPCs, streaming_job_meta.chunk_list must grow
// in steps — one-shot splitting would land the full list in a single UPSERT.
suite("test_streaming_job_cdc_stream_postgres_async_split",
        "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_job_cdc_stream_postgres_async_split"
    def currentDb = (sql "select database()")[0][0]
    def dorisTable = "test_streaming_job_cdc_stream_postgres_async_split_tbl"
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"
    def pgTable = "test_streaming_job_cdc_stream_postgres_async_split_src"
    def totalRows = 500
    int expectedChunks = (int) Math.ceil(totalRows / 5.0)

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
        String pg_port = context.config.otherConfigs.get("pg_14_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        // Shrink batch_size so the splitter must make multiple RPCs — otherwise the
        // default 100 lets 100 chunks fit in one shot and the "in batches" assertion
        // becomes vacuous on the uneven path too.
        def origBatchSize = sql("""ADMIN SHOW FRONTEND CONFIG LIKE 'streaming_cdc_fetch_splits_batch_size'""")
                .get(0).get(1)
        sql """ADMIN SET FRONTEND CONFIG ('streaming_cdc_fetch_splits_batch_size' = '5')"""

        try {
            // VARCHAR PK bypasses isEvenlySplitColumn() -> uneven splitter path.
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
                    "snapshot_parallelism" = "2"
                )
            """

            def jobIdRow = sql """select Id from jobs("type"="insert") where Name='${jobName}'"""
            assert jobIdRow.size() == 1
            def jobId = jobIdRow.get(0).get(0).toString()

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
                    } catch (Throwable ignored) { /* meta table may not have row yet */ }
                    try { Thread.sleep(150) } catch (InterruptedException ie) { break }
                }
            }

            try {
                Awaitility.await().atMost(600, SECONDS)
                        .pollInterval(2, SECONDS).until(
                        {
                            def cnt = sql """SELECT COUNT(*) FROM ${currentDb}.${dorisTable}"""
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
                    "TVF uneven splitting should produce chunk_list in batches (>=3 distinct lengths)," +
                    " saw only ${lengthsSeen.toSorted()} — implies one-shot splitting"

            def doneDistinct = sql """SELECT COUNT(DISTINCT id) FROM ${currentDb}.${dorisTable}"""
            assert doneDistinct.get(0).get(0) == totalRows :
                    "DISTINCT pk count ${doneDistinct.get(0).get(0)} != ${totalRows} — chunks may have re-cut"

            // Verify binlog phase: INSERT/UPDATE/DELETE all propagate after snapshot.
            connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
                sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (id, name, age) VALUES ('k_99999', 'incr', 999);"""
                sql """UPDATE ${pgDB}.${pgSchema}.${pgTable} SET age = 8888 WHERE id = 'k_00001';"""
                sql """DELETE FROM ${pgDB}.${pgSchema}.${pgTable} WHERE id = 'k_00002';"""
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
                                FROM ${currentDb}.${dorisTable}"""
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
