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

// MySQL uneven splitter path — VARCHAR PK forces splitOneUnevenlySizedChunk on the
// MySQL side (separate impl from PG: MySqlSourceReader.resolveSplitKeyClass lives
// in cdc_client). batch_size shrunk so chunk_list growth is observably stepped.
suite("test_streaming_mysql_job_async_split_uneven",
        "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_async_split_uneven_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "user_info_mysql_async_split_uneven"
    def mysqlDb = "test_cdc_db"
    def totalRows = 500
    int expectedChunks = (int) Math.ceil(totalRows / 5.0)

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

        def origBatchSize = sql("""ADMIN SHOW FRONTEND CONFIG LIKE 'streaming_cdc_fetch_splits_batch_size'""")
                .get(0).get(1)
        sql """ADMIN SET FRONTEND CONFIG ('streaming_cdc_fetch_splits_batch_size' = '5')"""

        try {
            connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
                sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
                sql """DROP TABLE IF EXISTS ${mysqlDb}.${table1}"""
                sql """CREATE TABLE ${mysqlDb}.${table1} (
                      `id` varchar(20) NOT NULL,
                      `name` varchar(200),
                      `age` int,
                      PRIMARY KEY (`id`)
                    ) ENGINE=InnoDB"""
                StringBuilder sb = new StringBuilder()
                sb.append("INSERT INTO ${mysqlDb}.${table1} (id, name, age) VALUES ")
                for (int i = 1; i <= totalRows; i++) {
                    if (i > 1) sb.append(", ")
                    String key = "k_" + String.format("%05d", i)
                    sb.append("('${key}', 'name_${i}', ${i})")
                }
                sql sb.toString()
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

            def lengthsSeen = new CopyOnWriteArraySet<Integer>()
            def samplerStop = new AtomicBoolean(false)
            def sampler = Thread.start {
                while (!samplerStop.get()) {
                    try {
                        def r = sql """SELECT json_length(chunk_list)
                                       FROM internal.__internal_schema.streaming_job_meta
                                       WHERE job_id='${jobId}'"""
                        if (r.size() > 0 && r.get(0).get(0) != null) {
                            lengthsSeen.add(r.get(0).get(0) as int)
                        }
                    } catch (Throwable ignored) { /* meta row not visible yet */ }
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

            assert lengthsSeen.contains(expectedChunks) :
                    "chunk_list never reached the full ${expectedChunks} chunks, observed: ${lengthsSeen.toSorted()}"
            assert lengthsSeen.size() >= 3 :
                    "MySQL uneven splitting should produce chunk_list in batches (>=3 distinct lengths)," +
                    " saw only ${lengthsSeen.toSorted()} — implies one-shot splitting"

            def doneDistinct = sql """SELECT COUNT(DISTINCT id) FROM ${currentDb}.${table1}"""
            assert doneDistinct.get(0).get(0) == totalRows :
                    "DISTINCT pk count ${doneDistinct.get(0).get(0)} != ${totalRows} — chunks may have re-cut"

            def loadStat0 = parseJson(sql("""
                select loadStatistic from jobs("type"="insert") where Name='${jobName}'
            """).get(0).get(0))
            log.info("loadStat after MySQL uneven snapshot: ${loadStat0}")
            assert loadStat0.scannedRows == totalRows

            // Binlog phase: INSERT/UPDATE/DELETE must all propagate after the uneven snapshot.
            connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
                sql """INSERT INTO ${mysqlDb}.${table1} (id, name, age) VALUES ('k_99999', 'incr', 999);"""
                sql """UPDATE ${mysqlDb}.${table1} SET age = 8888 WHERE id = 'k_00001';"""
                sql """DELETE FROM ${mysqlDb}.${table1} WHERE id = 'k_00002';"""
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
