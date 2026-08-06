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

// Restart FE AFTER snapshot fully transitioned to binlog. Replay goes through the
// binlogOffsetPersist non-empty branch, which leaves finishedSplits/remainingSplits
// empty. Without the noMoreSplits() guard on currentOffset=BinlogSplit, the scheduler
// next tick would (mis)compute every cachedSyncTables entry as untouched and call
// advanceSplits() to re-cut snapshot chunks, duplicating rows.
suite("test_streaming_postgres_job_binlog_restart_fe",
        "docker,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_binlog_restart_fe"
    def options = new ClusterOptions()
    options.setFeNum(1)
    options.cloudMode = null

    docker(options) {
        def currentDb = (sql "select database()")[0][0]
        def table1 = "user_info_pg_binlog_restart_fe"
        def pgDB = "postgres"
        def pgSchema = "cdc_test"
        def pgUser = "postgres"
        def pgPassword = "123456"
        def totalRows = 50

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
                        "snapshot_split_size" = "10",
                        "snapshot_parallelism" = "1"
                    )
                    TO DATABASE ${currentDb} (
                      "table.create.properties.replication_num" = "1"
                    )
                """

            def jobIdRow = sql """select Id from jobs("type"="insert") where Name='${jobName}'"""
            assert jobIdRow.size() == 1
            def jobId = jobIdRow.get(0).get(0).toString()

            // Step 1: wait until snapshot fully drained into Doris.
            Awaitility.await().atMost(180, SECONDS).pollInterval(1, SECONDS).until(
                    {
                        def cnt = sql """SELECT COUNT(*) FROM ${currentDb}.${table1}"""
                        cnt.size() == 1 && cnt.get(0).get(0) == totalRows
                    }
            )

            // Step 2: insert one binlog row so updateOffset commits a BinlogSplit ->
            // binlogOffsetPersist gets written to EditLog. This is the precondition
            // for the replay path we want to exercise.
            connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
                sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, name) VALUES ('k_99999', 'pre_restart')"""
            }
            Awaitility.await().atMost(120, SECONDS).pollInterval(1, SECONDS).until(
                    {
                        def r = sql """SELECT count(*) FROM ${currentDb}.${table1} WHERE id='k_99999'"""
                        r.size() == 1 && r.get(0).get(0) == 1
                    }
            )

            // Snapshot of state before restart — used to detect re-cut / re-read.
            def rowsBefore = (sql """SELECT COUNT(*) FROM ${currentDb}.${table1}""").get(0).get(0)
            int chunkLenBefore = -1
            String chunkListBefore = null
            try {
                def chunkRow = sql """SELECT json_length(chunk_list), chunk_list
                                      FROM internal.__internal_schema.streaming_job_meta
                                      WHERE job_id='${jobId}'"""
                if (chunkRow.size() > 0 && chunkRow.get(0).get(0) != null) {
                    chunkLenBefore = chunkRow.get(0).get(0) as int
                    chunkListBefore = chunkRow.get(0).get(1).toString()
                }
            } catch (Throwable ignored) { /* meta table optional */ }
            log.info("before restart: rows=${rowsBefore} chunkLen=${chunkLenBefore}")

            // Step 3: restart FE, then give scheduler several ticks to misbehave (if buggy).
            cluster.restartFrontends()
            sleep(30000)
            context.reconnectFe()
            sleep(15000)

            // Step 4: state must be stable — no extra rows, no duplicates, chunk_list unchanged.
            // chunk_list content (not size) is the key signal: a buggy advanceSplits in
            // binlog phase re-cuts chunks and UPSERTs them under the same (job_id, split_id)
            // keys, so size stays the same but the rows themselves get rewritten.
            def rowsAfter = (sql """SELECT COUNT(*) FROM ${currentDb}.${table1}""").get(0).get(0)
            def distinctAfter = (sql """SELECT COUNT(DISTINCT id) FROM ${currentDb}.${table1}""").get(0).get(0)
            int chunkLenAfter = -1
            String chunkListAfter = null
            try {
                def chunkRow = sql """SELECT json_length(chunk_list), chunk_list
                                      FROM internal.__internal_schema.streaming_job_meta
                                      WHERE job_id='${jobId}'"""
                if (chunkRow.size() > 0 && chunkRow.get(0).get(0) != null) {
                    chunkLenAfter = chunkRow.get(0).get(0) as int
                    chunkListAfter = chunkRow.get(0).get(1).toString()
                }
            } catch (Throwable ignored) { /* meta table optional */ }
            log.info("after restart: rows=${rowsAfter} distinct=${distinctAfter} chunkLen=${chunkLenAfter}")

            assert rowsAfter == rowsBefore :
                    "row count grew after restart (${rowsBefore} -> ${rowsAfter}) — " +
                    "scheduler re-cut snapshot in binlog phase"
            assert distinctAfter == rowsAfter :
                    "duplicate ids after restart (rows=${rowsAfter} distinct=${distinctAfter})"
            if (chunkLenBefore >= 0) {
                assert chunkLenAfter == chunkLenBefore :
                        "chunk_list grew after restart (${chunkLenBefore} -> ${chunkLenAfter}) — " +
                        "advanceSplits ran in binlog phase"
            }
            if (chunkListBefore != null && chunkListAfter != null) {
                assert chunkListBefore == chunkListAfter :
                        "chunk_list content rewritten after restart — advanceSplits ran in binlog phase.\n" +
                        "  before: ${chunkListBefore}\n" +
                        "  after:  ${chunkListAfter}"
            }

            // Step 5: binlog must still work after restart.
            connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
                sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, name) VALUES ('k_99998', 'post_restart')"""
            }
            Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until(
                    {
                        def r = sql """SELECT count(*) FROM ${currentDb}.${table1} WHERE id='k_99998'"""
                        r.size() == 1 && r.get(0).get(0) == 1
                    }
            )

            def status = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
            assert status.get(0).get(0) == "RUNNING"

            sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        }
    }
}
