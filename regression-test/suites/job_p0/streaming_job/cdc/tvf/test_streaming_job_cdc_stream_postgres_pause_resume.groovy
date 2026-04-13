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

/**
 * Test pause and resume of a streaming INSERT job using cdc_stream TVF for PostgreSQL.
 *
 * Scenario:
 *   1. Snapshot phase: pre-existing rows (A1, B1) are synced via full snapshot.
 *   2. Binlog phase begins: INSERT (C1, D1) are applied; job enters steady binlog state.
 *   3. Job is paused; verify status stays PAUSED and currentOffset / endOffset are non-empty.
 *   4. While paused: INSERT (E1, F1) into the PG source table.
 *   5. Job is resumed; verify status transitions to RUNNING.
 *   6. Verify all rows inserted before and during pause eventually appear in Doris (no data loss).
 *   7. Verify FailedTaskCount == 0 and no error message after resume.
 */
suite("test_streaming_job_cdc_stream_postgres_pause_resume", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_job_cdc_stream_pg_pause_resume"
    def currentDb = (sql "select database()")[0][0]
    def dorisTable = "test_streaming_job_cdc_stream_pg_pause_resume_tbl"
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"
    def pgTable = "test_streaming_job_cdc_stream_pg_pause_resume_src"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${dorisTable} force"""

    sql """
        CREATE TABLE IF NOT EXISTS ${currentDb}.${dorisTable} (
            `name` varchar(200) NULL,
            `age`  int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`name`)
        DISTRIBUTED BY HASH(`name`) BUCKETS AUTO
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port = context.config.otherConfigs.get("pg_14_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        // ── Phase 1: prepare source table with pre-existing snapshot rows ──────────────
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${pgTable}"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${pgTable} (
                      "name" varchar(200) PRIMARY KEY,
                      "age"  int2
                  )"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('A1', 1)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('B1', 2)"""
        }

        // ── Phase 2: create streaming job (offset=initial → snapshot then binlog) ─────
        sql """
            CREATE JOB ${jobName}
            ON STREAMING DO INSERT INTO ${currentDb}.${dorisTable} (name, age)
            SELECT name, age FROM cdc_stream(
                "type"         = "postgres",
                "jdbc_url"     = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}",
                "driver_url"   = "${driver_url}",
                "driver_class" = "org.postgresql.Driver",
                "user"         = "${pgUser}",
                "password"     = "${pgPassword}",
                "database"     = "${pgDB}",
                "schema"       = "${pgSchema}",
                "table"        = "${pgTable}",
                "offset"       = "initial"
            )
        """

        // wait for snapshot tasks to complete (A1 and B1 visible)
        try {
            Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                def rows = sql """SELECT count(1) FROM ${currentDb}.${dorisTable} WHERE name IN ('A1', 'B1')"""
                log.info("snapshot rows: " + rows)
                (rows.get(0).get(0) as int) == 2
            })
        } catch (Exception ex) {
            log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            log.info("tasks: " + (sql """select * from tasks("type"="insert") where JobName='${jobName}'"""))
            throw ex
        }

        qt_snapshot_data """ SELECT * FROM ${currentDb}.${dorisTable} ORDER BY name """

        // ── Phase 3: insert incremental rows; wait for binlog phase to catch up ────────
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('C1', 3)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('D1', 4)"""
            def xminResult = sql """SELECT xmin, xmax , * FROM ${pgDB}.${pgSchema}.${pgTable} WHERE name = 'D1'; """
            log.info("xminResult: " + xminResult)
        }

        try {
            Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                def rows = sql """SELECT count(1) FROM ${currentDb}.${dorisTable} WHERE name IN ('C1', 'D1')"""
                log.info("binlog rows before pause: " + rows)
                (rows.get(0).get(0) as int) == 2
            })
        } catch (Exception ex) {
            log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            log.info("tasks: " + (sql """select * from tasks("type"="insert") where JobName='${jobName}'"""))
            throw ex
        }

        // ── Phase 4: verify offset display in binlog state ────────────────────────────
        def jobInfoBeforePause = sql """select currentOffset, endOffset, FailedTaskCount, ErrorMsg
                                        from jobs("type"="insert") where Name='${jobName}'"""
        log.info("job info before pause: " + jobInfoBeforePause)

        def currentOffsetBeforePause = jobInfoBeforePause.get(0).get(0) as String
        def failedCountBeforePause   = jobInfoBeforePause.get(0).get(2)
        // currentOffset should be non-empty and contain the binlog split id
        assert currentOffsetBeforePause != null && !currentOffsetBeforePause.isEmpty() :
                "currentOffset should be non-empty in binlog state, got: ${currentOffsetBeforePause}"
        assert currentOffsetBeforePause.contains("binlog-split") :
                "currentOffset should contain 'binlog-split' in binlog state, got: ${currentOffsetBeforePause}"
        assert (failedCountBeforePause as int) == 0 : "FailedTaskCount should be 0 before pause"

        // ── Phase 5: pause the job ─────────────────────────────────────────────────────
        sql """PAUSE JOB where jobname = '${jobName}'"""

        // verify job transitions to PAUSED
        try {
            Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).until({
                def status = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
                log.info("job status after pause: " + status)
                status.size() == 1 && status.get(0).get(0) == "PAUSED"
            })
        } catch (Exception ex) {
            log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            throw ex
        }

        // verify PAUSED state is stable (not auto-resumed) for several seconds
        sleep(5000)
        def pausedStatus = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
        assert pausedStatus.get(0).get(0) == "PAUSED" : "Job should remain PAUSED, got: ${pausedStatus.get(0).get(0)}"

        // verify offset is still reported correctly while paused
        def jobInfoWhilePaused = sql """select currentOffset, endOffset from jobs("type"="insert") where Name='${jobName}'"""
        log.info("job info while paused: " + jobInfoWhilePaused)
        def currentOffsetWhilePaused = jobInfoWhilePaused.get(0).get(0) as String
        assert currentOffsetWhilePaused != null && !currentOffsetWhilePaused.isEmpty() :
                "currentOffset should still be non-empty while paused"

        // ── Phase 6: DML while job is paused ─────────────────────────────────────────
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('E1', 5)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('F1', 6)"""
            def xminResult = sql """SELECT xmin, xmax , * FROM ${pgDB}.${pgSchema}.${pgTable} WHERE name = 'F1'; """
            log.info("xminResult: " + xminResult)
        }

        // ── Phase 7: resume the job ────────────────────────────────────────────────────
        sql """RESUME JOB where jobname = '${jobName}'"""

        // verify job transitions back to RUNNING
        try {
            Awaitility.await().atMost(60, SECONDS).pollInterval(1, SECONDS).until({
                def status = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
                log.info("job status after resume: " + status)
                status.size() == 1 && status.get(0).get(0) == "RUNNING"
            })
        } catch (Exception ex) {
            log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            throw ex
        }

        // ── Phase 8: wait for rows inserted while paused to appear ────────────────────
        try {
            Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                def rows = sql """SELECT count(1) FROM ${currentDb}.${dorisTable} WHERE name IN ('E1', 'F1')"""
                log.info("rows inserted while paused: " + rows)
                (rows.get(0).get(0) as int) == 2
            })
        } catch (Exception ex) {
            log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            log.info("tasks: " + (sql """select * from tasks("type"="insert") where JobName='${jobName}'"""))
            throw ex
        }

        // ── Phase 9: assert final correctness ─────────────────────────────────────────
        qt_final_data """ SELECT * FROM ${currentDb}.${dorisTable} ORDER BY name """

        def jobInfoAfterResume = sql """select status, currentOffset, FailedTaskCount, ErrorMsg
                                        from jobs("type"="insert") where Name='${jobName}'"""
        log.info("job info after resume: " + jobInfoAfterResume)
        assert jobInfoAfterResume.get(0).get(0) == "RUNNING" : "Job should be RUNNING after resume"
        def currentOffsetAfterResume = jobInfoAfterResume.get(0).get(1) as String
        assert currentOffsetAfterResume != null && !currentOffsetAfterResume.isEmpty() :
                "currentOffset should be non-empty after resume"
        assert (jobInfoAfterResume.get(0).get(2) as int) == 0 : "FailedTaskCount should be 0 after resume"
        def errorMsgAfterResume = jobInfoAfterResume.get(0).get(3) as String
        assert errorMsgAfterResume == null || errorMsgAfterResume.isEmpty() :
                "ErrorMsg should be empty after successful resume, got: ${errorMsgAfterResume}"

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        sql """drop table if exists ${currentDb}.${dorisTable} force"""
    }
}
