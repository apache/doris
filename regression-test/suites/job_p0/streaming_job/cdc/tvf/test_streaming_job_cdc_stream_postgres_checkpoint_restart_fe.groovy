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
 * Test FE checkpoint-restart recovery of a cdc_stream TVF streaming job for PostgreSQL.
 *
 * Counterpart of test_streaming_job_cdc_stream_postgres_restart_fe but with the pre-checkpoint
 * journal GC'd before each restart, so recovery cannot rely on EditLog txn replay and must
 * fall back to image-persisted bop/chw written via getPersistInfo (inherited from parent) and
 * read back by JdbcTvfSourceOffsetProvider.restoreFromPersistInfo.
 *
 * edit_log_roll_num=1 finalizes journals on every write so the next checkpoint cycle picks
 * them up; sleeping >= 90s after each commit ensures a checkpoint runs (interval defaults
 * to 60s) and the pre-checkpoint journal is then eligible for GC.
 *
 * Two checkpoint-restart scenarios are covered in sequence:
 *
 * Restart 1 — mid-snapshot:
 *   snapshot_split_size=1 splits 5 pre-existing rows (A1-E1) into 5 separate tasks.
 *   After the first task succeeds and a checkpoint runs, FE is restarted. This exercises
 *   replayIfNeed() with currentOffset == null but chunkHighWatermarkMap restored from image:
 *   remainingSplits is rebuilt from the meta table with chw remap so already-finished splits
 *   are not re-processed.
 *
 * Restart 2 — binlog phase:
 *   After the full snapshot completes and F1/G1 are consumed via binlog, a checkpoint runs
 *   and FE is restarted. This exercises the binlog recovery path where currentOffset == null
 *   but binlogOffsetPersist is restored from image: a BinlogSplit is rebuilt from bop so
 *   the job resumes from the last committed binlog position rather than the initial one.
 *   H1/I1 are then inserted to verify the job continues reading binlog correctly.
 */
suite("test_streaming_job_cdc_stream_postgres_checkpoint_restart_fe",
        "docker,p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_job_cdc_stream_pg_ckpt_restart_fe"
    def options = new ClusterOptions()
    options.setFeNum(1)
    options.cloudMode = null
    // Roll the journal on every write so the checkpoint thread has finalized journals to
    // include; without this, a small steady-state EditLog stays in the active segment and
    // never reaches the checkpoint image, defeating the test.
    options.feConfigs += [
        'edit_log_roll_num=1'
    ]

    docker(options) {
        def currentDb = (sql "select database()")[0][0]
        def dorisTable = "test_streaming_job_cdc_stream_pg_ckpt_restart_fe_tbl"
        def pgDB = "postgres"
        def pgSchema = "cdc_test"
        def pgUser = "postgres"
        def pgPassword = "123456"
        def pgTable = "test_streaming_job_cdc_stream_pg_ckpt_restart_fe_src"

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

            // ── Phase 1: prepare source table with pre-existing snapshot rows ───────────
            connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
                sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${pgTable}"""
                sql """CREATE TABLE ${pgDB}.${pgSchema}.${pgTable} (
                          "name" varchar(200) PRIMARY KEY,
                          "age"  int2
                      )"""
                sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('A1', 1)"""
                sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('B1', 2)"""
                sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('C1', 3)"""
                sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('D1', 4)"""
                sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('E1', 5)"""
            }

            // ── Phase 2: create streaming job (offset=initial, split_size=1 → 5 tasks) ─
            sql """
                CREATE JOB ${jobName}
                ON STREAMING DO INSERT INTO ${currentDb}.${dorisTable} (name, age)
                SELECT name, age FROM cdc_stream(
                    "type"                = "postgres",
                    "jdbc_url"            = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}",
                    "driver_url"          = "${driver_url}",
                    "driver_class"        = "org.postgresql.Driver",
                    "user"                = "${pgUser}",
                    "password"            = "${pgPassword}",
                    "database"            = "${pgDB}",
                    "schema"              = "${pgSchema}",
                    "table"               = "${pgTable}",
                    "offset"              = "initial",
                    "snapshot_split_size" = "1"
                )
            """

            // ── Phase 3: wait for the first snapshot task to succeed, then trigger checkpoint ──
            try {
                Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                    def cnt = sql """select SucceedTaskCount from jobs("type"="insert")
                                     where Name='${jobName}' and ExecuteType='STREAMING'"""
                    log.info("SucceedTaskCount before first ckpt restart: " + cnt)
                    cnt.size() == 1 && (cnt.get(0).get(0) as int) >= 1
                })
            } catch (Exception ex) {
                log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
                log.info("tasks: " + (sql """select * from tasks("type"="insert") where JobName='${jobName}'"""))
                throw ex
            }

            def jobInfoBeforeRestart = sql """
                select status, currentOffset, loadStatistic
                from jobs("type"="insert") where Name='${jobName}'
            """
            log.info("job info before first ckpt restart: " + jobInfoBeforeRestart)
            assert jobInfoBeforeRestart.get(0).get(0) == "RUNNING" :
                    "Job should be RUNNING before first restart, got: ${jobInfoBeforeRestart.get(0).get(0)}"
            def scannedRowsBeforeFirstRestart = parseJson(jobInfoBeforeRestart.get(0).get(2) as String).scannedRows as long
            log.info("scannedRows before first ckpt restart: " + scannedRowsBeforeFirstRestart)

            // Wait >= 90s so the checkpoint thread (60s interval) runs at least once after our
            // last commit, then GC's the pre-checkpoint journal. Subsequent FE restart can no
            // longer recover via journal txn replay and must rely on image-persisted chw.
            log.info("Waiting 90s for checkpoint to run before first FE restart...")
            sleep(90000)

            // ── Phase 4: restart FE (mid-snapshot, post-checkpoint) ──────────────────
            cluster.restartFrontends()
            sleep(60000)
            context.reconnectFe()

            // ── Phase 5: verify job recovers and finishedSplits are not re-processed ───
            try {
                Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                    def status = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
                    log.info("job status after first ckpt restart: " + status)
                    status.size() == 1 && (status.get(0).get(0) == "RUNNING" || status.get(0).get(0) == "PENDING")
                })
            } catch (Exception ex) {
                log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
                throw ex
            }

            def jobInfoAfterRestart1 = sql """select currentOffset, loadStatistic from jobs("type"="insert") where Name='${jobName}'"""
            log.info("job info after first ckpt restart: " + jobInfoAfterRestart1)
            def scannedRowsAfterFirstRestart = parseJson(jobInfoAfterRestart1.get(0).get(1) as String).scannedRows as long
            assert scannedRowsAfterFirstRestart >= scannedRowsBeforeFirstRestart :
                    "scannedRows should not reset after first ckpt restart: before=${scannedRowsBeforeFirstRestart}, after=${scannedRowsAfterFirstRestart}"

            // ── Phase 6: wait for full snapshot to complete and assert exact row count ─
            try {
                Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                    def rows = sql """SELECT count(1) FROM ${currentDb}.${dorisTable}
                                      WHERE name IN ('A1', 'B1', 'C1', 'D1', 'E1')"""
                    log.info("snapshot rows after first ckpt restart: " + rows)
                    (rows.get(0).get(0) as int) >= 5
                })
            } catch (Exception ex) {
                log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
                log.info("tasks: " + (sql """select * from tasks("type"="insert") where JobName='${jobName}'"""))
                throw ex
            }

            // DUPLICATE KEY table: re-processed snapshot splits would inflate the count above 5.
            def snapshotCount = sql """SELECT count(1) FROM ${currentDb}.${dorisTable}
                                       WHERE name IN ('A1', 'B1', 'C1', 'D1', 'E1')"""
            assert (snapshotCount.get(0).get(0) as int) == 5 :
                    "Snapshot rows should be exactly 5 after mid-snapshot ckpt restart (no re-processing), got: ${snapshotCount.get(0).get(0)}"

            // ── Phase 7: insert F1/G1 and wait for binlog to consume them ───────────────
            connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
                sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('F1', 6)"""
                sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('G1', 7)"""
            }

            try {
                Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                    def rows = sql """SELECT count(1) FROM ${currentDb}.${dorisTable} WHERE name IN ('F1', 'G1')"""
                    log.info("binlog rows before second ckpt restart: " + rows)
                    (rows.get(0).get(0) as int) == 2
                })
            } catch (Exception ex) {
                log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
                log.info("tasks: " + (sql """select * from tasks("type"="insert") where JobName='${jobName}'"""))
                throw ex
            }

            // ── Phase 8: restart FE in binlog phase, post-checkpoint ──────────────────
            def jobInfoBeforeSecondRestart = sql """
                select status, currentOffset, loadStatistic
                from jobs("type"="insert") where Name='${jobName}'
            """
            log.info("job info before second ckpt restart (binlog phase): " + jobInfoBeforeSecondRestart)
            def offsetBeforeSecondRestart = jobInfoBeforeSecondRestart.get(0).get(1) as String
            assert offsetBeforeSecondRestart != null && offsetBeforeSecondRestart.contains("binlog-split") :
                    "currentOffset should be in binlog state before second restart, got: ${offsetBeforeSecondRestart}"
            def scannedRowsBefore = parseJson(jobInfoBeforeSecondRestart.get(0).get(2) as String).scannedRows as long
            log.info("scannedRows before second ckpt restart: " + scannedRowsBefore)

            log.info("Waiting 90s for checkpoint to run before second FE restart...")
            sleep(90000)

            cluster.restartFrontends()
            sleep(60000)
            context.reconnectFe()

            // ── Phase 9: verify job recovers and binlog offset did not regress ─────────
            try {
                Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                    def status = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
                    log.info("job status after second ckpt restart: " + status)
                    status.size() == 1 && (status.get(0).get(0) == "RUNNING" || status.get(0).get(0) == "PENDING")
                })
            } catch (Exception ex) {
                log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
                throw ex
            }

            def jobInfoAfterRestart2 = sql """select currentOffset, loadStatistic from jobs("type"="insert") where Name='${jobName}'"""
            log.info("job info after second ckpt restart: " + jobInfoAfterRestart2)

            // Binlog offset advances with heartbeats; assert correctness via row counts.
            // If binlog offset regressed (bop not restored from image), F1/G1 would be
            // re-inserted and count > 2.
            def binlogCountAfterRestart = sql """SELECT count(1) FROM ${currentDb}.${dorisTable}
                                                 WHERE name IN ('F1', 'G1')"""
            assert (binlogCountAfterRestart.get(0).get(0) as int) == 2 :
                    "Binlog rows F1/G1 should be exactly 2 after binlog-phase ckpt restart (no re-processing), got: ${binlogCountAfterRestart.get(0).get(0)}"

            // ── Phase 10: insert H1, I1 and verify binlog still progresses post-restart ─
            connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
                sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('H1', 8)"""
                sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('I1', 9)"""
            }

            try {
                Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                    def rows = sql """SELECT count(1) FROM ${currentDb}.${dorisTable} WHERE name IN ('H1', 'I1')"""
                    log.info("binlog rows after second ckpt restart: " + rows)
                    (rows.get(0).get(0) as int) == 2
                })
            } catch (Exception ex) {
                log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
                log.info("tasks: " + (sql """select * from tasks("type"="insert") where JobName='${jobName}'"""))
                throw ex
            }

            qt_final_data """ SELECT * FROM ${currentDb}.${dorisTable} ORDER BY name """

            // ── Phase 11: final assertions ────────────────────────────────────────────
            def jobInfoFinal = sql """
                select status, FailedTaskCount, ErrorMsg, currentOffset, loadStatistic
                from jobs("type"="insert") where Name='${jobName}'
            """
            log.info("job info final: " + jobInfoFinal)
            assert jobInfoFinal.get(0).get(0) == "RUNNING" : "Job should be RUNNING at end"
            assert (jobInfoFinal.get(0).get(1) as int) == 0 : "FailedTaskCount should be 0"

            def scannedRowsAfter = parseJson(jobInfoFinal.get(0).get(4) as String).scannedRows as long
            log.info("scannedRows after second ckpt restart: " + scannedRowsAfter)
            assert scannedRowsAfter >= scannedRowsBefore :
                    "scannedRows should not reset after FE ckpt restart: before=${scannedRowsBefore}, after=${scannedRowsAfter}"

            sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
            sql """drop table if exists ${currentDb}.${dorisTable} force"""
        }
    }
}
