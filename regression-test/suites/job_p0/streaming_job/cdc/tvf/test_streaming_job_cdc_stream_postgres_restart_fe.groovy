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
 * Test FE restart recovery of a cdc_stream TVF streaming job for PostgreSQL.
 *
 * Two restart scenarios are covered in sequence:
 *
 * Restart 1 — mid-snapshot:
 *   snapshot_split_size=1 splits 5 pre-existing rows (A1-E1) into 5 separate tasks.
 *   FE is restarted after the first task succeeds (SucceedTaskCount >= 1) but before
 *   all splits complete. This exercises JdbcTvfSourceOffsetProvider.replayIfNeed()
 *   when currentOffset.snapshotSplit() == true: remainingSplits is rebuilt from the
 *   meta table using chunkHighWatermarkMap recovered via txn replay, so already-finished
 *   splits are not re-processed.
 *
 * Restart 2 — binlog phase:
 *   After the full snapshot completes and F1/G1 are consumed via binlog, FE is restarted
 *   again. This exercises the binlog recovery path in replayIfNeed() where
 *   currentOffset.snapshotSplit() == false: currentOffset is restored directly from
 *   txn replay without any remainingSplits rebuild.
 *   H1/I1 are then inserted to verify the job continues reading binlog correctly.
 */
suite("test_streaming_job_cdc_stream_postgres_restart_fe",
        "docker,p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_job_cdc_stream_pg_restart_fe"
    def options = new ClusterOptions()
    options.setFeNum(1)
    options.cloudMode = null

    docker(options) {
        def currentDb = (sql "select database()")[0][0]
        def dorisTable = "test_streaming_job_cdc_stream_pg_restart_fe_tbl"
        def pgDB = "postgres"
        def pgSchema = "cdc_test"
        def pgUser = "postgres"
        def pgPassword = "123456"
        def pgTable = "test_streaming_job_cdc_stream_pg_restart_fe_src"

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
            // Insert 5 rows so that snapshot_split_size=1 reliably produces multiple splits.
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

            // ── Phase 3: wait for the first snapshot task to succeed, then restart FE ──
            // snapshot_split_size=1 splits 5 rows (A1-E1) into 5 separate tasks; we restart
            // after the first one completes to exercise mid-snapshot FE recovery.
            try {
                Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                    def cnt = sql """select SucceedTaskCount from jobs("type"="insert")
                                     where Name='${jobName}' and ExecuteType='STREAMING'"""
                    log.info("SucceedTaskCount before restart: " + cnt)
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
            log.info("job info before first FE restart: " + jobInfoBeforeRestart)
            assert jobInfoBeforeRestart.get(0).get(0) == "RUNNING" :
                    "Job should be RUNNING before first restart, got: ${jobInfoBeforeRestart.get(0).get(0)}"
            assert jobInfoBeforeRestart.get(0).get(1) != null && !jobInfoBeforeRestart.get(0).get(1).isEmpty() :
                    "currentOffset should be non-empty before first restart"
            def scannedRowsBeforeFirstRestart = parseJson(jobInfoBeforeRestart.get(0).get(2) as String).scannedRows as long
            log.info("scannedRows before first restart: " + scannedRowsBeforeFirstRestart)

            // ── Phase 4: restart FE (mid-snapshot) ───────────────────────────────────
            cluster.restartFrontends()
            sleep(60000)
            context.reconnectFe()

            // ── Phase 5: verify job recovers after mid-snapshot restart ──────────────────
            try {
                Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                    def status = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
                    log.info("job status after first restart: " + status)
                    status.size() == 1 && (status.get(0).get(0) == "RUNNING" || status.get(0).get(0) == "PENDING")
                })
            } catch (Exception ex) {
                log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
                throw ex
            }

            def jobInfoAfterRestart1 = sql """select currentOffset, loadStatistic from jobs("type"="insert") where Name='${jobName}'"""
            log.info("job info after first FE restart: " + jobInfoAfterRestart1)
            def scannedRowsAfterFirstRestart = parseJson(jobInfoAfterRestart1.get(0).get(1) as String).scannedRows as long
            assert scannedRowsAfterFirstRestart >= scannedRowsBeforeFirstRestart :
                    "scannedRows should not reset after first FE restart: before=${scannedRowsBeforeFirstRestart}, after=${scannedRowsAfterFirstRestart}"

            // ── Phase 6: wait for full snapshot to complete, then assert no re-processing ─
            // Wait with >= 5 so the await does not time out if a duplicate row briefly
            // pushes the count past 5 before we get a chance to observe exactly 5.
            try {
                Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                    def rows = sql """SELECT count(1) FROM ${currentDb}.${dorisTable}
                                      WHERE name IN ('A1', 'B1', 'C1', 'D1', 'E1')"""
                    log.info("snapshot rows after restart: " + rows)
                    (rows.get(0).get(0) as int) >= 5
                })
            } catch (Exception ex) {
                log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
                log.info("tasks: " + (sql """select * from tasks("type"="insert") where JobName='${jobName}'"""))
                throw ex
            }

            // Verify no snapshot split was re-processed: DUPLICATE KEY table would retain
            // duplicate rows, inflating the count above 5.
            def snapshotCount = sql """SELECT count(1) FROM ${currentDb}.${dorisTable}
                                       WHERE name IN ('A1', 'B1', 'C1', 'D1', 'E1')"""
            assert (snapshotCount.get(0).get(0) as int) == 5 :
                    "Snapshot rows should be exactly 5 after mid-snapshot restart (no re-processing), got: ${snapshotCount.get(0).get(0)}"

            // ── Phase 7: insert F1, G1 and wait for binlog to consume them ──────────────
            connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
                sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('F1', 6)"""
                sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('G1', 7)"""
            }

            try {
                Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                    def rows = sql """SELECT count(1) FROM ${currentDb}.${dorisTable} WHERE name IN ('F1', 'G1')"""
                    log.info("binlog rows before second restart: " + rows)
                    (rows.get(0).get(0) as int) == 2
                })
            } catch (Exception ex) {
                log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
                log.info("tasks: " + (sql """select * from tasks("type"="insert") where JobName='${jobName}'"""))
                throw ex
            }

            // ── Phase 8: restart FE in binlog phase ───────────────────────────────────
            // This exercises the binlog recovery path in replayIfNeed():
            //   currentOffset.snapshotSplit() == false → currentOffset is reused directly.
            def jobInfoBeforeSecondRestart = sql """
                select status, currentOffset, loadStatistic
                from jobs("type"="insert") where Name='${jobName}'
            """
            log.info("job info before second FE restart (binlog phase): " + jobInfoBeforeSecondRestart)
            def offsetBeforeSecondRestart = jobInfoBeforeSecondRestart.get(0).get(1) as String
            assert offsetBeforeSecondRestart != null && offsetBeforeSecondRestart.contains("binlog-split") :
                    "currentOffset should be in binlog state before second restart (F1/G1 consumed), got: ${offsetBeforeSecondRestart}"

            // Record scannedRows before restart — snapshot(5) + F1 + G1 = at least 7.
            def scannedRowsBefore = parseJson(jobInfoBeforeSecondRestart.get(0).get(2) as String).scannedRows as long
            log.info("scannedRows before second restart: " + scannedRowsBefore)

            cluster.restartFrontends()
            sleep(60000)
            context.reconnectFe()

            // ── Phase 9: verify job recovers after binlog-phase restart ──────────────────
            try {
                Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                    def status = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
                    log.info("job status after second restart: " + status)
                    status.size() == 1 && (status.get(0).get(0) == "RUNNING" || status.get(0).get(0) == "PENDING")
                })
            } catch (Exception ex) {
                log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
                throw ex
            }

            def jobInfoAfterRestart2 = sql """select currentOffset, loadStatistic from jobs("type"="insert") where Name='${jobName}'"""
            log.info("job info after second FE restart: " + jobInfoAfterRestart2)

            // Binlog offset advances with heartbeats even without user data, so offset equality
            // is not a stable assertion. Verify correctness via row counts instead:
            // if binlog offset regressed after restart, F1/G1 would be re-inserted and count > 2.
            def binlogCountAfterRestart = sql """SELECT count(1) FROM ${currentDb}.${dorisTable}
                                                 WHERE name IN ('F1', 'G1')"""
            assert (binlogCountAfterRestart.get(0).get(0) as int) == 2 :
                    "Binlog rows F1/G1 should be exactly 2 after binlog-phase restart (no re-processing), got: ${binlogCountAfterRestart.get(0).get(0)}"

            // ── Phase 10: insert H1, I1 and verify binlog still works after restart ────
            connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
                sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('H1', 8)"""
                sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('I1', 9)"""
            }

            try {
                Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                    def rows = sql """SELECT count(1) FROM ${currentDb}.${dorisTable} WHERE name IN ('H1', 'I1')"""
                    log.info("binlog rows after second restart: " + rows)
                    (rows.get(0).get(0) as int) == 2
                })
            } catch (Exception ex) {
                log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
                log.info("tasks: " + (sql """select * from tasks("type"="insert") where JobName='${jobName}'"""))
                throw ex
            }

            qt_final_data """ SELECT * FROM ${currentDb}.${dorisTable} ORDER BY name """

            // ── Phase 11: verify no failures and scannedRows persisted across restart ──
            def jobInfoFinal = sql """
                select status, FailedTaskCount, ErrorMsg, currentOffset, loadStatistic
                from jobs("type"="insert") where Name='${jobName}'
            """
            log.info("job info final: " + jobInfoFinal)
            assert jobInfoFinal.get(0).get(0) == "RUNNING" : "Job should be RUNNING at end"
            assert (jobInfoFinal.get(0).get(1) as int) == 0 : "FailedTaskCount should be 0"

            // Verify scannedRows is not reset to 0 after FE restart (jobStatistic must persist).
            def scannedRowsAfter = parseJson(jobInfoFinal.get(0).get(4) as String).scannedRows as long
            log.info("scannedRows after second restart: " + scannedRowsAfter)
            assert scannedRowsAfter >= scannedRowsBefore :
                    "scannedRows should not reset after FE restart: before=${scannedRowsBefore}, after=${scannedRowsAfter}"

            sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
            sql """drop table if exists ${currentDb}.${dorisTable} force"""
        }
    }
}
