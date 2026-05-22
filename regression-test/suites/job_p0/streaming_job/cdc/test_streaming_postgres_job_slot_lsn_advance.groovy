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

// pg_replication_slots.confirmed_flush_lsn must advance as cdc_client consumes
// WAL, must NOT advance while the job is paused (after the in-flight task
// finishes its final ack), and must resume advancing after RESUME. A stuck
// confirmed_flush_lsn means WAL piles up on the source and eventually exhausts
// disk — operationally this is the single most important health signal for a
// long-running PG CDC job. See [[project_pgcdc_task_offset_stuck]] for the
// customer-site bug class this guards against.
//
// PAUSE semantics (verified in code):
//   PipelineCoordinator.writeRecords runs one maxInterval window per task,
//   then commitSourceOffset (acks LSN to PG) and finishSplitRecords (closes
//   the replication connection). PAUSE only stops the FE from scheduling the
//   next task — the in-flight task runs to its natural end and acks LSN one
//   last time. After that final ack, no consumer exists and no further LSN
//   advancement is possible regardless of WAL growth on the source.
// Hence the test waits for LSN to settle after PAUSE before asserting it stays
// frozen. max_interval=3 keeps the in-flight task short so the settle window
// stays under 30s.
//
// Uses a user-provided slot/publication for two reasons: (1) the slot name is
// known up front so we don't have to fish jobId out of the jobs() view, and
// (2) DROP JOB then leaves the slot intact, so post-test cleanup is explicit.
suite("test_streaming_postgres_job_slot_lsn_advance",
        "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_pg_slot_lsn_advance_job"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "slot_lsn_advance_pg_tbl"
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"
    def userSlot = "slot_lsn_advance_user_slot"
    def userPub = "slot_lsn_advance_user_pub"

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
                  "id" int PRIMARY KEY,
                  "name" varchar(200)
                )"""
            sql """DROP PUBLICATION IF EXISTS ${userPub}"""
            sql """CREATE PUBLICATION ${userPub} FOR TABLE ${pgDB}.${pgSchema}.${table1}"""
            def existing = sql """SELECT COUNT(1) FROM pg_replication_slots WHERE slot_name = '${userSlot}'"""
            if (existing[0][0] != 0) {
                sql """SELECT pg_drop_replication_slot('${userSlot}')"""
            }
            sql """SELECT pg_create_logical_replication_slot('${userSlot}', 'pgoutput')"""
        }

        // offset=latest skips snapshot and goes straight to streaming — that is
        // where confirmed_flush_lsn is meaningful. max_interval=3 keeps each
        // task short so PAUSE settles fast.
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
                    "slot_name" = "${userSlot}",
                    "publication_name" = "${userPub}",
                    "offset" = "latest"
                )
                PROPERTIES ("max_interval" = "3")
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        Awaitility.await().atMost(120, SECONDS).pollInterval(1, SECONDS).until({
            def st = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
            st.size() == 1 && st.get(0).get(0) == "RUNNING"
        })

        // Helper to read confirmed_flush_lsn as BigInteger (Long would overflow
        // on production-scale wraparound; BigInteger is safe).
        Closure<BigInteger> readLsn = {
            BigInteger out = BigInteger.valueOf(-1L)
            connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
                def r = sql """SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = '${userSlot}'"""
                if (r.size() == 1 && r.get(0).get(0) != null) {
                    def parts = r.get(0).get(0).toString().split("/")
                    out = new BigInteger(parts[0], 16).shiftLeft(32).add(new BigInteger(parts[1], 16))
                }
            }
            out
        }

        def lsn0 = readLsn()
        log.info("initial confirmed_flush_lsn=${lsn0}")
        assert lsn0.signum() > 0 : "user slot ${userSlot} confirmed_flush_lsn is null/invalid"

        // ===== Phase 1: steady-state advancement =====
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            for (int i = 1; i <= 20; i++) {
                sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (${i}, 'name_${i}')"""
            }
        }
        Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
            def cnt = sql """SELECT count(*) FROM ${currentDb}.${table1}"""
            cnt.size() == 1 && cnt.get(0).get(0) >= 20
        })
        Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
            def cur = readLsn()
            log.info("phase1 poll lsn=${cur} (init=${lsn0})")
            cur > lsn0
        })

        // ===== Phase 2: PAUSE — in-flight task acks once then LSN must freeze =====
        sql """PAUSE JOB where jobname = '${jobName}'"""
        Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).until({
            def st = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
            st.size() == 1 && st.get(0).get(0) == "PAUSED"
        })

        // Wait for LSN to settle: N consecutive equal samples confirm the in-flight
        // task has completed its final commitSourceOffset and the connection is closed.
        BigInteger lastLsn = BigInteger.valueOf(-1L)
        int stable = 0
        final int requiredStable = 4
        Awaitility.await().atMost(120, SECONDS).pollInterval(4, SECONDS).until({
            BigInteger cur = readLsn()
            if (cur == lastLsn) {
                stable++
            } else {
                stable = 1
                lastLsn = cur
            }
            log.info("pause-settle lsn=${cur} stable=${stable}/${requiredStable}")
            stable >= requiredStable
        })
        def lsnAtPauseSettled = lastLsn

        // Generate WAL while paused. With no consumer the slot's
        // confirmed_flush_lsn must remain frozen even though WAL is growing.
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            for (int i = 100; i < 120; i++) {
                sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (${i}, 'paused_${i}')"""
            }
        }
        sleep(15000)
        def lsnDuringPause = readLsn()
        log.info("paused: settled=${lsnAtPauseSettled} duringPause=${lsnDuringPause}")
        assert lsnDuringPause == lsnAtPauseSettled :
                "confirmed_flush_lsn advanced while paused with no consumer: " +
                "${lsnAtPauseSettled} -> ${lsnDuringPause}"

        // ===== Phase 3: RESUME — LSN must advance past the paused snapshot =====
        sql """RESUME JOB where jobname = '${jobName}'"""
        Awaitility.await().atMost(60, SECONDS).pollInterval(1, SECONDS).until({
            def st = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
            st.size() == 1 && st.get(0).get(0) == "RUNNING"
        })
        Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
            def cnt = sql """SELECT count(*) FROM ${currentDb}.${table1}"""
            cnt.size() == 1 && cnt.get(0).get(0) >= 40
        })
        Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
            def cur = readLsn()
            log.info("phase3 poll lsn=${cur} (pause=${lsnAtPauseSettled})")
            cur > lsnAtPauseSettled
        })

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        // User-provided slot/pub survive DROP JOB; clean up manually. After DROP JOB
        // the cdc_client may still be winding down its replication connection — PG
        // rejects pg_drop_replication_slot on an active slot, so poll active=false
        // before issuing the drop.
        Awaitility.await().atMost(60, SECONDS).pollInterval(1, SECONDS).until({
            boolean inactive = false
            connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
                def r = sql """SELECT active FROM pg_replication_slots WHERE slot_name = '${userSlot}'"""
                inactive = r.size() == 1 && r.get(0).get(0) == false
            }
            inactive
        })
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            def slotStillThere = sql """SELECT COUNT(1) FROM pg_replication_slots WHERE slot_name = '${userSlot}'"""
            assert slotStillThere[0][0] == 1 : "user-provided slot must not be dropped by Doris"
            sql """SELECT pg_drop_replication_slot('${userSlot}')"""
            sql """DROP PUBLICATION IF EXISTS ${userPub}"""
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
        }
        sql """drop table if exists ${currentDb}.${table1} force"""
    }
}
