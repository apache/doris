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

// When the replication slot is dropped out from under a running incremental job,
// resuming from the committed position would silently skip the WAL discarded with
// the slot. On rebuild, validateStreamSource detects the missing slot and fails with
// the "Replication slot invalidated" marker; FE classifies that as CANNOT_RESUME_ERR,
// so the job settles in PAUSED and is NOT pulled back to RUNNING by auto-resume.
//
// Uses a user-provided slot so (1) the slot name is known up front and (2) Doris does
// not auto-recreate it (createSlotForGlobalStreamSplit only fires for Doris-owned
// slots), keeping the "slot not found" branch deterministic.
//
// We assert state + error marker only — NOT a hard slot-count check, which is
// TOCTOU-flaky against the cdc_client winding down its connection.
suite("test_streaming_postgres_job_slot_dropped_during_incremental",
        "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_pg_slot_dropped_job"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "slot_dropped_pg_tbl"
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"
    def userSlot = "slot_dropped_user_slot"
    def userPub = "slot_dropped_user_pub"

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

        // offset=latest skips snapshot and goes straight to streaming; max_interval=3
        // keeps tasks short so the rebuild after the drop happens quickly.
        sql """CREATE JOB ${jobName}
                PROPERTIES ("max_interval" = "3")
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
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        Awaitility.await().atMost(120, SECONDS).pollInterval(1, SECONDS).until({
            def st = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
            st.size() == 1 && st.get(0).get(0) == "RUNNING"
        })

        // ===== Phase 1: confirm steady-state incremental sync =====
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            for (int i = 1; i <= 10; i++) {
                sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (${i}, 'name_${i}')"""
            }
        }
        Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
            def cnt = sql """SELECT count(*) FROM ${currentDb}.${table1}"""
            cnt.size() == 1 && cnt.get(0).get(0) >= 10
        })

        // ===== Phase 2: drop the slot out from under the running job =====
        // Terminate whatever consumer holds the slot, then drop it. Retry until gone:
        // an auto-resume task may briefly re-acquire it before we manage to drop.
        Awaitility.await().atMost(60, SECONDS).pollInterval(2, SECONDS).until({
            boolean dropped = false
            connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
                sql """SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots
                       WHERE slot_name = '${userSlot}' AND active_pid IS NOT NULL"""
                def cnt = sql """SELECT COUNT(1) FROM pg_replication_slots WHERE slot_name = '${userSlot}'"""
                if (cnt[0][0] == 0) {
                    dropped = true
                } else {
                    try {
                        sql """SELECT pg_drop_replication_slot('${userSlot}')"""
                        dropped = true
                    } catch (Exception e) {
                        log.info("slot still active, retry drop: " + e.getMessage())
                    }
                }
            }
            dropped
        })

        // Generate WAL so the rebuilt reader is forced to locate the now-missing slot.
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            for (int i = 100; i < 110; i++) {
                sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (${i}, 'after_drop_${i}')"""
            }
        }

        // ===== Phase 3: job settles in PAUSED with the slot-invalidated marker =====
        Awaitility.await().atMost(240, SECONDS).pollInterval(3, SECONDS).until({
            def r = sql """select status, ErrorMsg from jobs("type"="insert") where Name='${jobName}'"""
            if (r.size() != 1) {
                return false
            }
            def status = r.get(0).get(0)
            def errMsg = r.get(0).get(1)
            log.info("waiting slot-invalidated PAUSED: status=${status} errMsg=${errMsg}")
            status == "PAUSED" && errMsg != null && errMsg.toString().contains("Replication slot invalidated")
        })

        // ===== Phase 4: CANNOT_RESUME — must stay PAUSED, not auto-resumed to RUNNING =====
        for (int i = 0; i < 8; i++) {
            sleep(5000)
            def r = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
            assert r.size() == 1 && r.get(0).get(0) == "PAUSED" :
                    "job must stay PAUSED after slot invalidation, got ${r}"
        }

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            def slotLeft = sql """SELECT COUNT(1) FROM pg_replication_slots WHERE slot_name = '${userSlot}'"""
            if (slotLeft[0][0] != 0) {
                sql """SELECT pg_drop_replication_slot('${userSlot}')"""
            }
            sql """DROP PUBLICATION IF EXISTS ${userPub}"""
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
        }
        sql """drop table if exists ${currentDb}.${table1} force"""
    }
}
