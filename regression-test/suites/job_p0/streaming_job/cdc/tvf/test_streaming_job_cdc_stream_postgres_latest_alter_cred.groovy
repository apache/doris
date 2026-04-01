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
 * Test cdc_stream TVF streaming job with offset=latest and credential update via ALTER JOB.
 *
 * Scenario A — offset=latest:
 *   1. Pre-existing rows (A1, B1) are present in the PG source table before the job is created.
 *   2. Job is created with offset=latest: snapshot is skipped, only changes after job creation are synced.
 *   3. INSERT (C1, D1) after job creation → should appear in Doris.
 *   4. Verify A1, B1 are NOT in Doris (skipped by offset=latest).
 *
 * Scenario B — ALTER with wrong credentials:
 *   1. Pause the job.
 *   2. ALTER JOB with wrong password (same type/database/table, so the unmodifiable-property check passes).
 *   3. Resume the job → fetchMeta fails → job auto-pauses with an error message.
 *   4. Verify the error message reflects the credential failure.
 *
 * Scenario C — ALTER back to correct credentials and recover:
 *   1. Alter back to correct credentials while still PAUSED.
 *   2. Resume the job → job returns to RUNNING.
 *   3. INSERT (E1, F1) after recovery → should appear in Doris (no data loss during pause).
 *   4. Verify final data: only C1, D1, E1, F1 are present (A1, B1 still absent).
 */
suite("test_streaming_job_cdc_stream_postgres_latest_alter_cred",
        "p0,external,pg,external_docker,external_docker_pg,nondatalake") {

    def jobName   = "test_streaming_job_cdc_stream_pg_latest_alter_cred"
    def currentDb = (sql "select database()")[0][0]
    def dorisTable = "test_streaming_job_cdc_stream_pg_latest_alter_cred_tbl"
    def pgDB       = "postgres"
    def pgSchema   = "cdc_test"
    def pgUser     = "postgres"
    def pgPassword = "123456"
    def pgTable    = "test_streaming_job_cdc_stream_pg_latest_alter_cred_src"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${dorisTable} force"""

    sql """
        CREATE TABLE IF NOT EXISTS ${currentDb}.${dorisTable} (
            `name` varchar(200) NOT NULL,
            `age`  int NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`name`)
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
        String jdbcUrl = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}"

        // ── Scenario A setup: pre-existing rows exist before job creation ─────────────
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${pgTable}"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${pgTable} (
                      "name" varchar(200) PRIMARY KEY,
                      "age"  int2
                  )"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('A1', 1)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('B1', 2)"""
        }

        // ── Scenario A: create job with offset=latest (skip snapshot) ─────────────────
        sql """
            CREATE JOB ${jobName}
            ON STREAMING DO INSERT INTO ${currentDb}.${dorisTable} (name, age)
            SELECT name, age FROM cdc_stream(
                "type"         = "postgres",
                "jdbc_url"     = "${jdbcUrl}",
                "driver_url"   = "${driver_url}",
                "driver_class" = "org.postgresql.Driver",
                "user"         = "${pgUser}",
                "password"     = "${pgPassword}",
                "database"     = "${pgDB}",
                "schema"       = "${pgSchema}",
                "table"        = "${pgTable}",
                "offset"       = "latest"
            )
        """

        // wait for job to reach RUNNING state (at least one task scheduled)
        try {
            Awaitility.await().atMost(60, SECONDS).pollInterval(1, SECONDS).until({
                def status = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
                status.size() == 1 && status.get(0).get(0) == "RUNNING"
            })
        } catch (Exception ex) {
            log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            throw ex
        }

        // insert rows AFTER job creation — only these should appear in Doris
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('C1', 3)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('D1', 4)"""
        }

        // wait for C1 and D1 to arrive via binlog
        try {
            Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                def rows = sql """SELECT count(1) FROM ${currentDb}.${dorisTable} WHERE name IN ('C1', 'D1')"""
                log.info("binlog rows (offset=latest): " + rows)
                (rows.get(0).get(0) as int) == 2
            })
        } catch (Exception ex) {
            log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            log.info("tasks: " + (sql """select * from tasks("type"="insert") where JobName='${jobName}'"""))
            throw ex
        }

        // offset=latest: A1 and B1 must NOT be in Doris (they existed before job creation)
        def preExisting = sql """SELECT count(1) FROM ${currentDb}.${dorisTable} WHERE name IN ('A1', 'B1')"""
        assert (preExisting.get(0).get(0) as int) == 0 :
                "offset=latest: pre-existing rows A1/B1 must not be synced, but found ${preExisting.get(0).get(0)}"

        qt_offset_latest_data """ SELECT * FROM ${currentDb}.${dorisTable} ORDER BY name """

        // ── Scenario B: ALTER to wrong credentials → job should auto-pause ────────────
        sql """PAUSE JOB where jobname = '${jobName}'"""
        Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).until({
            def s = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
            s.size() == 1 && s.get(0).get(0) == "PAUSED"
        })

        // alter with wrong password; type/jdbc_url/database/schema/table remain unchanged
        sql """
            ALTER JOB ${jobName}
            INSERT INTO ${currentDb}.${dorisTable} (name, age)
            SELECT name, age FROM cdc_stream(
                "type"         = "postgres",
                "jdbc_url"     = "${jdbcUrl}",
                "driver_url"   = "${driver_url}",
                "driver_class" = "org.postgresql.Driver",
                "user"         = "${pgUser}",
                "password"     = "wrong_password_for_test",
                "database"     = "${pgDB}",
                "schema"       = "${pgSchema}",
                "table"        = "${pgTable}",
                "offset"       = "latest"
            )
        """

        sql """RESUME JOB where jobname = '${jobName}'"""

        // job should re-pause due to credential failure
        try {
            Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                def s = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
                log.info("status after wrong cred resume: " + s)
                s.size() == 1 && s.get(0).get(0) == "PAUSED"
            })
        } catch (Exception ex) {
            log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            throw ex
        }

        def errorMsgAfterWrongCred = sql """select ErrorMsg from jobs("type"="insert") where Name='${jobName}'"""
        log.info("error msg after wrong cred: " + errorMsgAfterWrongCred)
        assert errorMsgAfterWrongCred.get(0).get(0) != null && !errorMsgAfterWrongCred.get(0).get(0).isEmpty() :
                "ErrorMsg should be non-empty when wrong credentials are used"

        // ── Scenario C: ALTER back to correct credentials and recover ─────────────────
        sql """
            ALTER JOB ${jobName}
            INSERT INTO ${currentDb}.${dorisTable} (name, age)
            SELECT name, age FROM cdc_stream(
                "type"         = "postgres",
                "jdbc_url"     = "${jdbcUrl}",
                "driver_url"   = "${driver_url}",
                "driver_class" = "org.postgresql.Driver",
                "user"         = "${pgUser}",
                "password"     = "${pgPassword}",
                "database"     = "${pgDB}",
                "schema"       = "${pgSchema}",
                "table"        = "${pgTable}",
                "offset"       = "latest"
            )
        """

        sql """RESUME JOB where jobname = '${jobName}'"""

        // verify job transitions back to RUNNING
        try {
            Awaitility.await().atMost(60, SECONDS).pollInterval(1, SECONDS).until({
                def s = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
                log.info("status after correct cred resume: " + s)
                s.size() == 1 && s.get(0).get(0) == "RUNNING"
            })
        } catch (Exception ex) {
            log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            throw ex
        }

        // insert rows after recovery; they should be consumed
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('E1', 5)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('F1', 6)"""
        }

        try {
            Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                def rows = sql """SELECT count(1) FROM ${currentDb}.${dorisTable} WHERE name IN ('E1', 'F1')"""
                log.info("rows after credential recovery: " + rows)
                (rows.get(0).get(0) as int) == 2
            })
        } catch (Exception ex) {
            log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            log.info("tasks: " + (sql """select * from tasks("type"="insert") where JobName='${jobName}'"""))
            throw ex
        }

        // final state: only post-creation rows (C1, D1, E1, F1) — A1/B1 absent throughout
        qt_final_data """ SELECT * FROM ${currentDb}.${dorisTable} ORDER BY name """

        def finalJobInfo = sql """select status, FailedTaskCount, ErrorMsg
                                  from jobs("type"="insert") where Name='${jobName}'"""
        log.info("final job info: " + finalJobInfo)
        assert finalJobInfo.get(0).get(0) == "RUNNING"

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        sql """drop table if exists ${currentDb}.${dorisTable} force"""
    }
}
