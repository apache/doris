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

// PG slot/publication ownership on the cdc_stream TVF path (streaming INSERT job).
//   Test 1: auto-generated slot & publication — default names injected at task scheduling,
//           created on PG side, cleaned up on DROP JOB
//   Test 2: user-provided slot & publication — Doris uses them but never drops them
//   Test 3: mixed (user publication + auto slot) — only the auto slot is dropped
//
// cdc_stream TVF only supports a single source table (`table` property), so each case
// uses one PG table instead of the multi-table include_tables list used by the from-to
// ownership test.
suite("test_cdc_stream_tvf_publication", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_cdc_tvf_pub_job"
    def currentDb = (sql "select database()")[0][0]
    def dorisTable = "cdc_tvf_pub_tbl"
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"
    def pgTable = "cdc_tvf_pub_src"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${dorisTable} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port = context.config.otherConfigs.get("pg_14_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        def prepareDorisTable = {
            sql """drop table if exists ${currentDb}.${dorisTable} force"""
            sql """
                CREATE TABLE IF NOT EXISTS ${currentDb}.${dorisTable} (
                    `id`   int NOT NULL,
                    `name` varchar(200) NULL
                ) ENGINE=OLAP
                UNIQUE KEY(`id`)
                DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
            """
        }

        def preparePgTable = {
            connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
                sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${pgTable}"""
                sql """CREATE TABLE ${pgDB}.${pgSchema}.${pgTable} (
                        "id" int PRIMARY KEY,
                        "name" varchar(200)
                    )"""
                sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (id, name) VALUES (1, 'Alice')"""
            }
        }

        def waitJobRunning = { String name ->
            try {
                Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                    def cnt = sql """select SucceedTaskCount from jobs("type"="insert") where Name='${name}' and ExecuteType='STREAMING'"""
                    cnt.size() == 1 && (cnt.get(0).get(0) as int) >= 2
                })
            } catch (Exception ex) {
                log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${name}'"""))
                log.info("tasks: " + (sql """select * from tasks("type"="insert") where JobName='${name}'"""))
                throw ex
            }
        }

        // ========== Test 1: auto-generated slot_name and publication_name ==========
        prepareDorisTable()
        preparePgTable()

        sql """
            CREATE JOB ${jobName}
            ON STREAMING DO INSERT INTO ${currentDb}.${dorisTable} (id, name)
            SELECT id, name FROM cdc_stream(
                "type"         = "postgres",
                "jdbc_url"     = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}",
                "driver_url"   = "${driver_url}",
                "driver_class" = "org.postgresql.Driver",
                "user"         = "${pgUser}",
                "password"     = "${pgPassword}",
                "database"     = "${pgDB}",
                "schema"       = "${pgSchema}",
                "table"        = "${pgTable}",
                "offset"       = "initial",
                "snapshot_split_size" = "1"
            )
        """

        def jobRow = sql """select Id from jobs("type"="insert") where Name='${jobName}'"""
        def jobId = jobRow.get(0).get(0).toString()
        def expectedSlot = "doris_cdc_${jobId}"
        def expectedPub = "doris_pub_${jobId}"
        log.info("jobId: ${jobId}, expected slot: ${expectedSlot}, pub: ${expectedPub}")

        waitJobRunning(jobName)

        // Defaults are injected runtime (not in ExecuteSql); verify via PG side.
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            def pubResult = sql """SELECT pubname, puballtables FROM pg_publication WHERE pubname = '${expectedPub}'"""
            assert pubResult.size() == 1 : "auto publication ${expectedPub} should exist"
            assert pubResult[0][1] == false : "auto publication should NOT be FOR ALL TABLES"

            def pubTables = sql """SELECT tablename FROM pg_publication_tables WHERE pubname = '${expectedPub}' AND schemaname = '${pgSchema}'"""
            assert pubTables.size() == 1 : "publication should contain exactly 1 table"
            assert pubTables[0][0] == pgTable : "publication should contain ${pgTable}"

            def slotResult = sql """SELECT slot_name FROM pg_replication_slots WHERE slot_name = '${expectedSlot}'"""
            assert slotResult.size() == 1 : "auto replication slot ${expectedSlot} should exist"
        }

        qt_select_auto_snapshot """ SELECT * FROM ${currentDb}.${dorisTable} ORDER BY id """

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        Awaitility.await().atMost(60, SECONDS).pollInterval(1, SECONDS).untilAsserted {
            connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
                def pubAfter = sql """SELECT COUNT(1) FROM pg_publication WHERE pubname = '${expectedPub}'"""
                assert pubAfter[0][0] == 0 : "auto publication should be dropped after job deletion"
                def slotAfter = sql """SELECT COUNT(1) FROM pg_replication_slots WHERE slot_name = '${expectedSlot}'"""
                assert slotAfter[0][0] == 0 : "auto slot should be dropped after job deletion"
            }
        }

        // ========== Test 2: user-provided slot_name and publication_name ==========
        def userSlot = "test_tvf_user_slot"
        def userPub = "test_tvf_user_pub"

        prepareDorisTable()
        preparePgTable()
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP PUBLICATION IF EXISTS ${userPub}"""
            sql """CREATE PUBLICATION ${userPub} FOR TABLE ${pgDB}.${pgSchema}.${pgTable}"""
            def existing = sql """SELECT COUNT(1) FROM pg_replication_slots WHERE slot_name = '${userSlot}'"""
            if (existing[0][0] != 0) {
                sql """SELECT pg_drop_replication_slot('${userSlot}')"""
            }
            sql """SELECT pg_create_logical_replication_slot('${userSlot}', 'pgoutput')"""
        }

        sql """
            CREATE JOB ${jobName}
            ON STREAMING DO INSERT INTO ${currentDb}.${dorisTable} (id, name)
            SELECT id, name FROM cdc_stream(
                "type"             = "postgres",
                "jdbc_url"         = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}",
                "driver_url"       = "${driver_url}",
                "driver_class"     = "org.postgresql.Driver",
                "user"             = "${pgUser}",
                "password"         = "${pgPassword}",
                "database"         = "${pgDB}",
                "schema"           = "${pgSchema}",
                "table"            = "${pgTable}",
                "slot_name"        = "${userSlot}",
                "publication_name" = "${userPub}",
                "offset"           = "initial",
                "snapshot_split_size" = "1"
            )
        """

        waitJobRunning(jobName)
        qt_select_user_snapshot """ SELECT * FROM ${currentDb}.${dorisTable} ORDER BY id """

        // Drop job: Doris must NOT touch user-provided slot or publication.
        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        sleep(5000)
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            def pubAfter = sql """SELECT COUNT(1) FROM pg_publication WHERE pubname = '${userPub}'"""
            assert pubAfter[0][0] == 1 : "user-provided publication must be preserved after job deletion"
            def slotAfter = sql """SELECT COUNT(1) FROM pg_replication_slots WHERE slot_name = '${userSlot}'"""
            assert slotAfter[0][0] == 1 : "user-provided slot must be preserved after job deletion"
            sql """SELECT pg_drop_replication_slot('${userSlot}')"""
            sql """DROP PUBLICATION IF EXISTS ${userPub}"""
        }

        // ========== Test 3: only publication_name provided (slot is Doris-managed) ==========
        def mixedJob = "test_cdc_tvf_pub_mixed"
        def mixedPub = "test_tvf_mixed_pub"
        sql """DROP JOB IF EXISTS where jobname = '${mixedJob}'"""

        prepareDorisTable()
        preparePgTable()
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP PUBLICATION IF EXISTS ${mixedPub}"""
            sql """CREATE PUBLICATION ${mixedPub} FOR TABLE ${pgDB}.${pgSchema}.${pgTable}"""
        }

        sql """
            CREATE JOB ${mixedJob}
            ON STREAMING DO INSERT INTO ${currentDb}.${dorisTable} (id, name)
            SELECT id, name FROM cdc_stream(
                "type"             = "postgres",
                "jdbc_url"         = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}",
                "driver_url"       = "${driver_url}",
                "driver_class"     = "org.postgresql.Driver",
                "user"             = "${pgUser}",
                "password"         = "${pgPassword}",
                "database"         = "${pgDB}",
                "schema"           = "${pgSchema}",
                "table"            = "${pgTable}",
                "publication_name" = "${mixedPub}",
                "offset"           = "initial",
                "snapshot_split_size" = "1"
            )
        """

        waitJobRunning(mixedJob)

        def mixedRow = sql """select Id from jobs("type"="insert") where Name='${mixedJob}'"""
        def mixedJobId = mixedRow.get(0).get(0).toString()
        def mixedExpectedSlot = "doris_cdc_${mixedJobId}"

        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            def slotResult = sql """SELECT slot_name FROM pg_replication_slots WHERE slot_name = '${mixedExpectedSlot}'"""
            assert slotResult.size() == 1 : "auto slot ${mixedExpectedSlot} should exist"
            def pubTables = sql """SELECT tablename FROM pg_publication_tables WHERE pubname = '${mixedPub}' AND schemaname = '${pgSchema}'"""
            assert pubTables.size() == 1 : "user-provided publication should contain the table"
        }

        // Drop job: Doris drops the auto-managed slot but preserves the user-provided publication.
        sql """DROP JOB IF EXISTS where jobname = '${mixedJob}'"""
        Awaitility.await().atMost(60, SECONDS).pollInterval(1, SECONDS).untilAsserted {
            connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
                def slotCount = sql """SELECT COUNT(1) FROM pg_replication_slots WHERE slot_name = '${mixedExpectedSlot}'"""
                assert slotCount[0][0] == 0 : "Doris-managed slot ${mixedExpectedSlot} should be dropped"
            }
        }
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            def pubAfter = sql """SELECT COUNT(1) FROM pg_publication WHERE pubname = '${mixedPub}'"""
            assert pubAfter[0][0] == 1 : "user-provided publication must be preserved"
            sql """DROP PUBLICATION IF EXISTS ${mixedPub}"""
        }

        // Cleanup
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${pgTable}"""
        }
        sql """drop table if exists ${currentDb}.${dorisTable} force"""
    }
}
