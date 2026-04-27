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

// Verify per-resource ownership of PG replication slot and publication:
//   Test 1: auto-generated slot & publication — created on job start, cleaned up on drop
//   Test 2: user-provided slot & publication — Doris uses but never drops them
//   Test 3: mixed (user publication + auto slot) — only auto slot is dropped on job deletion
//   Test 4: slot_name / publication_name are immutable via ALTER JOB
suite("test_streaming_postgres_job_publication", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_pg_pub_job"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "pub_test_table1"
    def table2 = "pub_test_table2"
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""
    sql """drop table if exists ${currentDb}.${table2} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port = context.config.otherConfigs.get("pg_14_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        // Setup PG tables
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table2}"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${table1} (
                  "id" int PRIMARY KEY,
                  "name" varchar(200)
                )"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${table2} (
                  "id" int PRIMARY KEY,
                  "name" varchar(200)
                )"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, name) VALUES (1, 'Alice');"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table2} (id, name) VALUES (1, 'Bob');"""
        }

        // ========== Test 1: auto-generated slot_name and publication_name ==========
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
                    "include_tables" = "${table1},${table2}",
                    "offset" = "initial"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        // Verify SHOW contains the expected auto-generated slot_name and publication_name
        def jobRow = sql """select Id, ExecuteSql from jobs("type"="insert") where Name='${jobName}'"""
        def jobId = jobRow.get(0).get(0).toString()
        def executeSql = jobRow.get(0).get(1).toString()
        log.info("jobId: ${jobId}, ExecuteSql: ${executeSql}")
        def expectedSlot = "doris_cdc_${jobId}"
        def expectedPub = "doris_pub_${jobId}"
        assert executeSql.contains(expectedSlot) : "ExecuteSql should contain slot_name ${expectedSlot}"
        assert executeSql.contains(expectedPub) : "ExecuteSql should contain publication_name ${expectedPub}"

        // Wait for job to start running and create slot/publication on PG
        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(1, SECONDS).until(
                    {
                        def jobSuccendCount = sql """ select SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                        log.info("jobSuccendCount: " + jobSuccendCount)
                        if (jobSuccendCount.size() != 1) {
                            return false
                        }
                        def succeedTaskCount = jobSuccendCount.get(0).get(0).toString().toLong()
                        succeedTaskCount >= 2L
                    }
            )
        } catch (Exception ex){
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex;
        }

        // Verify on PG side: publication is NOT "FOR ALL TABLES"
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            def pubResult = sql """SELECT pubname, puballtables FROM pg_publication WHERE pubname = '${expectedPub}'"""
            log.info("pg_publication: " + pubResult)
            assert pubResult.size() == 1 : "Publication should exist"
            assert pubResult[0][1] == false : "Publication should NOT be FOR ALL TABLES"

            // Verify publication contains only the specified tables
            def pubTables = sql """SELECT tablename FROM pg_publication_tables WHERE pubname = '${expectedPub}' AND schemaname = '${pgSchema}' ORDER BY tablename"""
            log.info("pg_publication_tables: " + pubTables)
            assert pubTables.size() == 2 : "Publication should contain exactly 2 tables"
            def tableNames = pubTables.collect { it[0] }
            assert tableNames.contains(table1) : "Publication should contain ${table1}"
            assert tableNames.contains(table2) : "Publication should contain ${table2}"

            // Verify replication slot exists
            def slotResult = sql """SELECT slot_name, active FROM pg_replication_slots WHERE slot_name = '${expectedSlot}'"""
            log.info("pg_replication_slots: " + slotResult)
            assert slotResult.size() == 1 : "Replication slot should exist"
        }

        // Check snapshot data synced correctly
        qt_select_pub_snapshot_table1 """ SELECT * FROM ${table1} order by id asc """
        qt_select_pub_snapshot_table2 """ SELECT * FROM ${table2} order by id asc """

        // Drop job and verify PG resources are cleaned up
        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        // Poll until cleanup completes - avoids flakiness on slower environments
        Awaitility.await()
                .atMost(60, SECONDS)
                .pollInterval(1, SECONDS)
                .untilAsserted {
                    connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
                        def pubAfterDrop = sql """SELECT COUNT(1) FROM pg_publication WHERE pubname = '${expectedPub}'"""
                        log.info("pg_publication after drop: " + pubAfterDrop)
                        assert pubAfterDrop[0][0] == 0 : "Publication should be dropped after job deletion"

                        def slotAfterDrop = sql """SELECT COUNT(1) FROM pg_replication_slots WHERE slot_name = '${expectedSlot}'"""
                        log.info("pg_replication_slots after drop: " + slotAfterDrop)
                        assert slotAfterDrop[0][0] == 0 : "Replication slot should be dropped after job deletion"
                    }
                }

        // ========== Test 2: user-provided slot_name and publication_name ==========
        def userSlot = "test_user_slot"
        def userPub = "test_user_pub"

        sql """drop table if exists ${currentDb}.${table1} force"""
        sql """drop table if exists ${currentDb}.${table2} force"""

        // Pre-create publication AND slot; with user-provided names Doris manages neither.
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table2}"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${table1} (
                  "id" int PRIMARY KEY,
                  "name" varchar(200)
                )"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${table2} (
                  "id" int PRIMARY KEY,
                  "name" varchar(200)
                )"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, name) VALUES (1, 'Alice');"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table2} (id, name) VALUES (1, 'Bob');"""
            sql """DROP PUBLICATION IF EXISTS ${userPub}"""
            sql """CREATE PUBLICATION ${userPub} FOR TABLE ${pgDB}.${pgSchema}.${table1}, ${pgDB}.${pgSchema}.${table2}"""
            def existing = sql """SELECT COUNT(1) FROM pg_replication_slots WHERE slot_name = '${userSlot}'"""
            if (existing[0][0] != 0) {
                sql """SELECT pg_drop_replication_slot('${userSlot}')"""
            }
            sql """SELECT pg_create_logical_replication_slot('${userSlot}', 'pgoutput')"""
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
                    "include_tables" = "${table1},${table2}",
                    "slot_name" = "${userSlot}",
                    "publication_name" = "${userPub}",
                    "offset" = "initial"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(1, SECONDS).until(
                    {
                        def jobSuccendCount = sql """ select SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                        if (jobSuccendCount.size() != 1) {
                            return false
                        }
                        def succeedTaskCount = jobSuccendCount.get(0).get(0).toString().toLong()
                        succeedTaskCount >= 2L
                    }
            )
        } catch (Exception ex){
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            log.info("show job: " + showjob)
            throw ex;
        }

        qt_select_user_pub_table1 """ SELECT * FROM ${table1} order by id asc """
        qt_select_user_pub_table2 """ SELECT * FROM ${table2} order by id asc """

        // Drop job: Doris must NOT touch user-provided slot or publication.
        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        sleep(5000)
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            def pubAfter = sql """SELECT COUNT(1) FROM pg_publication WHERE pubname = '${userPub}'"""
            assert pubAfter[0][0] == 1 : "User-provided publication must be preserved after job deletion"
            def slotAfter = sql """SELECT COUNT(1) FROM pg_replication_slots WHERE slot_name = '${userSlot}'"""
            assert slotAfter[0][0] == 1 : "User-provided slot must be preserved after job deletion"
            sql """SELECT pg_drop_replication_slot('${userSlot}')"""
            sql """DROP PUBLICATION IF EXISTS ${userPub}"""
        }

        // ========== Test 3: only publication_name provided (slot is Doris-managed) ==========
        def mixedJob = "test_pg_pub_job_mixed"
        def mixedPub = "test_mixed_pub"
        sql """DROP JOB IF EXISTS where jobname = '${mixedJob}'"""
        sql """drop table if exists ${currentDb}.${table1} force"""
        sql """drop table if exists ${currentDb}.${table2} force"""

        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table2}"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${table1} (
                  "id" int PRIMARY KEY,
                  "name" varchar(200)
                )"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${table2} (
                  "id" int PRIMARY KEY,
                  "name" varchar(200)
                )"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, name) VALUES (1, 'Alice');"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table2} (id, name) VALUES (1, 'Bob');"""
            sql """DROP PUBLICATION IF EXISTS ${mixedPub}"""
            sql """CREATE PUBLICATION ${mixedPub} FOR TABLE ${pgDB}.${pgSchema}.${table1}, ${pgDB}.${pgSchema}.${table2}"""
        }

        sql """CREATE JOB ${mixedJob}
                ON STREAMING
                FROM POSTGRES (
                    "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "org.postgresql.Driver",
                    "user" = "${pgUser}",
                    "password" = "${pgPassword}",
                    "database" = "${pgDB}",
                    "schema" = "${pgSchema}",
                    "include_tables" = "${table1},${table2}",
                    "publication_name" = "${mixedPub}",
                    "offset" = "initial"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(1, SECONDS).until(
                    {
                        def jobSuccendCount = sql """ select SucceedTaskCount from jobs("type"="insert") where Name = '${mixedJob}' and ExecuteType='STREAMING' """
                        if (jobSuccendCount.size() != 1) {
                            return false
                        }
                        jobSuccendCount.get(0).get(0).toString().toLong() >= 2L
                    }
            )
        } catch (Exception ex){
            def showjob = sql """select * from jobs("type"="insert") where Name='${mixedJob}'"""
            log.info("show job: " + showjob)
            throw ex;
        }

        def mixedJobRow = sql """select Id, ExecuteSql from jobs("type"="insert") where Name='${mixedJob}'"""
        def mixedJobId = mixedJobRow.get(0).get(0).toString()
        def mixedExecuteSql = mixedJobRow.get(0).get(1).toString()
        def mixedExpectedSlot = "doris_cdc_${mixedJobId}"
        def mixedExpectedPub = "doris_pub_${mixedJobId}"
        log.info("Mixed job id: ${mixedJobId}, auto slot: ${mixedExpectedSlot}")
        assert mixedExecuteSql.contains(mixedExpectedSlot) : "ExecuteSql should contain slot_name ${mixedExpectedSlot}"
        assert mixedExecuteSql.contains(mixedPub) : "ExecuteSql should contain user-provided publication_name ${mixedPub}"

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
            assert pubAfter[0][0] == 1 : "User-provided publication must be preserved"
            sql """DROP PUBLICATION IF EXISTS ${mixedPub}"""
        }

        // ========== Test 4: slot_name / publication_name are immutable via ALTER JOB ==========
        def alterJob = "test_pg_pub_alter_immutable"
        sql """DROP JOB IF EXISTS where jobname = '${alterJob}'"""
        sql """CREATE JOB ${alterJob}
                ON STREAMING
                FROM POSTGRES (
                    "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "org.postgresql.Driver",
                    "user" = "${pgUser}",
                    "password" = "${pgPassword}",
                    "database" = "${pgDB}",
                    "schema" = "${pgSchema}",
                    "include_tables" = "${table1},${table2}",
                    "offset" = "initial"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        sql """PAUSE JOB where jobname = '${alterJob}'"""
        Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).until(
                {
                    def status = sql """select status from jobs("type"="insert") where Name='${alterJob}'"""
                    status.size() == 1 && status.get(0).get(0).toString() == "PAUSED"
                }
        )

        // Reject ALTER that introduces a new slot_name
        test {
            sql """ALTER JOB ${alterJob}
                FROM POSTGRES (
                    "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "org.postgresql.Driver",
                    "user" = "${pgUser}",
                    "password" = "${pgPassword}",
                    "database" = "${pgDB}",
                    "schema" = "${pgSchema}",
                    "include_tables" = "${table1},${table2}",
                    "slot_name" = "some_other_slot",
                    "offset" = "initial"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """
            exception "The slot_name property cannot be modified in ALTER JOB"
        }

        // Reject ALTER that introduces a new publication_name
        test {
            sql """ALTER JOB ${alterJob}
                FROM POSTGRES (
                    "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "org.postgresql.Driver",
                    "user" = "${pgUser}",
                    "password" = "${pgPassword}",
                    "database" = "${pgDB}",
                    "schema" = "${pgSchema}",
                    "include_tables" = "${table1},${table2}",
                    "publication_name" = "some_other_pub",
                    "offset" = "initial"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """
            exception "The publication_name property cannot be modified in ALTER JOB"
        }

        sql """DROP JOB IF EXISTS where jobname = '${alterJob}'"""

        // Cleanup PG tables
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table2}"""
        }
        sql """drop table if exists ${currentDb}.${table1} force"""
        sql """drop table if exists ${currentDb}.${table2} force"""
    }
}
