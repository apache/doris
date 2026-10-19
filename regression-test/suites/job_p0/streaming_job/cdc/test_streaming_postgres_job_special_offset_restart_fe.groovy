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

// Mirror of test_streaming_mysql_job_special_offset_restart_fe for the PG path:
// CREATE JOB with a JSON LSN offset, sync, restart FE, verify currentOffset
// survives the replay and subsequent binlog DML still lands.
//
// PG-specific wrinkle: an auto-managed slot starts retaining WAL only at slot
// creation time, so a CREATE-with-past-LSN against an auto slot would fail
// because PG has already purged the requested LSN. We therefore pre-create a
// user-provided slot first — that pins the WAL retention horizon back in time
// far enough to make the LSN we capture valid.
suite("test_streaming_postgres_job_special_offset_restart_fe",
        "docker,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_pg_special_offset_restart_fe"
    def options = new ClusterOptions()
    options.setFeNum(1)
    // TODO: remove once cleanMeta targets the BE that owns the reader (binlog
    // phase pinned to a single BE). Today selectBackend() round-robins across
    // BEs, so /api/close can land on a BE that doesn't hold the PG replication
    // connection — the real holder is never notified and the slot stays active.
    options.setBeNum(1)
    options.cloudMode = null

    docker(options) {
        def currentDb = (sql "select database()")[0][0]
        def table1 = "special_offset_restart_pg_tbl"
        def pgDB = "postgres"
        def pgSchema = "cdc_test"
        def pgUser = "postgres"
        def pgPassword = "123456"
        def userSlot = "special_offset_restart_slot"
        def userPub = "special_offset_restart_pub"

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        sql """drop table if exists ${currentDb}.${table1} force"""

        String enabled = context.config.otherConfigs.get("enableJdbcTest")
        if (enabled != null && enabled.equalsIgnoreCase("true")) {
            String pg_port = context.config.otherConfigs.get("pg_14_port");
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String s3_endpoint = getS3Endpoint()
            String bucket = getS3BucketName()
            String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

            // Setup: fresh PG table + fresh user slot/pub. Slot must be created
            // BEFORE the LSN we capture below, otherwise PG would have purged
            // the WAL covering that LSN by the time the job tries to replay it.
            def lsnAtCreate = ""
            connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
                sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
                sql """CREATE TABLE ${pgDB}.${pgSchema}.${table1} (
                      "id" int PRIMARY KEY,
                      "name" varchar(100)
                    )"""
                sql """DROP PUBLICATION IF EXISTS ${userPub}"""
                sql """CREATE PUBLICATION ${userPub} FOR TABLE ${pgDB}.${pgSchema}.${table1}"""
                def existing = sql """SELECT COUNT(1) FROM pg_replication_slots WHERE slot_name = '${userSlot}'"""
                if (existing[0][0] != 0) {
                    sql """SELECT pg_drop_replication_slot('${userSlot}')"""
                }
                sql """SELECT pg_create_logical_replication_slot('${userSlot}', 'pgoutput')"""

                // Capture LSN AFTER slot creation, BEFORE the INSERTs the job will read.
                def lsnRows = sql """SELECT pg_current_wal_lsn()::text"""
                def lsnStr = lsnRows[0][0].toString()
                def parts = lsnStr.split("/")
                lsnAtCreate = new BigInteger(parts[0], 16).shiftLeft(32)
                        .add(new BigInteger(parts[1], 16)).toString()
                log.info("CREATE LSN mark: ${lsnStr} -> numeric: ${lsnAtCreate}")

                // Inserts after the mark: these are what the job should stream.
                sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (1, 'alice')"""
                sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (2, 'bob')"""
            }

            def offsetJson = """{"lsn":"${lsnAtCreate}"}"""
            log.info("Creating job with LSN offset: ${offsetJson}")
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
                        "offset" = '${offsetJson}'
                    )
                    TO DATABASE ${currentDb} (
                      "table.create.properties.replication_num" = "1"
                    )
                """

            try {
                Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                    def succeed = sql """select SucceedTaskCount from jobs("type"="insert") where Name='${jobName}' and ExecuteType='STREAMING'"""
                    def cnt = sql """SELECT count(*) FROM ${currentDb}.${table1}"""
                    log.info("pre-restart succeed=${succeed} rows=${cnt}")
                    succeed.size() == 1 &&
                            (succeed.get(0).get(0) as int) >= 1 &&
                            cnt.size() == 1 && cnt.get(0).get(0) == 2
                })
            } catch (Exception ex) {
                def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
                def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
                log.info("show job: " + showjob)
                log.info("show task: " + showtask)
                throw ex;
            }

            def jobInfoBefore = sql """
                select loadStatistic, status, currentOffset from jobs("type"="insert") where Name='${jobName}'
            """
            log.info("jobInfoBefore: " + jobInfoBefore)
            assert jobInfoBefore.get(0).get(1) == "RUNNING"

            // Restart FE — currentOffset must replay cleanly from BDBJE editlog + txn attachments.
            cluster.restartFrontends()
            sleep(60000)
            context.reconnectFe()

            def jobInfoAfter = sql """
                select loadStatistic, status, currentOffset from jobs("type"="insert") where Name='${jobName}'
            """
            log.info("jobInfoAfter: " + jobInfoAfter)
            assert jobInfoAfter.get(0).get(1) == "RUNNING"
            assert jobInfoAfter.get(0).get(2) == jobInfoBefore.get(0).get(2) :
                    "currentOffset diverged after restart: before=${jobInfoBefore.get(0).get(2)} after=${jobInfoAfter.get(0).get(2)}"

            // Post-restart binlog still lands.
            connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
                sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (3, 'charlie')"""
            }
            Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                def result = sql """SELECT count(*) FROM ${currentDb}.${table1}"""
                result[0][0] >= 3
            })

            sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
            sql """drop table if exists ${currentDb}.${table1} force"""

            // User-provided slot/pub must survive DROP JOB; clean up manually.
            // After DROP JOB the cdc_client may still be winding down its replication
            // connection — PG rejects pg_drop_replication_slot on an active slot, so
            // poll active=false before issuing the drop.
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
                assert slotStillThere[0][0] == 1 :
                        "user-provided slot ${userSlot} must not be dropped by Doris"
                sql """SELECT pg_drop_replication_slot('${userSlot}')"""
                sql """DROP PUBLICATION IF EXISTS ${userPub}"""
                sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
            }
        }
    }
}
