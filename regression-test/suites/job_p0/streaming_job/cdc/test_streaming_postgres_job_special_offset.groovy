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

suite("test_streaming_postgres_job_special_offset", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_pg_job_special_offset"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "special_offset_pg_tbl"
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port = context.config.otherConfigs.get("pg_14_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        // prepare PG source table
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${table1} (
                  "id" int,
                  "name" varchar(100),
                  PRIMARY KEY ("id")
                )"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (1, 'alice'), (2, 'bob')"""
        }

        // ===== Test 1: offset = latest, then insert new data, verify synced =====
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
                    "offset" = "latest"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """
        Awaitility.await().atMost(60, SECONDS).pollInterval(2, SECONDS).until({
            def jobStatus = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
            return jobStatus.size() == 1 && jobStatus[0][0] == "RUNNING"
        })
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (3, 'charlie')"""
        }
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
            def result = sql """SELECT count(*) FROM ${currentDb}.${table1}"""
            return result[0][0] >= 1
        })
        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        sql """drop table if exists ${currentDb}.${table1} force"""

        // ===== Test 2: CREATE with initial, then ALTER with JSON LSN offset =====
        // Pre-create a DUPLICATE KEY table so duplicate rows from re-consuming are visible
        sql """
            CREATE TABLE IF NOT EXISTS ${currentDb}.${table1} (
                `id` int NULL,
                `name` varchar(100) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS AUTO
            PROPERTIES ("replication_allocation" = "tag.location.default: 1")
        """

        // Step 1: Create job with initial to establish replication slot and sync snapshot
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
                    "offset" = "initial"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
            def result = sql """SELECT count(*) FROM ${currentDb}.${table1}"""
            return result[0][0] >= 2
        })
        qt_select_after_create """ SELECT * FROM ${currentDb}.${table1} ORDER BY id """

        // Wait for current task to complete (commit offset successfully) before PAUSE,
        // otherwise PAUSE may race with a running task and cause commit offset failure.
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
            def cnt = sql """select SucceedTaskCount from jobs("type"="insert") where Name='${jobName}' and ExecuteType='STREAMING'"""
            return cnt.size() == 1 && (cnt.get(0).get(0) as int) >= 2
        })

        // Step 2: PAUSE, insert data before and after a LSN mark, ALTER to that LSN
        sql "PAUSE JOB where jobname = '${jobName}'"
        Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).until({
            def jobStatus = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
            return jobStatus[0][0] == "PAUSED"
        })
        def alterLsn = ""
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            // insert data BEFORE the LSN mark
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (20, 'before_lsn')"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (21, 'before_lsn')"""
            // record LSN mark
            def lsnResult = sql """SELECT pg_current_wal_lsn()::text"""
            def lsnStr = lsnResult[0][0].toString()
            def parts = lsnStr.split("/")
            def high = Long.parseLong(parts[0], 16)
            def low = Long.parseLong(parts[1], 16)
            alterLsn = String.valueOf((high << 32) + low)
            log.info("ALTER LSN mark: ${lsnStr} -> numeric: ${alterLsn}")
            // insert data AFTER the LSN mark
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (30, 'after_lsn')"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (31, 'after_lsn')"""
        }
        def alterOffsetJson = """{"lsn":"${alterLsn}"}"""
        log.info("ALTER to LSN: ${alterOffsetJson}")
        sql """ALTER JOB ${jobName}
                PROPERTIES('offset' = '${alterOffsetJson}')
            """
        sql "RESUME JOB where jobname = '${jobName}'"
        // After ALTER to LSN mark, only data AFTER that LSN (id 30,31) should be synced
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
            def result = sql """SELECT count(*) FROM ${currentDb}.${table1} WHERE id IN (30, 31)"""
            return result[0][0] >= 2
        })
        qt_select_after_alter """ SELECT * FROM ${currentDb}.${table1} ORDER BY id """

        // Step 3: ALTER with named mode should fail for CDC
        sql "PAUSE JOB where jobname = '${jobName}'"
        Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).until({
            def jobStatus = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
            return jobStatus[0][0] == "PAUSED"
        })
        test {
            sql """ALTER JOB ${jobName}
                    PROPERTIES('offset' = 'initial')
                """
            exception "ALTER JOB for CDC only supports JSON specific offset"
        }
        // ALTER offset via source properties should be rejected
        test {
            sql """ALTER JOB ${jobName}
                    FROM POSTGRES (
                        "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}",
                        "driver_url" = "${driver_url}",
                        "driver_class" = "org.postgresql.Driver",
                        "user" = "${pgUser}",
                        "password" = "${pgPassword}",
                        "database" = "${pgDB}",
                        "schema" = "${pgSchema}",
                        "include_tables" = "${table1}",
                        "offset" = "latest"
                    )
                    TO DATABASE ${currentDb} (
                      "table.create.properties.replication_num" = "1"
                    )
                """
            exception "The offset in source properties cannot be modified in ALTER JOB"
        }
        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        sql """drop table if exists ${currentDb}.${table1} force"""

        // ===== Test 3: earliest should fail for PG =====
        test {
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
                    "offset" = "earliest"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """
            exception "Invalid value for key 'offset'"
        }

        // ===== Test 4: invalid offset format =====
        test {
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
                    "offset" = "not_valid"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """
            exception "Invalid value for key 'offset'"
        }

        // cleanup PG source table
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
        }
    }
}
