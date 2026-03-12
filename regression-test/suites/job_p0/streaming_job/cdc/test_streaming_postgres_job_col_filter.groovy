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

suite("test_streaming_postgres_job_col_filter", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_pg_col_filter"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "user_info_pg_col_filter"
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """DROP JOB IF EXISTS where jobname = '${jobName}_err1'"""
    sql """DROP JOB IF EXISTS where jobname = '${jobName}_err2'"""
    sql """drop table if exists ${currentDb}.${table1} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port = context.config.otherConfigs.get("pg_14_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        // Create PG table with an extra "secret" column to be excluded
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${table1} (
                      "name"   varchar(200),
                      "age"    int2,
                      "secret" varchar(200),
                      PRIMARY KEY ("name")
                    )"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES ('A1', 1, 'secret_A1')"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES ('B1', 2, 'secret_B1')"""
        }

        // ── Validation: exclude a non-existent column should fail ──────────────
        try {
            sql """CREATE JOB ${jobName}_err1
                    ON STREAMING
                    FROM POSTGRES (
                        "jdbc_url"       = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}",
                        "driver_url"     = "${driver_url}",
                        "driver_class"   = "org.postgresql.Driver",
                        "user"           = "${pgUser}",
                        "password"       = "${pgPassword}",
                        "database"       = "${pgDB}",
                        "schema"         = "${pgSchema}",
                        "include_tables" = "${table1}",
                        "offset"         = "initial",
                        "table.${table1}.exclude_columns" = "nonexistent_col"
                    )
                    TO DATABASE ${currentDb} (
                        "table.create.properties.replication_num" = "1"
                    )"""
            assert false : "Should have thrown exception for non-existent excluded column"
        } catch (Exception e) {
            log.info("Expected error for non-existent column: " + e.message)
            assert e.message.contains("does not exist") : "Unexpected error message: " + e.message
        }

        // ── Validation: exclude a PK column should fail ────────────────────────
        try {
            sql """CREATE JOB ${jobName}_err2
                    ON STREAMING
                    FROM POSTGRES (
                        "jdbc_url"       = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}",
                        "driver_url"     = "${driver_url}",
                        "driver_class"   = "org.postgresql.Driver",
                        "user"           = "${pgUser}",
                        "password"       = "${pgPassword}",
                        "database"       = "${pgDB}",
                        "schema"         = "${pgSchema}",
                        "include_tables" = "${table1}",
                        "offset"         = "initial",
                        "table.${table1}.exclude_columns" = "name"
                    )
                    TO DATABASE ${currentDb} (
                        "table.create.properties.replication_num" = "1"
                    )"""
            assert false : "Should have thrown exception for excluding PK column"
        } catch (Exception e) {
            log.info("Expected error for PK column: " + e.message)
            assert e.message.contains("primary key") : "Unexpected error message: " + e.message
        }

        // ── Main job: exclude "secret" column ──────────────────────────────────
        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM POSTGRES (
                    "jdbc_url"       = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}",
                    "driver_url"     = "${driver_url}",
                    "driver_class"   = "org.postgresql.Driver",
                    "user"           = "${pgUser}",
                    "password"       = "${pgPassword}",
                    "database"       = "${pgDB}",
                    "schema"         = "${pgSchema}",
                    "include_tables" = "${table1}",
                    "offset"         = "initial",
                    "table.${table1}.exclude_columns" = "secret"
                )
                TO DATABASE ${currentDb} (
                    "table.create.properties.replication_num" = "1"
                )"""

        // Verify Doris table was created WITHOUT the excluded column
        def colNames = (sql """desc ${currentDb}.${table1}""").collect { it[0] }
        assert !colNames.contains("secret") : "Excluded column 'secret' must not appear in Doris table"
        assert colNames.contains("name")
        assert colNames.contains("age")

        // Wait for snapshot to complete
        try {
            Awaitility.await().atMost(300, SECONDS).pollInterval(1, SECONDS).until({
                def cnt = sql """select SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING'"""
                cnt.size() == 1 && cnt.get(0).get(0).toLong() >= 2
            })
        } catch (Exception ex) {
            def showJob  = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showTask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showJob)
            log.info("show task: " + showTask)
            throw ex
        }

        // Snapshot: only name and age, secret absent
        qt_select_snapshot """ SELECT * FROM ${table1} ORDER BY name ASC """

        // ── Incremental DML: secret values must not appear in Doris ───────────
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES ('C1', 3, 'secret_C1')"""
            sql """UPDATE ${pgDB}.${pgSchema}.${table1} SET age = 20, secret = 'updated_secret' WHERE name = 'B1'"""
            sql """DELETE FROM ${pgDB}.${pgSchema}.${table1} WHERE name = 'A1'"""
        }
        // Wait until C1 appears and A1 is gone
        try {
            Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                def names = (sql """ SELECT name FROM ${table1} ORDER BY name ASC """).collect { it[0] }
                names.contains('C1') && !names.contains('A1')
            })
        } catch (Exception ex) {
            def showJob  = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showTask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showJob)
            log.info("show task: " + showTask)
            throw ex
        }

        qt_select_incremental """ SELECT * FROM ${table1} ORDER BY name ASC """

        // ── Schema change: DROP excluded column → DDL skipped, sync continues ─
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """ALTER TABLE ${pgDB}.${pgSchema}.${table1} DROP COLUMN secret"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (name, age) VALUES ('D1', 4)"""
        }
        // Wait until D1 appears (schema change was skipped, sync should continue normally)
        try {
            Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                def names = (sql """ SELECT name FROM ${table1} ORDER BY name ASC """).collect { it[0] }
                names.contains('D1')
            })
        } catch (Exception ex) {
            def showJob  = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showTask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showJob)
            log.info("show task: " + showTask)
            throw ex
        }

        // Doris table still has no secret column (DDL was skipped)
        def colNamesAfterDrop = (sql """desc ${currentDb}.${table1}""").collect { it[0] }
        assert !colNamesAfterDrop.contains("secret") : "secret column must not appear in Doris after DROP"

        qt_select_after_drop_excluded """ SELECT * FROM ${table1} ORDER BY name ASC """

        // ── Schema change: re-ADD excluded column → DDL also skipped ──────────
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """ALTER TABLE ${pgDB}.${pgSchema}.${table1} ADD COLUMN secret varchar(200)"""
            sql """UPDATE ${pgDB}.${pgSchema}.${table1} SET secret = 're_secret_C1' WHERE name = 'C1'"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (name, age, secret) VALUES ('E1', 5, 'secret_E1')"""
        }
        // Wait until E1 appears (re-ADD DDL skipped, sync should continue normally)
        try {
            Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                def names = (sql """ SELECT name FROM ${table1} ORDER BY name ASC """).collect { it[0] }
                names.contains('E1')
            })
        } catch (Exception ex) {
            def showJob  = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showTask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showJob)
            log.info("show task: " + showTask)
            throw ex
        }

        // Doris table still does NOT have secret column (ADD DDL skipped)
        def colNamesAfterReAdd = (sql """desc ${currentDb}.${table1}""").collect { it[0] }
        assert !colNamesAfterReAdd.contains("secret") : "secret column must not appear in Doris after re-ADD"

        qt_select_after_readd_excluded """ SELECT * FROM ${table1} ORDER BY name ASC """

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name = '${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
