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

// Guard PG UUID column boundaries across cdc snapshot + binlog paths.
//
// Why a dedicated suite:
//   PG stores UUID as 16-byte binary on the wire. wal2json/pgoutput emit it
//   as 36-char hyphenated text. cdc must format it the same way for snapshot
//   (JDBC) and binlog paths. _all_type covers a single normal UUID; this
//   suite adds the boundary values (all-zero, all-ff), array-of-uuid, and
//   NULL handling that customers actually hit (UUID is the de-facto primary
//   key in many overseas PG setups).
//
// snapshot ids 1..5 then binlog ids 101..105 repeat the same themes.
// UPDATE rewrites a few rows to validate UPDATE binlog parsing on UUID.
suite("test_streaming_postgres_job_uuid", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_uuid_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "streaming_uuid_pk"
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

        // ===== Prepare PG side =====
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
            sql """
            create table ${pgDB}.${pgSchema}.${table1} (
                id          integer PRIMARY KEY,
                tag         varchar(64),
                uuid_col    uuid,
                uuid_arr    uuid[]
            );
            """

            // ----- Snapshot rows: 5 UUID boundary themes via JDBC path -----
            // basic: regular UUID value (the kind _all_type uses)
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (1, 'basic',     '11111111-2222-3333-4444-555555555555'::uuid, NULL)"""

            // all_zero: the all-zero UUID (legal but degenerate)
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (2, 'all_zero',  '00000000-0000-0000-0000-000000000000'::uuid, NULL)"""

            // all_ff: the all-ones UUID (16 bytes of 0xff, upper bound of UUID space)
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (3, 'all_ff',    'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid, NULL)"""

            // uuid_array: UUID array column with mixed values including all-zero
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (4, 'uuid_array', NULL, ARRAY['11111111-1111-1111-1111-111111111111'::uuid, '22222222-2222-2222-2222-222222222222'::uuid, '00000000-0000-0000-0000-000000000000'::uuid])"""

            // sql_null: both columns NULL
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, tag) VALUES (5, 'sql_null')"""
        }

        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM POSTGRES (
                    "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}?timezone=UTC",
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

        // Wait for snapshot to land all 5 rows in Doris.
        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                        log.info("snapshot row count: " + cnt)
                        cnt.get(0).get(0) == 5
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex
        }

        // Key contract: UUID columns must surface as the 36-char hyphenated
        // form (lower-case, no surrounding quotes) and arrays preserve order.
        qt_desc_uuid """desc ${currentDb}.${table1};"""
        qt_select_snapshot """select id, tag, uuid_col, uuid_arr from ${currentDb}.${table1} order by id;"""

        // ===== Binlog phase: same UUID boundary themes via wal2json/pgoutput =====
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (101, 'basic',      'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'::uuid, NULL)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (102, 'all_zero',   '00000000-0000-0000-0000-000000000000'::uuid, NULL)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (103, 'all_ff',     'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid, NULL)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (104, 'uuid_array', NULL, ARRAY['33333333-3333-3333-3333-333333333333'::uuid, '44444444-4444-4444-4444-444444444444'::uuid])"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, tag) VALUES (105, 'sql_null')"""

            // UPDATEs: validate UPDATE binlog parsing on UUID fields.
            //   id=1 (basic) -> swap to all-ff
            //   id=2 (all_zero) -> back to a normal UUID
            //   id=5 (sql_null) -> NULL -> non-null UUID
            sql """UPDATE ${pgDB}.${pgSchema}.${table1} SET uuid_col='ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid WHERE id=1"""
            sql """UPDATE ${pgDB}.${pgSchema}.${table1} SET uuid_col='99999999-9999-9999-9999-999999999999'::uuid WHERE id=2"""
            sql """UPDATE ${pgDB}.${pgSchema}.${table1} SET uuid_col='12345678-1234-1234-1234-123456789012'::uuid WHERE id=5"""
        }

        // Wait until all 5 binlog inserts and 3 updates are visible.
        try {
            Awaitility.await().atMost(180, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                        def upd1 = sql """select uuid_col from ${currentDb}.${table1} where id=1"""
                        def upd2 = sql """select uuid_col from ${currentDb}.${table1} where id=2"""
                        def upd5 = sql """select uuid_col from ${currentDb}.${table1} where id=5"""
                        def u1 = upd1.get(0).get(0) == null ? '' : upd1.get(0).get(0).toString()
                        def u2 = upd2.get(0).get(0) == null ? '' : upd2.get(0).get(0).toString()
                        def u5 = upd5.get(0).get(0) == null ? '' : upd5.get(0).get(0).toString()
                        log.info("incr count=" + cnt + " id1.uuid=" + u1 + " id2.uuid=" + u2 + " id5.uuid=" + u5)
                        cnt.get(0).get(0) == 10 &&
                                u1.toLowerCase().startsWith('ffffffff') &&
                                u2.toLowerCase().startsWith('99999999') &&
                                u5.toLowerCase().startsWith('12345678')
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job (incr): " + showjob)
            log.info("show task (incr): " + showtask)
            throw ex
        }

        qt_select_after_incr """select id, tag, uuid_col, uuid_arr from ${currentDb}.${table1} order by id;"""

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
