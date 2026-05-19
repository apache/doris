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

// Guard PG array column boundaries across cdc snapshot + binlog paths.
//
// PG _all_type already covers a basic single-dimension integer[] / text[] case.
// This suite covers the boundaries that _all_type and _array_types do not:
//   - Array elements containing NULL  '{1,NULL,3}'
//   - Multi-dimensional arrays         '{{1,2},{3,4},{5,6}}'
//   - Empty array '{}' VS SQL NULL    (two flavours of "empty")
//   - Text array elements with commas / quotes that need PG escape
//
// snapshot ids 1..5 then binlog ids 101..105 repeat the same themes.
// UPDATE rewrites a few rows to validate UPDATE binlog parsing on arrays.
suite("test_streaming_postgres_job_array_boundary", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_array_boundary_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "streaming_array_boundary_pk"
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
                id              integer PRIMARY KEY,
                tag             varchar(64),
                int_arr         integer[],
                text_arr        text[],
                multi_dim       integer[],
                empty_or_null   integer[]
            );
            """

            // ----- Snapshot rows: 5 array boundary themes via JDBC path -----
            // basic: regular single-dim arrays
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (1, 'basic',         ARRAY[1,2,3],           ARRAY['a','b','c'], NULL,                       ARRAY[7,8,9])"""

            // null_elements: NULL inside arrays must roundtrip distinctly from missing
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (2, 'null_elements', ARRAY[1,NULL,3],        ARRAY['a',NULL,'c'], NULL,                       NULL)"""

            // multi_dim: PG natively supports multidimensional arrays. Doris may flatten.
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (3, 'multi_dim',     NULL,                    NULL,               ARRAY[[1,2],[3,4],[5,6]],   NULL)"""

            // empty_array: '{}' is the empty array, distinct from SQL NULL
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (4, 'empty_array',   '{}'::integer[],         '{}'::text[],       NULL,                       '{}'::integer[])"""

            // sql_null_array: all arrays are SQL NULL
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, tag) VALUES (5, 'sql_null_array')"""
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

        qt_desc_array_boundary """desc ${currentDb}.${table1};"""
        qt_select_snapshot """select id, tag, int_arr, text_arr, multi_dim, empty_or_null from ${currentDb}.${table1} order by id;"""

        // ===== Binlog phase: repeat same themes via wal2json/pgoutput -----
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (101, 'basic',         ARRAY[10,20,30],         ARRAY['x','y','z'], NULL,                          ARRAY[40,50])"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (102, 'null_elements', ARRAY[NULL,2,NULL],      ARRAY[NULL,'mid',NULL], NULL,                       NULL)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (103, 'multi_dim',     NULL,                     NULL,              ARRAY[[10,20],[30,40]],         NULL)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (104, 'empty_array',   '{}'::integer[],          '{}'::text[],     NULL,                           '{}'::integer[])"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, tag) VALUES (105, 'sql_null_array')"""

            // Extra binlog-only probe: text element containing comma and double quote
            // PG must escape these in the wire format; cdc must unescape.
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (106, 'text_special',  NULL,                     ARRAY['with,comma', 'with"quote', 'plain'], NULL, NULL)"""

            // UPDATEs: validate UPDATE binlog parsing on array columns.
            //   id=1 (basic) -> grow int_arr
            //   id=4 (empty) -> empty array '{}' becomes non-empty
            //   id=5 (sql_null) -> NULL becomes non-null array
            sql """UPDATE ${pgDB}.${pgSchema}.${table1} SET int_arr=ARRAY[100,200,300,400] WHERE id=1"""
            sql """UPDATE ${pgDB}.${pgSchema}.${table1} SET empty_or_null=ARRAY[1,2,3] WHERE id=4"""
            sql """UPDATE ${pgDB}.${pgSchema}.${table1} SET int_arr=ARRAY[9,9,9] WHERE id=5"""
        }

        // Wait until all 6 binlog inserts and 3 updates are visible.
        try {
            Awaitility.await().atMost(180, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                        def upd1 = sql """select int_arr       from ${currentDb}.${table1} where id=1"""
                        def upd4 = sql """select empty_or_null from ${currentDb}.${table1} where id=4"""
                        def upd5 = sql """select int_arr       from ${currentDb}.${table1} where id=5"""
                        def a1 = upd1.get(0).get(0) == null ? '' : upd1.get(0).get(0).toString()
                        def a4 = upd4.get(0).get(0) == null ? '' : upd4.get(0).get(0).toString()
                        def a5 = upd5.get(0).get(0) == null ? '' : upd5.get(0).get(0).toString()
                        log.info("incr count=" + cnt + " id1.int_arr=" + a1 + " id4.eon=" + a4 + " id5.int_arr=" + a5)
                        cnt.get(0).get(0) == 11 &&
                                a1.contains('400') &&
                                a4.contains('1') && a4.contains('2') && a4.contains('3') &&
                                a5.contains('9')
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job (incr): " + showjob)
            log.info("show task (incr): " + showtask)
            throw ex
        }

        qt_select_after_incr """select id, tag, int_arr, text_arr, multi_dim, empty_or_null from ${currentDb}.${table1} order by id;"""

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
