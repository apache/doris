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

// Guard PG json vs jsonb fidelity across cdc snapshot + binlog paths.
//
// json   : textual storage, preserves whitespace and duplicate keys as-is.
// jsonb  : binary storage, drops insignificant whitespace and dedups keys
//          (last value wins). Most production PG schemas use jsonb.
//
// _all_type already covers simple objects in both columns. This suite covers
// the shapes that actually matter for cdc fidelity:
//   - Nested objects, arrays, and mixed structures
//   - Unicode (Chinese), JSON null vs SQL NULL
//   - Whitespace difference between json and jsonb on the SAME input
//   - Duplicate key handling difference between json and jsonb
//
// snapshot ids 1..7 then binlog ids 101..107 repeat the same themes.
// UPDATE rewrites a few rows to validate UPDATE binlog parsing.
suite("test_streaming_postgres_job_jsonb_types", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_jsonb_types_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "streaming_jsonb_types_pk"
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
                j_json      json,
                j_jsonb     jsonb
            );
            """

            // ----- Snapshot rows: 7 JSON shape themes via JDBC path -----
            // simple_kv: baseline single-level object
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (1, 'simple_kv',  '{"id":1,"name":"a"}',         '{"id":1,"name":"a"}')"""

            // nested: 3-level nested object containing an array
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (2, 'nested',     '{"a":{"b":{"c":1,"d":[1,2]}}}', '{"a":{"b":{"c":1,"d":[1,2]}}}')"""

            // mixed: object containing arrays of objects (real-world shape)
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (3, 'mixed',      '{"users":[{"id":1,"tags":["a","b"]},{"id":2,"tags":[]}]}', '{"users":[{"id":1,"tags":["a","b"]},{"id":2,"tags":[]}]}')"""

            // unicode_chinese: 4-byte UTF-8 / non-ASCII keys+values
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (4, 'unicode',    '{"name":"张三","city":"上海"}', '{"name":"张三","city":"上海"}')"""

            // with_spaces: whitespace differs between json (preserved) and jsonb (canonicalized)
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (5, 'with_spaces','{ "a" : 1 , "b" : 2 }',        '{ "a" : 1 , "b" : 2 }')"""

            // dup_keys: json keeps duplicate keys (last value wins on access);
            // jsonb dedups at parse time and only retains the last value for the key.
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (6, 'dup_keys',   '{"k":1,"k":2}',                '{"k":1,"k":2}')"""

            // sql_null: both columns are SQL NULL
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, tag) VALUES (7, 'sql_null')"""
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

        // Wait for snapshot to land all 7 rows in Doris.
        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                        log.info("snapshot row count: " + cnt)
                        cnt.get(0).get(0) == 7
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex
        }

        qt_desc_jsonb_types """desc ${currentDb}.${table1};"""
        qt_select_snapshot """select id, tag, j_json, j_jsonb from ${currentDb}.${table1} order by id;"""

        // ===== Binlog phase: same shapes through wal2json/pgoutput =====
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (101, 'simple_kv',  '{"id":101,"name":"b"}',       '{"id":101,"name":"b"}')"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (102, 'nested',     '{"a":{"b":{"c":2,"d":[3,4]}}}', '{"a":{"b":{"c":2,"d":[3,4]}}}')"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (103, 'mixed',      '{"users":[{"id":3,"tags":["c"]},{"id":4,"tags":["d","e"]}]}', '{"users":[{"id":3,"tags":["c"]},{"id":4,"tags":["d","e"]}]}')"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (104, 'unicode',    '{"name":"李四","city":"北京"}',   '{"name":"李四","city":"北京"}')"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (105, 'with_spaces','{ "x"  :  10 ,  "y"  :  20 }',  '{ "x"  :  10 ,  "y"  :  20 }')"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (106, 'dup_keys',   '{"k":10,"k":20,"k":30}',         '{"k":10,"k":20,"k":30}')"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, tag) VALUES (107, 'sql_null')"""

            // UPDATEs: validate UPDATE binlog parsing on json/jsonb columns.
            //   id=1 (simple_kv) -> swap to nested shape
            //   id=4 (unicode)   -> swap to emoji content
            //   id=7 (sql_null)  -> NULL -> non-null jsonb
            sql """UPDATE ${pgDB}.${pgSchema}.${table1} SET j_json='{"u":{"v":[1,2,3]}}', j_jsonb='{"u":{"v":[1,2,3]}}' WHERE id=1"""
            sql """UPDATE ${pgDB}.${pgSchema}.${table1} SET j_json='{"msg":"updated 🎉"}', j_jsonb='{"msg":"updated 🎉"}' WHERE id=4"""
            sql """UPDATE ${pgDB}.${pgSchema}.${table1} SET j_jsonb='{"recovered":true}' WHERE id=7"""
        }

        // Wait until all 7 binlog inserts and 3 updates are visible.
        try {
            Awaitility.await().atMost(180, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                        def upd1 = sql """select j_jsonb from ${currentDb}.${table1} where id=1"""
                        def upd4 = sql """select j_jsonb from ${currentDb}.${table1} where id=4"""
                        def upd7 = sql """select j_jsonb from ${currentDb}.${table1} where id=7"""
                        def b1 = upd1.get(0).get(0) == null ? '' : upd1.get(0).get(0).toString()
                        def b4 = upd4.get(0).get(0) == null ? '' : upd4.get(0).get(0).toString()
                        def b7 = upd7.get(0).get(0) == null ? '' : upd7.get(0).get(0).toString()
                        log.info("incr count=" + cnt + " id1.jb=" + b1 + " id4.jb=" + b4 + " id7.jb=" + b7)
                        cnt.get(0).get(0) == 14 &&
                                b1.contains('"u"') &&
                                b4.contains('updated') &&
                                b7.contains('recovered')
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job (incr): " + showjob)
            log.info("show task (incr): " + showtask)
            throw ex
        }

        qt_select_after_incr """select id, tag, j_json, j_jsonb from ${currentDb}.${table1} order by id;"""

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
