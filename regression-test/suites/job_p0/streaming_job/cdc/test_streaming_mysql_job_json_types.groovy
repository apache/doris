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

// Guard MySQL JSON column fidelity across both cdc paths:
//   - snapshot (JDBC + information_schema)
//   - binlog event (cdc-client + debezium)
// MySQL JSON maps to Doris `text`, so the contract is roundtrip of common
// JSON shapes that customers hit. Both paths must produce identical output.
//
// Coverage is run TWICE: ids 1-14 cover the snapshot path, ids 101-114
// repeat the same 14 shapes through the binlog path. Plus UPDATEs that
// switch one shape to another to validate UPDATE binlog parsing.
suite("test_streaming_mysql_job_json_types", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_json_types_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "streaming_json_types_pk"
    def mysqlDb = "test_cdc_json_db"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

        // ===== Prepare MySQL side =====
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table1}"""
            sql """
            create table ${mysqlDb}.${table1} (
              `id` int primary key,
              `tag` varchar(64),
              `j` json
            ) engine=innodb default charset=utf8mb4;
            """

            // ----- Snapshot rows: 14 JSON shapes via JDBC path -----
            sql """insert into ${mysqlDb}.${table1} values (1,  'simple_kv',       '{"id":1,"name":"a"}')"""
            sql """insert into ${mysqlDb}.${table1} values (2,  'nested_obj',      '{"a":{"b":{"c":1,"d":[1,2]}}}')"""
            sql """insert into ${mysqlDb}.${table1} values (3,  'array',           '[1,2,3,4,5]')"""
            sql """insert into ${mysqlDb}.${table1} values (4,  'nested_array',    '[[1,2],[3,4],[5,6]]')"""
            sql """insert into ${mysqlDb}.${table1} values (5,  'mixed',           '{"users":[{"id":1,"tags":["a","b"]},{"id":2,"tags":[]}]}')"""
            sql """insert into ${mysqlDb}.${table1} values (6,  'empty_obj',       '{}')"""
            sql """insert into ${mysqlDb}.${table1} values (7,  'empty_arr',       '[]')"""
            sql """insert into ${mysqlDb}.${table1} values (8,  'unicode_chinese', '{"name":"张三","city":"上海"}')"""
            sql """insert into ${mysqlDb}.${table1} values (9,  'emoji',           '{"msg":"Hello 🚀 World 😀"}')"""
            // Build values containing control chars via JSON_OBJECT to dodge SQL escape complexity.
            sql """insert into ${mysqlDb}.${table1} values (10, 'newline_in_value', JSON_OBJECT('text', CONCAT('line1', CHAR(10), 'line2')))"""
            sql """insert into ${mysqlDb}.${table1} values (11, 'quote_in_value',   JSON_OBJECT('text', 'she said "hi"'))"""
            sql """insert into ${mysqlDb}.${table1} values (12, 'scalar_array',    '[true,false,null,1,1.5,"str"]')"""
            sql """insert into ${mysqlDb}.${table1} values (13, 'sql_null',        NULL)"""
            sql """insert into ${mysqlDb}.${table1} values (14, 'json_null',       'null')"""
        }

        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM MYSQL (
                    "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}?serverTimezone=UTC",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "com.mysql.cj.jdbc.Driver",
                    "user" = "root",
                    "password" = "123456",
                    "database" = "${mysqlDb}",
                    "include_tables" = "${table1}",
                    "offset" = "initial"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        // Wait for snapshot to land all 14 rows in Doris.
        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                        log.info("snapshot row count: " + cnt)
                        cnt.get(0).get(0) == 14
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex
        }

        qt_desc_json_types """desc ${currentDb}.${table1};"""
        qt_select_snapshot """select id, tag, j from ${currentDb}.${table1} order by id;"""

        // ===== Binlog phase: repeat the SAME 14 shapes through binlog path =====
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """insert into ${mysqlDb}.${table1} values (101, 'simple_kv',       '{"id":101,"name":"b"}')"""
            sql """insert into ${mysqlDb}.${table1} values (102, 'nested_obj',      '{"a":{"b":{"c":2,"d":[3,4]}}}')"""
            sql """insert into ${mysqlDb}.${table1} values (103, 'array',           '[10,20,30]')"""
            sql """insert into ${mysqlDb}.${table1} values (104, 'nested_array',    '[[7,8],[9,10]]')"""
            sql """insert into ${mysqlDb}.${table1} values (105, 'mixed',           '{"users":[{"id":3,"tags":["c"]},{"id":4,"tags":["d","e"]}]}')"""
            sql """insert into ${mysqlDb}.${table1} values (106, 'empty_obj',       '{}')"""
            sql """insert into ${mysqlDb}.${table1} values (107, 'empty_arr',       '[]')"""
            sql """insert into ${mysqlDb}.${table1} values (108, 'unicode_chinese', '{"name":"李四","city":"北京"}')"""
            sql """insert into ${mysqlDb}.${table1} values (109, 'emoji',           '{"msg":"Bye 👋 👀"}')"""
            sql """insert into ${mysqlDb}.${table1} values (110, 'newline_in_value', JSON_OBJECT('text', CONCAT('abc', CHAR(10), 'def')))"""
            sql """insert into ${mysqlDb}.${table1} values (111, 'quote_in_value',   JSON_OBJECT('text', 'he said "ok"'))"""
            sql """insert into ${mysqlDb}.${table1} values (112, 'scalar_array',    '[false,true,null,42,3.14,"x"]')"""
            sql """insert into ${mysqlDb}.${table1} values (113, 'sql_null',        NULL)"""
            sql """insert into ${mysqlDb}.${table1} values (114, 'json_null',       'null')"""

            // UPDATEs: switch JSON shape via binlog path
            //   id=1: simple_kv -> nested object (verifies UPDATE-on-JSON shape change)
            //   id=8: chinese    -> emoji         (verifies UPDATE on 4-byte UTF-8)
            //   id=13: SQL NULL  -> non-null JSON (verifies NULL -> value transition)
            sql """update ${mysqlDb}.${table1} set j='{"u":{"v":{"w":[1,2,3]}}}' where id=1"""
            sql """update ${mysqlDb}.${table1} set j='{"msg":"after-update 🎉"}' where id=8"""
            sql """update ${mysqlDb}.${table1} set j='{"recovered":true}' where id=13"""
        }

        // Wait until all 14 inserts and 3 updates are visible.
        try {
            Awaitility.await().atMost(180, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                        def upd1 = sql """select j from ${currentDb}.${table1} where id=1"""
                        def upd8 = sql """select j from ${currentDb}.${table1} where id=8"""
                        def upd13 = sql """select j from ${currentDb}.${table1} where id=13"""
                        def j1  = upd1.get(0).get(0) == null ? '' : upd1.get(0).get(0).toString()
                        def j8  = upd8.get(0).get(0) == null ? '' : upd8.get(0).get(0).toString()
                        def j13 = upd13.get(0).get(0) == null ? '' : upd13.get(0).get(0).toString()
                        log.info("incr count=" + cnt + " j1=" + j1 + " j8=" + j8 + " j13=" + j13)
                        cnt.get(0).get(0) == 28 &&
                                j1.contains('"u"') &&
                                j8.contains('after-update') &&
                                j13.contains('recovered')
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job (incr): " + showjob)
            log.info("show task (incr): " + showtask)
            throw ex
        }

        qt_select_after_incr """select id, tag, j from ${currentDb}.${table1} order by id;"""

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
