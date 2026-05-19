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

// Guard MySQL ENUM and SET column fidelity across cdc snapshot + binlog paths.
//
// Why this matters:
//   MySQL stores ENUM as a 1-based index and SET as a bitmap in binlog.
//   The connector MUST resolve those back to label strings against the table
//   schema, otherwise downstream sees "1"/"2"/"3" instead of value names.
//   all_type already inserts ENUM='Value1' and SET='Option1' single values
//   but does NOT cover: max-index ENUM, multi-element SET, full-bitmap SET,
//   empty SET, dedup/reorder of SET input, NULL, or UPDATE paths.
//
// Coverage:
//   - ENUM at first / middle / last index, plus NULL.
//   - SET single element / multi-element / full bitmap / empty string '' /
//     repeated input ('audit,read,admin,read' should be normalized).
//   - snapshot ids 1..5 then binlog ids 101..106 repeat the same themes
//     plus a binlog-only "dedup_reorder" probe.
//   - UPDATE on ENUM (cross multiple indices) and SET (shrink / grow).
suite("test_streaming_mysql_job_enum_set", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_enum_set_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "streaming_enum_set_pk"
    def mysqlDb = "test_cdc_enum_set_db"

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
              `status` enum('pending', 'active', 'closed', 'deleted'),
              `perms` set('read', 'write', 'admin', 'audit')
            ) engine=innodb default charset=utf8;
            """

            // ----- Snapshot rows: 5 ENUM/SET themes via JDBC path -----
            // first_value:  ENUM index 1, SET single element bit 0
            sql """insert into ${mysqlDb}.${table1} values (1, 'first_value',  'pending',  'read')"""
            // middle_value: ENUM mid index, SET two-element bitmap
            sql """insert into ${mysqlDb}.${table1} values (2, 'middle_value', 'active',   'read,write')"""
            // last_value:   ENUM last index, SET full bitmap (all four bits set)
            sql """insert into ${mysqlDb}.${table1} values (3, 'last_value',   'deleted',  'read,write,admin,audit')"""
            // empty_set:    ENUM NULL, SET empty string '' (empty bitmap, NOT NULL)
            sql """insert into ${mysqlDb}.${table1} (id, tag, perms) values (4, 'empty_set', '')"""
            // sql_null:     ENUM NULL, SET NULL
            sql """insert into ${mysqlDb}.${table1} (id, tag) values (5, 'sql_null')"""
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

        // Key contract: status returns 'pending'/'active'/etc., NOT '1'/'2'/'3'.
        qt_desc_enum_set """desc ${currentDb}.${table1};"""
        qt_select_snapshot """select id, tag, status, perms from ${currentDb}.${table1} order by id;"""

        // ===== Binlog phase: repeat the SAME 5 themes through binlog path =====
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """insert into ${mysqlDb}.${table1} values (101, 'first_value',  'pending',  'read')"""
            sql """insert into ${mysqlDb}.${table1} values (102, 'middle_value', 'active',   'read,write')"""
            sql """insert into ${mysqlDb}.${table1} values (103, 'last_value',   'deleted',  'read,write,admin,audit')"""
            sql """insert into ${mysqlDb}.${table1} (id, tag, perms) values (104, 'empty_set', '')"""
            sql """insert into ${mysqlDb}.${table1} (id, tag) values (105, 'sql_null')"""

            // SET dedup + reorder probe (binlog-only). MySQL normalizes the
            // input 'audit,read,admin,read' to declaration order with dedup:
            //   declaration order is ('read', 'write', 'admin', 'audit')
            //   bits set: read=1, admin=4, audit=8 -> bitmap = 13
            //   canonical text: 'read,admin,audit'
            // The binlog event carries the bitmap, so cdc must resolve it
            // back to the canonical comma-separated label string.
            sql """insert into ${mysqlDb}.${table1} values (106, 'dedup_reorder', 'closed', 'audit,read,admin,read')"""

            // UPDATEs: validate UPDATE binlog parsing on ENUM/SET fields.
            //   id=1 (first) -> status crosses multiple indices (1 -> 4)
            //   id=3 (last)  -> perms shrinks from full bitmap -> single element
            //   id=4 (empty) -> perms grows from '' -> 'read,audit'
            sql """update ${mysqlDb}.${table1} set status='deleted' where id=1"""
            sql """update ${mysqlDb}.${table1} set perms='admin' where id=3"""
            sql """update ${mysqlDb}.${table1} set perms='read,audit' where id=4"""
        }

        // Wait until all 6 binlog inserts and 3 updates are visible.
        try {
            Awaitility.await().atMost(180, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                        def upd1 = sql """select status from ${currentDb}.${table1} where id=1"""
                        def upd3 = sql """select perms  from ${currentDb}.${table1} where id=3"""
                        def upd4 = sql """select perms  from ${currentDb}.${table1} where id=4"""
                        def s1 = upd1.get(0).get(0) == null ? '' : upd1.get(0).get(0).toString()
                        def p3 = upd3.get(0).get(0) == null ? '' : upd3.get(0).get(0).toString()
                        def p4 = upd4.get(0).get(0) == null ? '' : upd4.get(0).get(0).toString()
                        log.info("incr count=" + cnt + " id1.status=" + s1 + " id3.perms=" + p3 + " id4.perms=" + p4)
                        cnt.get(0).get(0) == 11 &&
                                s1 == 'deleted' &&
                                p3 == 'admin' &&
                                p4.contains('read') && p4.contains('audit')
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job (incr): " + showjob)
            log.info("show task (incr): " + showtask)
            throw ex
        }

        qt_select_after_incr """select id, tag, status, perms from ${currentDb}.${table1} order by id;"""

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
