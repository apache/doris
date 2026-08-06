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

// offset=earliest is only valid for MySQL. It skips the snapshot phase entirely and
// replays binlog events from the oldest position available on the server. Two
// invariants this case guards:
//   1. Pre-existing rows whose INSERT events are still in binlog are picked up via
//      the binlog path (no JDBC snapshot is run).
//   2. Subsequent binlog DML (INSERT/UPDATE/DELETE) lands as usual.
// The table name is randomized so binlog events from prior CI runs of this case
// (with the same fixed name) cannot leak in and skew the final state — debezium
// include_tables filtering is the only line of defense and it matches by name.
suite("test_streaming_mysql_job_offset_earliest",
        "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_offset_earliest_name"
    def currentDb = (sql "select database()")[0][0]
    def suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 8)
    def table1 = "earliest_offset_mysql_tbl_${suffix}"
    def mysqlDb = "test_cdc_db"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

        // Two INSERTs land in binlog BEFORE the job exists. earliest must replay them.
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table1}"""
            sql """CREATE TABLE ${mysqlDb}.${table1} (
                  `id` int NOT NULL,
                  `name` varchar(100) DEFAULT NULL,
                  PRIMARY KEY (`id`)
                ) ENGINE=InnoDB"""
            sql """INSERT INTO ${mysqlDb}.${table1} VALUES (1, 'alice')"""
            sql """INSERT INTO ${mysqlDb}.${table1} VALUES (2, 'bob')"""
        }

        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM MYSQL (
                    "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "com.mysql.cj.jdbc.Driver",
                    "user" = "root",
                    "password" = "123456",
                    "database" = "${mysqlDb}",
                    "include_tables" = "${table1}",
                    "offset" = "earliest"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        try {
            Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                def cnt = sql """SELECT count(*) FROM ${currentDb}.${table1}"""
                cnt.size() == 1 && cnt.get(0).get(0) == 2
            })
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex
        }

        qt_select_after_earliest_replay """ SELECT id, name FROM ${currentDb}.${table1} ORDER BY id """

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """INSERT INTO ${mysqlDb}.${table1} VALUES (3, 'charlie')"""
            sql """UPDATE ${mysqlDb}.${table1} SET name='alice_upd' WHERE id=1"""
            sql """DELETE FROM ${mysqlDb}.${table1} WHERE id=2"""
        }

        try {
            Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
                def cnt = sql """SELECT count(*) FROM ${currentDb}.${table1}"""
                def upd = sql """SELECT name FROM ${currentDb}.${table1} WHERE id=1"""
                def del = sql """SELECT count(*) FROM ${currentDb}.${table1} WHERE id=2"""
                def updName = upd.size() == 0 ? null : upd.get(0).get(0)
                log.info("incr cnt=${cnt} upd=${updName} del=${del}")
                cnt.get(0).get(0) == 2 && updName == 'alice_upd' && del.get(0).get(0) == 0
            })
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job (incr): " + showjob)
            log.info("show task (incr): " + showtask)
            throw ex
        }

        qt_select_after_earliest_incr """ SELECT id, name FROM ${currentDb}.${table1} ORDER BY id """

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table1}"""
        }
    }
}
