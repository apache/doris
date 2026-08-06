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

// Regression for the snapshot completion bug: a single split whose row count exceeds
// debezium's default max.batch.size=2048 must still be drained completely. The fix
// keys completion off the high-watermark event instead of the first non-empty batch.
// 2100 rows / split_size=3000 -> one snapshot split that the fetcher needs at least
// two batches to drain (2048 + 52 + hw event).
suite("test_streaming_mysql_job_snapshot_fat_split", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_snapshot_fat_split_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "streaming_fat_split_mysql"
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

        // ===== Prepare MySQL side: 2100 rows so a single split spans > 1 fetcher batch =====
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table1}"""
            sql """CREATE TABLE ${mysqlDb}.${table1} (
                       `id` bigint NOT NULL,
                       `payload` varchar(32),
                       `version` int,
                       PRIMARY KEY (`id`)
                   ) ENGINE=InnoDB"""

            // Bulk insert in 500-row chunks to stay under MySQL's default max_allowed_packet.
            int total = 2100
            int chunk = 500
            int sent = 0
            while (sent < total) {
                int end = Math.min(sent + chunk, total)
                StringBuilder sb = new StringBuilder()
                sb.append("INSERT INTO ${mysqlDb}.${table1} (id, payload, version) VALUES ")
                for (int i = sent + 1; i <= end; i++) {
                    if (i > sent + 1) sb.append(", ")
                    sb.append("(${i}, 'snap', 0)")
                }
                sql sb.toString()
                sent = end
            }
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
                    "offset" = "initial",
                    "snapshot_split_size" = "3000"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                        log.info("snapshot row count: " + cnt)
                        cnt.get(0).get(0) == 2100
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex
        }

        def distinctCount = sql """SELECT COUNT(DISTINCT id) FROM ${currentDb}.${table1}"""
        assert distinctCount.get(0).get(0) == 2100
        def boundary = sql """SELECT MIN(id), MAX(id) FROM ${currentDb}.${table1}"""
        assert boundary.get(0).get(0) == 1
        assert boundary.get(0).get(1) == 2100
        // Specifically assert rows past the first batch (id > 2048) are present.
        def tail = sql """SELECT COUNT(1) FROM ${currentDb}.${table1} WHERE id BETWEEN 2049 AND 2100"""
        assert tail.get(0).get(0) == 52

        // ===== Incremental phase: verify post-snapshot DML still flows =====
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """INSERT INTO ${mysqlDb}.${table1} (id, payload, version) VALUES (3000, 'incr_ins', 1)"""
            sql """UPDATE ${mysqlDb}.${table1} SET version=99 WHERE id=2100"""
            sql """DELETE FROM ${mysqlDb}.${table1} WHERE id=1"""
        }

        try {
            Awaitility.await().atMost(180, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """select count(1) from ${currentDb}.${table1}"""
                        def upd = sql """select version from ${currentDb}.${table1} where id=2100"""
                        def ins = sql """select count(1) from ${currentDb}.${table1} where id=3000"""
                        def del = sql """select count(1) from ${currentDb}.${table1} where id=1"""
                        def v = upd.size() == 0 ? null : upd.get(0).get(0)
                        log.info("incr cnt=${cnt} id2100.version=${v} id3000.exists=${ins} id1.exists=${del}")
                        cnt.get(0).get(0) == 2100 &&
                                v != null && v.toString() == '99' &&
                                ins.get(0).get(0) == 1 &&
                                del.get(0).get(0) == 0
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job (incr): " + showjob)
            log.info("show task (incr): " + showtask)
            throw ex
        }

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
