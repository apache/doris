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

import java.sql.DriverManager
import java.util.concurrent.atomic.AtomicInteger

import static java.util.concurrent.TimeUnit.SECONDS

// Concurrent writes during snapshot: snapshot rows + binlog rows must add up
// exactly, no rows lost on the snapshot->binlog handoff. The new PK range is
// disjoint from the snapshot range so a missing row from either side shows up
// directly as a wrong total.
suite("test_streaming_postgres_job_async_split_concurrent_writes",
        "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_async_split_concurrent_writes_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "user_info_pg_async_split_concurrent"
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"
    def snapshotRows = 300
    def binlogRows = 200

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port = context.config.otherConfigs.get("pg_14_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"
        String pgJdbcUrl = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}"

        // Pre-load snapshot rows. Use varchar PK so we hit the uneven path and snapshot
        // stays slow enough for the concurrent writer to overlap it.
        connect("${pgUser}", "${pgPassword}", "${pgJdbcUrl}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${table1} (
                  "id" varchar(20) PRIMARY KEY,
                  "name" varchar(200)
                )"""
            StringBuilder sb = new StringBuilder()
            sb.append("INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, name) VALUES ")
            for (int i = 1; i <= snapshotRows; i++) {
                if (i > 1) sb.append(", ")
                String key = "k_" + String.format("%05d", i)
                sb.append("('${key}', 'snap_${i}')")
            }
            sql sb.toString()
        }

        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM POSTGRES (
                    "jdbc_url" = "${pgJdbcUrl}",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "org.postgresql.Driver",
                    "user" = "${pgUser}",
                    "password" = "${pgPassword}",
                    "database" = "${pgDB}",
                    "schema" = "${pgSchema}",
                    "include_tables" = "${table1}",
                    "offset" = "initial",
                    "snapshot_split_size" = "5",
                    "snapshot_parallelism" = "2"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        // Concurrent writer: disjoint PK range 'x_...' so we can tell snapshot
        // misses from binlog misses by counting either side independently.
        AtomicInteger writerErr = new AtomicInteger(0)
        Thread writer = Thread.start {
            try {
                Class.forName("org.postgresql.Driver")
                def conn = DriverManager.getConnection(pgJdbcUrl, pgUser, pgPassword)
                conn.setAutoCommit(true)
                try {
                    def stmt = conn.prepareStatement(
                            "INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, name) VALUES (?, ?)" as String)
                    for (int i = 1; i <= binlogRows; i++) {
                        stmt.setString(1, "x_" + String.format("%05d", i))
                        stmt.setString(2, "live_${i}" as String)
                        stmt.executeUpdate()
                        // Spread the writes across ~20s so they straddle snapshot+binlog phases.
                        Thread.sleep(100)
                    }
                    stmt.close()
                } finally {
                    conn.close()
                }
            } catch (Throwable t) {
                writerErr.incrementAndGet()
                log.warn("concurrent writer failed: ${t.message}", t)
            }
        }

        try {
            Awaitility.await().atMost(600, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """SELECT COUNT(*) FROM ${currentDb}.${table1}"""
                        log.info("doris row count: ${cnt}")
                        cnt.size() == 1 && cnt.get(0).get(0) == (snapshotRows + binlogRows)
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex
        }

        writer.join(60_000)
        assert !writer.isAlive() : "concurrent writer did not finish within 60s after rows visible"
        assert writerErr.get() == 0 : "concurrent writer hit error"

        // Per-range count guards against snapshot loss vs binlog loss being averaged out.
        def snapCnt = sql """SELECT COUNT(*) FROM ${currentDb}.${table1} WHERE id LIKE 'k\\_%' ESCAPE '\\\\'"""
        def liveCnt = sql """SELECT COUNT(*) FROM ${currentDb}.${table1} WHERE id LIKE 'x\\_%' ESCAPE '\\\\'"""
        assert snapCnt.get(0).get(0) == snapshotRows :
                "snapshot range got ${snapCnt.get(0).get(0)}, expected ${snapshotRows}"
        assert liveCnt.get(0).get(0) == binlogRows :
                "binlog range got ${liveCnt.get(0).get(0)}, expected ${binlogRows}"

        def distinctCnt = sql """SELECT COUNT(DISTINCT id) FROM ${currentDb}.${table1}"""
        assert distinctCnt.get(0).get(0) == (snapshotRows + binlogRows) :
                "DISTINCT ${distinctCnt.get(0).get(0)} != ${snapshotRows + binlogRows} — chunks duplicated"

        def status = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
        assert status.get(0).get(0) == "RUNNING"

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    }
}
