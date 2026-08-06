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

// Verifies cross-table progression of currentSplittingTable: 3 tables, mixed PK types
// (int4 -> evenly path, varchar -> uneven path), each must finish fully before the
// next starts splitting; binlog phase then drains DML for every table.
suite("test_streaming_postgres_job_async_split_multi_table",
        "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_async_split_multi_table_name"
    def currentDb = (sql "select database()")[0][0]
    def t1 = "user_info_pg_async_split_mt_a"        // int4 PK, evenly path
    def t2 = "user_info_pg_async_split_mt_b"        // varchar PK, uneven path
    def t3 = "user_info_pg_async_split_mt_c"        // int4 PK, evenly path
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"
    def rowsA = 300
    def rowsB = 200
    def rowsC = 250

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${t1} force"""
    sql """drop table if exists ${currentDb}.${t2} force"""
    sql """drop table if exists ${currentDb}.${t3} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port = context.config.otherConfigs.get("pg_14_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${t1}"""
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${t2}"""
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${t3}"""

            sql """CREATE TABLE ${pgDB}.${pgSchema}.${t1} (
                      "id" int4 PRIMARY KEY,
                      "name" varchar(200)
                  )"""
            StringBuilder sb1 = new StringBuilder("INSERT INTO ${pgDB}.${pgSchema}.${t1} (id, name) VALUES ")
            for (int i = 1; i <= rowsA; i++) {
                if (i > 1) sb1.append(", ")
                sb1.append("(${i}, 'a_${i}')")
            }
            sql sb1.toString()

            sql """CREATE TABLE ${pgDB}.${pgSchema}.${t2} (
                      "id" varchar(20) PRIMARY KEY,
                      "name" varchar(200)
                  )"""
            StringBuilder sb2 = new StringBuilder("INSERT INTO ${pgDB}.${pgSchema}.${t2} (id, name) VALUES ")
            for (int i = 1; i <= rowsB; i++) {
                if (i > 1) sb2.append(", ")
                String key = "k_" + String.format("%05d", i)
                sb2.append("('${key}', 'b_${i}')")
            }
            sql sb2.toString()

            sql """CREATE TABLE ${pgDB}.${pgSchema}.${t3} (
                      "id" int4 PRIMARY KEY,
                      "name" varchar(200)
                  )"""
            StringBuilder sb3 = new StringBuilder("INSERT INTO ${pgDB}.${pgSchema}.${t3} (id, name) VALUES ")
            for (int i = 1; i <= rowsC; i++) {
                if (i > 1) sb3.append(", ")
                sb3.append("(${i}, 'c_${i}')")
            }
            sql sb3.toString()
        }

        long createStartMs = System.currentTimeMillis()
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
                    "include_tables" = "${t1},${t2},${t3}",
                    "offset" = "initial",
                    "snapshot_split_size" = "10",
                    "snapshot_parallelism" = "4"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """
        long createElapsedMs = System.currentTimeMillis() - createStartMs
        log.info("CREATE JOB elapsed (3 tables): ${createElapsedMs} ms")

        def jobRows = sql """select Name from jobs("type"="insert") where Name='${jobName}'"""
        assert jobRows.size() == 1

        try {
            Awaitility.await().atMost(600, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def a = sql """SELECT COUNT(*) FROM ${currentDb}.${t1}"""
                        def b = sql """SELECT COUNT(*) FROM ${currentDb}.${t2}"""
                        def c = sql """SELECT COUNT(*) FROM ${currentDb}.${t3}"""
                        log.info("a=${a.get(0).get(0)} b=${b.get(0).get(0)} c=${c.get(0).get(0)}")
                        a.get(0).get(0) == rowsA && b.get(0).get(0) == rowsB && c.get(0).get(0) == rowsC
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex
        }

        // DISTINCT per table guards against cross-table chunk leakage / dedup misses.
        assert sql("""SELECT COUNT(DISTINCT id) FROM ${currentDb}.${t1}""").get(0).get(0) == rowsA
        assert sql("""SELECT COUNT(DISTINCT id) FROM ${currentDb}.${t2}""").get(0).get(0) == rowsB
        assert sql("""SELECT COUNT(DISTINCT id) FROM ${currentDb}.${t3}""").get(0).get(0) == rowsC

        def loadStat = parseJson(sql("""
            select loadStatistic from jobs("type"="insert") where Name='${jobName}'
        """).get(0).get(0))
        log.info("loadStat after multi-table snapshot: ${loadStat}")
        assert loadStat.scannedRows == (rowsA + rowsB + rowsC)

        // Binlog phase on every table — currentOffset has to drive all 3.
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """INSERT INTO ${pgDB}.${pgSchema}.${t1} (id, name) VALUES (9991, 'a_incr');"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${t2} (id, name) VALUES ('k_99991', 'b_incr');"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${t3} (id, name) VALUES (9993, 'c_incr');"""
        }

        try {
            Awaitility.await().atMost(120, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def a = sql """SELECT count(*) FROM ${currentDb}.${t1} WHERE id=9991"""
                        def b = sql """SELECT count(*) FROM ${currentDb}.${t2} WHERE id='k_99991'"""
                        def c = sql """SELECT count(*) FROM ${currentDb}.${t3} WHERE id=9993"""
                        a.get(0).get(0) == 1 && b.get(0).get(0) == 1 && c.get(0).get(0) == 1
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            log.info("show job: " + showjob)
            throw ex
        }

        def status = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
        assert status.get(0).get(0) == "RUNNING"

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    }
}
