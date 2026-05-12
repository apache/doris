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

// Verifies async chunk splitting end-to-end for the cdc_stream TVF path: CREATE returns
// quickly, snapshot fully syncs across multiple splits, and the binlog phase still works.
suite("test_streaming_job_cdc_stream_postgres_async_split",
        "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_job_cdc_stream_postgres_async_split"
    def currentDb = (sql "select database()")[0][0]
    def dorisTable = "test_streaming_job_cdc_stream_postgres_async_split_tbl"
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"
    def pgTable = "test_streaming_job_cdc_stream_postgres_async_split_src"
    def totalRows = 100

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${dorisTable} force"""

    sql """
        CREATE TABLE IF NOT EXISTS ${currentDb}.${dorisTable} (
            `id`   int NULL,
            `name` varchar(200) NULL,
            `age`  int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS AUTO
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port = context.config.otherConfigs.get("pg_14_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        // Prepare 100 rows in PG; snapshot_split_size=5 -> 20 splits across multiple RPCs.
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${pgTable}"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${pgTable} (
                  "id" int4 PRIMARY KEY,
                  "name" varchar(200),
                  "age" int4
                )"""
            StringBuilder sb = new StringBuilder()
            sb.append("INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (id, name, age) VALUES ")
            for (int i = 1; i <= totalRows; i++) {
                if (i > 1) sb.append(", ")
                sb.append("(${i}, 'name_${i}', ${i})")
            }
            sql sb.toString()
        }

        // CREATE should return promptly: splitting is now driven by the scheduler each tick.
        long createStartMs = System.currentTimeMillis()
        sql """
            CREATE JOB ${jobName}
            ON STREAMING DO INSERT INTO ${currentDb}.${dorisTable} (id, name, age)
            SELECT id, name, age FROM cdc_stream(
                "type"               = "postgres",
                "jdbc_url"           = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}",
                "driver_url"         = "${driver_url}",
                "driver_class"       = "org.postgresql.Driver",
                "user"               = "${pgUser}",
                "password"           = "${pgPassword}",
                "database"           = "${pgDB}",
                "schema"             = "${pgSchema}",
                "table"              = "${pgTable}",
                "offset"             = "initial",
                "snapshot_split_size"  = "5",
                "snapshot_parallelism" = "4"
            )
        """
        long createElapsedMs = System.currentTimeMillis() - createStartMs
        log.info("CREATE JOB elapsed: ${createElapsedMs} ms")
        assert createElapsedMs < 30_000 :
                "TVF CREATE should return quickly under async splitting, took ${createElapsedMs} ms"

        // Job metadata should be visible immediately.
        def jobRows = sql """select Name from jobs("type"="insert") where Name='${jobName}'"""
        assert jobRows.size() == 1

        // Wait until snapshot fully syncs (20 splits at parallelism=4 -> ~5 task ticks).
        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """SELECT COUNT(*) FROM ${currentDb}.${dorisTable}"""
                        log.info("doris row count: ${cnt}")
                        cnt.size() == 1 && cnt.get(0).get(0) == totalRows
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex
        }

        // Distinct PK count must equal total — no duplicates from dedup / retry paths.
        def doneDistinct = sql """SELECT COUNT(DISTINCT id) FROM ${currentDb}.${dorisTable}"""
        assert doneDistinct.get(0).get(0) == totalRows

        // Verify binlog phase: INSERT/UPDATE/DELETE all propagate after snapshot.
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (id, name, age) VALUES (101, 'incr_101', 101);"""
            sql """UPDATE ${pgDB}.${pgSchema}.${pgTable} SET age = 999 WHERE id = 1;"""
            sql """DELETE FROM ${pgDB}.${pgSchema}.${pgTable} WHERE id = 2;"""
        }

        try {
            Awaitility.await().atMost(120, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def res = sql """SELECT
                                COUNT(*),
                                SUM(CASE WHEN id=1 AND age=999 THEN 1 ELSE 0 END),
                                SUM(CASE WHEN id=101 THEN 1 ELSE 0 END),
                                SUM(CASE WHEN id=2 THEN 1 ELSE 0 END)
                            FROM ${currentDb}.${dorisTable}"""
                        log.info("binlog state: ${res}")
                        res.size() == 1
                                && res.get(0).get(0) == totalRows
                                && res.get(0).get(1) == 1
                                && res.get(0).get(2) == 1
                                && res.get(0).get(3) == 0
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            log.info("show job: " + showjob)
            throw ex
        }

        def jobInfo = sql """
            select status from jobs("type"="insert") where Name='${jobName}'
        """
        assert jobInfo.get(0).get(0) == "RUNNING"

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
