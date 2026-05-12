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

// Verifies async chunk splitting end-to-end: CREATE returns quickly, snapshot
// fully syncs across multiple splits, and the binlog phase still works after.
suite("test_streaming_postgres_job_async_split", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_async_split_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "user_info_pg_async_split"
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"
    def totalRows = 100

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port = context.config.otherConfigs.get("pg_14_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        // Prepare 100 rows in PG; snapshot_split_size=5 -> 20 splits.
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${table1} (
                  "id" int4 PRIMARY KEY,
                  "name" varchar(200),
                  "age" int4
                )"""
            StringBuilder sb = new StringBuilder()
            sb.append("INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, name, age) VALUES ")
            for (int i = 1; i <= totalRows; i++) {
                if (i > 1) sb.append(", ")
                sb.append("(${i}, 'name_${i}', ${i})")
            }
            sql sb.toString()
        }

        // CREATE should return promptly: splitting is now driven by the scheduler
        // each tick, not synchronously inside CREATE. Synchronous splitting on a
        // large table historically could take many minutes; here we just assert
        // the CREATE call itself doesn't block for that long.
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
                    "include_tables" = "${table1}",
                    "offset" = "initial",
                    "snapshot_split_size" = "5",
                    "snapshot_parallelism" = "4"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """
        long createElapsedMs = System.currentTimeMillis() - createStartMs
        log.info("CREATE JOB elapsed: ${createElapsedMs} ms")
        assert createElapsedMs < 30_000 :
                "CREATE JOB should return quickly under async splitting, took ${createElapsedMs} ms"

        // Job metadata should be visible immediately (no blocking on splitChunks).
        def jobRows = sql """select Name from jobs("type"="insert") where Name='${jobName}'"""
        assert jobRows.size() == 1

        // Wait until snapshot fully syncs. 20 splits at snapshot_parallelism=4 ->
        // ~5 task ticks; default max_interval=10s -> ~50s end-to-end. Cap at 5min.
        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def cnt = sql """SELECT COUNT(*) FROM ${currentDb}.${table1}"""
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

        // Distinct PK count must equal total — no duplicates from RPC retry / dedup paths.
        def doneDistinct = sql """SELECT COUNT(DISTINCT id) FROM ${currentDb}.${table1}"""
        assert doneDistinct.get(0).get(0) == totalRows

        def loadStat0 = parseJson(sql("""
            select loadStatistic from jobs("type"="insert") where Name='${jobName}'
        """).get(0).get(0))
        log.info("loadStat after snapshot: ${loadStat0}")
        assert loadStat0.scannedRows == totalRows

        // Snapshot done -> binlog phase. Verify INSERT/UPDATE/DELETE still propagate.
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, name, age) VALUES (101, 'incr_101', 101);"""
            sql """UPDATE ${pgDB}.${pgSchema}.${table1} SET age = 999 WHERE id = 1;"""
            sql """DELETE FROM ${pgDB}.${pgSchema}.${table1} WHERE id = 2;"""
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
                            FROM ${currentDb}.${table1}"""
                        log.info("binlog state: ${res}")
                        // delete(id=2) + insert(id=101) -> count == totalRows
                        // update(id=1)+age=999 -> 1; insert(id=101) -> 1; leftover(id=2) -> 0
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

        // Job should still be RUNNING (snapshot transitioned cleanly to binlog).
        def jobInfo = sql """
            select status from jobs("type"="insert") where Name='${jobName}'
        """
        assert jobInfo.get(0).get(0) == "RUNNING"

        sql """DROP JOB IF EXISTS where jobname =  '${jobName}'"""
        def jobCountRsp = sql """select count(1) from jobs("type"="insert")  where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
