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

// Verifies async chunk splitting end-to-end for MySQL: CREATE returns quickly,
// snapshot fully syncs across multiple splits, and the binlog phase still works after.
suite("test_streaming_mysql_job_async_split", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_async_split_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "user_info_mysql_async_split"
    def mysqlDb = "test_cdc_db"
    def totalRows = 100

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

        // Prepare 100 rows in MySQL; snapshot_split_size=5 -> 20 splits.
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table1}"""
            sql """CREATE TABLE ${mysqlDb}.${table1} (
                  `id` int NOT NULL,
                  `name` varchar(200),
                  `age` int,
                  PRIMARY KEY (`id`)
                ) ENGINE=InnoDB"""
            StringBuilder sb = new StringBuilder()
            sb.append("INSERT INTO ${mysqlDb}.${table1} (id, name, age) VALUES ")
            for (int i = 1; i <= totalRows; i++) {
                if (i > 1) sb.append(", ")
                sb.append("(${i}, 'name_${i}', ${i})")
            }
            sql sb.toString()
        }

        // CREATE should return promptly: splitting is now driven by the scheduler
        // each tick, not synchronously inside CREATE.
        long createStartMs = System.currentTimeMillis()
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
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """INSERT INTO ${mysqlDb}.${table1} (id, name, age) VALUES (101, 'incr_101', 101);"""
            sql """UPDATE ${mysqlDb}.${table1} SET age = 999 WHERE id = 1;"""
            sql """DELETE FROM ${mysqlDb}.${table1} WHERE id = 2;"""
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
