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

// Verifies the mode gate: with offset=latest, scheduler must NOT trigger any
// fetchSplits RPC even after FE replay. Existing source rows are skipped;
// only binlog changes after CREATE flow through.
suite("test_streaming_postgres_job_async_split_latest", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_async_split_latest_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "user_info_pg_async_split_latest"
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

        // Pre-existing rows that must NOT be synced (offset=latest skips snapshot).
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${table1} (
                  "id" int4 PRIMARY KEY,
                  "name" varchar(200)
                )"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (1, 'pre_1'), (2, 'pre_2'), (3, 'pre_3')"""
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
                    "include_tables" = "${table1}",
                    "offset" = "latest"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """
        long createElapsedMs = System.currentTimeMillis() - createStartMs
        log.info("CREATE JOB elapsed: ${createElapsedMs} ms")
        assert createElapsedMs < 30_000 :
                "CREATE JOB latest mode should return quickly, took ${createElapsedMs} ms"

        // Wait for job to reach RUNNING (binlog phase) without scanning any snapshot row.
        Awaitility.await().atMost(60, SECONDS)
                .pollInterval(1, SECONDS).until(
                {
                    def res = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
                    res.size() == 1 && res.get(0).get(0) == "RUNNING"
                }
        )

        // INSERT a new row after CREATE; binlog must pick it up.
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} VALUES (101, 'after_latest')"""
        }

        Awaitility.await().atMost(120, SECONDS)
                .pollInterval(2, SECONDS).until(
                {
                    def res = sql """SELECT COUNT(*) FROM ${currentDb}.${table1} WHERE id = 101"""
                    res.size() == 1 && res.get(0).get(0) == 1
                }
        )

        // Pre-existing rows (1, 2, 3) must NOT be in Doris: offset=latest skipped snapshot.
        def preCount = sql """SELECT COUNT(*) FROM ${currentDb}.${table1} WHERE id IN (1, 2, 3)"""
        assert preCount.get(0).get(0) == 0 :
                "offset=latest should skip pre-existing rows, but found ${preCount.get(0).get(0)} of them"

        def loadStat = parseJson(sql("""
            select loadStatistic from jobs("type"="insert") where Name='${jobName}'
        """).get(0).get(0))
        log.info("loadStat: ${loadStat}")
        // Only the post-CREATE INSERT should be scanned.
        assert loadStat.scannedRows == 1 :
                "expected scannedRows=1 (only the binlog INSERT), got ${loadStat.scannedRows}"

        sql """DROP JOB IF EXISTS where jobname =  '${jobName}'"""
        def jobCountRsp = sql """select count(1) from jobs("type"="insert")  where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
