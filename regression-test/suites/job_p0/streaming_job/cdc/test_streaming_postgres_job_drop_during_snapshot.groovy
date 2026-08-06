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

// DROP JOB during the snapshot phase (chunks still being dispatched) must still
// clean up auto-managed PG resources. test_streaming_postgres_job_publication only
// covers DROP JOB after the job has reached a steady state — the in-flight drop
// path goes through a different cancel/cleanup branch and historically leaks the
// replication slot if cdc_client cancellation races with the FE-side resource drop.
suite("test_streaming_postgres_job_drop_during_snapshot",
        "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_pg_drop_during_snapshot_job"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "drop_during_snapshot_pg_tbl"
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"
    def totalRows = 300

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port = context.config.otherConfigs.get("pg_14_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${table1} (
                  "id" varchar(20) PRIMARY KEY,
                  "name" varchar(200)
                )"""
            StringBuilder sb = new StringBuilder()
            sb.append("INSERT INTO ${pgDB}.${pgSchema}.${table1} (id, name) VALUES ")
            for (int i = 1; i <= totalRows; i++) {
                if (i > 1) sb.append(", ")
                String key = "k_" + String.format("%05d", i)
                sb.append("('${key}', 'name_${i}')")
            }
            sql sb.toString()
        }

        // Small split_size + single parallelism makes the snapshot slow enough that
        // we can reliably catch it mid-flight before DROP.
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
                    "snapshot_parallelism" = "1"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        // Capture jobId for derived slot/publication names (matches the
        // doris_cdc_${jobId} / doris_pub_${jobId} pattern in DataSourceConfigKeys).
        def jobRow = sql """select Id from jobs("type"="insert") where Name='${jobName}'"""
        assert jobRow.size() == 1 : "job did not register"
        def jobId = jobRow.get(0).get(0).toString()
        def expectedSlot = "doris_cdc_${jobId}"
        def expectedPub = "doris_pub_${jobId}"

        // Step 1: catch the job mid-snapshot. Confirm slot+publication actually exist
        // on PG before issuing DROP — otherwise the cleanup assertion below is vacuous.
        try {
            Awaitility.await().atMost(180, SECONDS).pollInterval(1, SECONDS).until({
                def succeed = sql """select SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}'"""
                def cnt = sql """SELECT COUNT(*) FROM ${currentDb}.${table1}"""
                log.info("pre-drop succeed=${succeed} rows=${cnt}")
                succeed.size() == 1 &&
                        Integer.parseInt(succeed.get(0).get(0).toString()) >= 2 &&
                        cnt.get(0).get(0) < totalRows
            })
        } catch (Exception ex) {
            log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            throw ex
        }
        def rowsBeforeDrop = sql("""SELECT COUNT(*) FROM ${currentDb}.${table1}""").get(0).get(0) as int
        assert rowsBeforeDrop < totalRows : "snapshot finished before we could DROP — case is moot"

        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            def slotBefore = sql """SELECT COUNT(1) FROM pg_replication_slots WHERE slot_name = '${expectedSlot}'"""
            def pubBefore = sql """SELECT COUNT(1) FROM pg_publication WHERE pubname = '${expectedPub}'"""
            log.info("pre-drop pg: slot=${slotBefore} pub=${pubBefore}")
            assert slotBefore[0][0] == 1 : "auto slot must exist before DROP — guard against fixture regression"
            assert pubBefore[0][0] == 1 : "auto publication must exist before DROP"
        }

        // Step 2: DROP while snapshot tasks are still being dispatched.
        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

        // Step 3: slot and publication must both be cleaned up despite in-flight task
        // cancellation. Polling avoids flakiness on slower environments where the
        // FE drop-resources phase runs asynchronously with task cancel.
        Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).untilAsserted {
            connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
                def slotAfter = sql """SELECT COUNT(1) FROM pg_replication_slots WHERE slot_name = '${expectedSlot}'"""
                def pubAfter = sql """SELECT COUNT(1) FROM pg_publication WHERE pubname = '${expectedPub}'"""
                log.info("post-drop pg: slot=${slotAfter} pub=${pubAfter}")
                assert slotAfter[0][0] == 0 : "slot ${expectedSlot} leaked after DROP during snapshot"
                assert pubAfter[0][0] == 0 : "publication ${expectedPub} leaked after DROP during snapshot"
            }
        }

        // Step 4: job row must be gone on FE side.
        def jobCount = sql """select count(1) from jobs("type"="insert") where Name = '${jobName}'"""
        assert jobCount.get(0).get(0) == 0 : "job row not removed after DROP"

        // Cleanup
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
        }
        sql """drop table if exists ${currentDb}.${table1} force"""
    }
}
