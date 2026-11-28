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

import org.apache.doris.regression.suite.ClusterOptions

suite("test_streaming_job_cdc_stream_postgres_compact_offset",
        "docker,p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableJdbcTest"))) {
        return
    }

    String pgPort = context.config.otherConfigs.get("pg_14_port")
    String pgHost = context.config.otherConfigs.get("externalEnvIp")
    String pgUrl = "jdbc:postgresql://${pgHost}:${pgPort}/postgres"
    String driverUrl = "https://${getS3BucketName()}.${getS3Endpoint()}/regression/jdbc_driver/postgresql-42.5.0.jar"
    String source = "cdc_test.streaming_job_cdc_stream_postgres_compact_offset_source"
    int snapshotRows = 1500
    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = null

    docker(options) {
        connect("postgres", "123456", pgUrl) {
            sql "DROP TABLE IF EXISTS ${source}"
            sql "CREATE TABLE ${source} (id integer PRIMARY KEY, version integer NOT NULL)"
            sql "INSERT INTO ${source} SELECT id, 0 FROM generate_series(1, ${snapshotRows}) AS g(id)"
        }

        sql "DROP JOB IF EXISTS WHERE jobname = 'pg_tvf_compact_offset'"
        sql "DROP TABLE IF EXISTS pg_tvf_compact_target FORCE"
        sql """
            CREATE TABLE pg_tvf_compact_target (id INT NOT NULL, version INT NOT NULL)
            UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")
        """

        try {
            sql """
                CREATE JOB pg_tvf_compact_offset ON STREAMING DO
                INSERT INTO pg_tvf_compact_target (id, version, __DORIS_DELETE_SIGN__)
                SELECT id, version, __DORIS_DELETE_SIGN__ FROM cdc_stream(
                    "type" = "postgres",
                    "jdbc_url" = "${pgUrl}",
                    "driver_url" = "${driverUrl}",
                    "driver_class" = "org.postgresql.Driver",
                    "user" = "postgres",
                    "password" = "123456",
                    "database" = "postgres",
                    "schema" = "cdc_test",
                    "table" = "streaming_job_cdc_stream_postgres_compact_offset_source",
                    "offset" = "initial",
                    "snapshot_split_size" = "1",
                    "snapshot_parallelism" = "8",
                    "include_delete_sign" = "true"
                )
            """

            def jobId = (sql """SELECT Id FROM jobs("type"="insert") WHERE Name = 'pg_tvf_compact_offset'""")[0][0]
            def readSnapshotProgress = {
                def result = sql """SELECT CurrentOffset FROM jobs("type"="insert")
                                    WHERE Name = 'pg_tvf_compact_offset'"""
                String offset = result[0][0]?.toString()
                if (!offset?.startsWith("[")) {
                    return null
                }
                def entries = parseJson(offset)
                assertEquals(1, entries.size())
                assertEquals("1", entries[0].highWatermark._compact)
                return entries[0]
            }
            def splitNumber = { progress ->
                String id = progress.splitId
                id.substring(id.lastIndexOf(":") + 1).toInteger()
            }

            def firstProgress
            awaitUntil(180, 1) {
                firstProgress = readSnapshotProgress()
                firstProgress != null
            }
            awaitUntil(180, 1) {
                def progress = readSnapshotProgress()
                if (progress == null || splitNumber(progress) <= splitNumber(firstProgress)) {
                    return false
                }
                assertEquals(firstProgress.highWatermark, progress.highWatermark)
                true
            }

            sql "PAUSE JOB WHERE jobname = 'pg_tvf_compact_offset'"
            def pausedProgress = readSnapshotProgress()
            assertNotNull(pausedProgress)
            assertTrue(splitNumber(pausedProgress) < snapshotRows - 1, "Expected unfinished snapshot splits")

            // Change keys from completed splits before continuing the remaining snapshot batches.
            connect("postgres", "123456", pgUrl) {
                sql "UPDATE ${source} SET version = 1 WHERE id = 1"
                sql "DELETE FROM ${source} WHERE id = 2"
                sql "INSERT INTO ${source} VALUES (${snapshotRows + 1}, 2)"
            }

            cluster.restartFrontends()
            context.reconnectFe()
            sql "RESUME JOB WHERE jobname = 'pg_tvf_compact_offset'"

            // Unique keys absorb duplicate delivery. Check the complete final key set and the DML.
            awaitUntil(600, 2) {
                def job = sql """SELECT CurrentOffset FROM jobs("type"="insert")
                                  WHERE Name = 'pg_tvf_compact_offset'"""
                def rows = sql """
                    SELECT COUNT(*), SUM(IF(id BETWEEN 3 AND ${snapshotRows} AND version = 0, 1, 0)),
                           SUM(IF(id = 1 AND version = 1, 1, 0)),
                           SUM(IF(id = 2, 1, 0)),
                           SUM(IF(id = ${snapshotRows + 1} AND version = 2, 1, 0))
                    FROM pg_tvf_compact_target
                """
                job[0][0]?.toString()?.contains('"splitId":"binlog-split"') &&
                        rows[0][0] == snapshotRows && rows[0][1] == snapshotRows - 2 &&
                        rows[0][2] == 1 && rows[0][3] == 0 && rows[0][4] == 1
            }

            def splits = sql """
                SELECT JSON_LENGTH(chunk_list) FROM internal.__internal_schema.streaming_job_meta
                WHERE job_id = '${jobId}'
            """
            // With this table name, 1,200 legacy offset entries would exceed 100 KB.
            assertTrue((splits[0][0] as long) >= 1200, "Expected at least 1,200 snapshot splits")
        } finally {
            sql "DROP JOB IF EXISTS WHERE jobname = 'pg_tvf_compact_offset'"
        }
    }
}
