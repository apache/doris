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

/**
 * Test streaming INSERT job using cdc_stream TVF for PostgreSQL.
 *
 * Scenario:
 *   1. Snapshot phase (offset=initial): pre-existing rows (A1, B1) are synced.
 *   2. Binlog phase: INSERT (C1, D1) are applied.
 */
suite("test_streaming_job_cdc_stream_postgres", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_job_cdc_stream_postgres_name"
    def currentDb = (sql "select database()")[0][0]
    def dorisTable = "test_streaming_job_cdc_stream_postgres_tbl"
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"
    def pgTable = "test_streaming_job_cdc_stream_postgres_src"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${dorisTable} force"""

    sql """
        CREATE TABLE IF NOT EXISTS ${currentDb}.${dorisTable} (
            `name` varchar(200) NULL,
            `age`  int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`name`)
        DISTRIBUTED BY HASH(`name`) BUCKETS AUTO
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port = context.config.otherConfigs.get("pg_14_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        // prepare source table with pre-existing snapshot data
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${pgTable}"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${pgTable} (
                      "name" varchar(200) PRIMARY KEY,
                      "age"  int2
                  )"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('A1', 1)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('B1', 2)"""
        }

        // create streaming job via cdc_stream TVF (offset=initial → snapshot then binlog)
        sql """
            CREATE JOB ${jobName}
            ON STREAMING DO INSERT INTO ${currentDb}.${dorisTable} (name, age)
            SELECT name, age FROM cdc_stream(
                "type"         = "postgres",
                "jdbc_url"     = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}",
                "driver_url"   = "${driver_url}",
                "driver_class" = "org.postgresql.Driver",
                "user"         = "${pgUser}",
                "password"     = "${pgPassword}",
                "database"     = "${pgDB}",
                "schema"             = "${pgSchema}",
                "table"              = "${pgTable}",
                "offset"             = "initial",
                "snapshot_split_size"             = "1"
            )
        """

        // wait for at least one snapshot task to succeed
        try {
            Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                def cnt = sql """select SucceedTaskCount from jobs("type"="insert") where Name='${jobName}' and ExecuteType='STREAMING'"""
                log.info("SucceedTaskCount: " + cnt)
                cnt.size() == 1 && (cnt.get(0).get(0) as int) >= 2
            })
        } catch (Exception ex) {
            log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            log.info("tasks: " + (sql """select * from tasks("type"="insert") where JobName='${jobName}'"""))
            throw ex
        }

        // verify snapshot data
        qt_snapshot_data """ SELECT * FROM ${currentDb}.${dorisTable} ORDER BY name """

        // insert incremental rows in PostgreSQL
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('C1', 3)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${pgTable} (name, age) VALUES ('D1', 4)"""
        }

        // wait for binlog tasks to pick up the new rows
        try {
            Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                def rows = sql """SELECT count(1) FROM ${currentDb}.${dorisTable} WHERE name IN ('C1', 'D1')"""
                log.info("incremental rows: " + rows)
                (rows.get(0).get(0) as int) == 2
            })
        } catch (Exception ex) {
            log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            log.info("tasks: " + (sql """select * from tasks("type"="insert") where JobName='${jobName}'"""))
            throw ex
        }

        qt_final_data """ SELECT * FROM ${currentDb}.${dorisTable} ORDER BY name """

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        sql """drop table if exists ${currentDb}.${dorisTable} force"""
    }
}
