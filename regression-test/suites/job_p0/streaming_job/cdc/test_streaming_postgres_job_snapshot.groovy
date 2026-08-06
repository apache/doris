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
 * Test snapshot-only mode (offset=snapshot):
 *   1. Job syncs existing data via full snapshot.
 *   2. Job transitions to FINISHED after snapshot completes (no binlog phase).
 *   3. Data inserted after job finishes is NOT synced to Doris.
 */
suite("test_streaming_postgres_job_snapshot", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_snapshot_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "user_info_pg_snapshot1"
    def table2 = "user_info_pg_snapshot2"
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""
    sql """drop table if exists ${currentDb}.${table2} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port = context.config.otherConfigs.get("pg_14_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        // prepare source tables and pre-existing data in postgres
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table2}"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${table1} (
                  "name" varchar(200),
                  "age" int2,
                  PRIMARY KEY ("name")
                )"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (name, age) VALUES ('A1', 1)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (name, age) VALUES ('B1', 2)"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${table2} (
                  "name" varchar(200),
                  "age" int2,
                  PRIMARY KEY ("name")
                )"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table2} (name, age) VALUES ('A2', 1)"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table2} (name, age) VALUES ('B2', 2)"""
        }

        // create streaming job with offset=snapshot (snapshot-only mode)
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
                    "include_tables" = "${table1},${table2}",
                    "offset" = "snapshot"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        // wait for job to transition to FINISHED
        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(2, SECONDS).until(
                    {
                        def jobStatus = sql """select Status from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING'"""
                        log.info("jobStatus: " + jobStatus)
                        jobStatus.size() == 1 && jobStatus.get(0).get(0) == 'FINISHED'
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex
        }

        // verify snapshot data is correctly synced
        qt_select_snapshot_table1 """ SELECT * FROM ${table1} order by name asc """
        qt_select_snapshot_table2 """ SELECT * FROM ${table2} order by name asc """

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    }
}
