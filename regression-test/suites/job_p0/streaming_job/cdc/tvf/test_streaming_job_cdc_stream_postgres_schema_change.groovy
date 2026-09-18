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

suite("test_streaming_job_cdc_stream_postgres_schema_change", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_job_cdc_stream_postgres_schema_change"
    def currentDb = (sql "select database()")[0][0]
    def dorisTable = "test_streaming_job_cdc_stream_postgres_sc_tbl"
    def pgDb = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"
    def pgTable = "test_streaming_job_cdc_stream_postgres_sc_src"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """DROP TABLE IF EXISTS ${currentDb}.${dorisTable} FORCE"""

    sql """
        CREATE TABLE IF NOT EXISTS ${currentDb}.${dorisTable} (
            `name` varchar(200) NULL,
            `age` int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`name`)
        DISTRIBUTED BY HASH(`name`) BUCKETS AUTO
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pgPort = context.config.otherConfigs.get("pg_14_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3Endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driverUrl = "https://${bucket}.${s3Endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        def dumpJobState = {
            log.info("job: " + (sql """SELECT * FROM jobs("type"="insert") WHERE Name='${jobName}'"""))
            log.info("tasks: " + (sql """SELECT * FROM tasks("type"="insert") WHERE JobName='${jobName}'"""))
            log.info("data: " + (sql """SELECT name, age FROM ${currentDb}.${dorisTable} ORDER BY name"""))
        }

        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pgPort}/${pgDb}") {
            sql """CREATE SCHEMA IF NOT EXISTS ${pgSchema}"""
            sql """DROP TABLE IF EXISTS ${pgSchema}.${pgTable}"""
            sql """CREATE TABLE ${pgSchema}.${pgTable} (
                      "name" varchar(200) PRIMARY KEY,
                      "age" int
                  )"""
            sql """INSERT INTO ${pgSchema}.${pgTable} (name, age) VALUES ('A1', 1)"""
        }

        sql """
            CREATE JOB ${jobName}
            ON STREAMING DO INSERT INTO ${currentDb}.${dorisTable} (name, age)
            SELECT name, age FROM cdc_stream(
                "type" = "postgres",
                "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pgPort}/${pgDb}",
                "driver_url" = "${driverUrl}",
                "driver_class" = "org.postgresql.Driver",
                "user" = "${pgUser}",
                "password" = "${pgPassword}",
                "database" = "${pgDb}",
                "schema" = "${pgSchema}",
                "table" = "${pgTable}",
                "offset" = "initial",
                "snapshot_split_size" = "1"
            )
        """

        try {
            Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                def rows = sql """SELECT age FROM ${currentDb}.${dorisTable} WHERE name='A1'"""
                rows.size() == 1 && rows[0][0] == 1
            })
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pgPort}/${pgDb}") {
            sql """ALTER TABLE ${pgSchema}.${pgTable} ADD COLUMN city varchar(30) DEFAULT NULL"""
            sql """INSERT INTO ${pgSchema}.${pgTable} (name, age, city) VALUES ('B1', 2, 'hz')"""
        }

        try {
            Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                def rows = sql """SELECT age FROM ${currentDb}.${dorisTable} WHERE name='B1'"""
                rows.size() == 1 && rows[0][0] == 2
            })
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        assert (sql """SELECT Status FROM jobs("type"="insert") WHERE Name='${jobName}'""")[0][0] == "RUNNING"
        def createTable = (sql """SHOW CREATE TABLE ${currentDb}.${dorisTable}""")[0][1] as String
        assert !createTable.contains("`city`")
        order_qt_final_rows """ SELECT * FROM ${currentDb}.${dorisTable} ORDER BY name """

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    }
}
