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

suite("test_streaming_postgres_job_priv", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def tableName = "test_streaming_postgres_job_priv_tbl"
    def jobName = "test_streaming_postgres_job_priv_name"
    def currentDb = (sql "select database()")[0][0]
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${tableName} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port = context.config.otherConfigs.get("pg_14_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        // create pg test table
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            // sql """CREATE SCHEMA IF NOT EXISTS ${pgSchema}"""
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${tableName}"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${tableName} (
                  "name" varchar(200),
                  "age" int2,
                  PRIMARY KEY ("name")
                )"""
            // mock snapshot data
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableName} (name, age) VALUES ('A1', 1);"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableName} (name, age) VALUES ('B1', 2);"""
        }

        // create a new pg user only has select priv
        def newPgUser = "test_job_priv_pg"
        def newPgPassword = "test123"
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            try {
                sql """DROP OWNED BY ${newPgUser}"""
            }catch (Exception e) {
                log.info("Drop owned failed, maybe user not exist yet.")
            }
            sql """DROP ROLE IF EXISTS ${newPgUser}"""
            sql """CREATE ROLE ${newPgUser} WITH LOGIN PASSWORD '${newPgPassword}'"""
            sql """GRANT USAGE ON SCHEMA ${pgSchema} TO ${newPgUser}"""
            sql """GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA ${pgSchema} TO ${newPgUser}"""
        }

        test {
            // create job by new user
            sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM POSTGRES (
                    "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "org.postgresql.Driver",
                    "user" = "${newPgUser}",
                    "password" = "${newPgPassword}",
                    "database" = "${pgDB}",
                    "schema" = "${pgSchema}",
                    "include_tables" = "${tableName}", 
                    "offset" = "latest"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """
            exception "Failed to init source reader"
        }

        // grant replication to user
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
             sql """ALTER ROLE ${newPgUser} WITH REPLICATION"""
        }


        // create job by new user
        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM POSTGRES (
                    "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "org.postgresql.Driver",
                    "user" = "${newPgUser}",
                    "password" = "${newPgPassword}",
                    "database" = "${pgDB}",
                    "schema" = "${pgSchema}",
                    "include_tables" = "${tableName}", 
                    "offset" = "latest"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        Awaitility.await().atMost(300, SECONDS)
                .pollInterval(3, SECONDS).until(
                {
                    def jobEndOffset = sql """select EndOffset from jobs("type"="insert") where Name='${jobName}'"""
                    log.info("jobEndOffset: " + jobEndOffset)
                    jobEndOffset.get(0).get(0).contains("lsn")
                }
        )

        Awaitility.await().atMost(300, SECONDS)
                .pollInterval(3, SECONDS).until(
                {
                    def jobSucceedTaskCount = sql """select SucceedTaskCount from jobs("type"="insert") where Name='${jobName}'"""
                    log.info("jobSucceedTaskCount: " + jobSucceedTaskCount)
                    jobSucceedTaskCount.size() == 1 && jobSucceedTaskCount.get(0).get(0) >= '1'
                }
        )

        // mock incremental into
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableName} (name,age) VALUES ('Doris',18);"""
            def xminResult = sql """SELECT xmin, xmax , * FROM ${pgDB}.${pgSchema}.${tableName} WHERE name = 'Doris';"""
            log.info("xminResult: " + xminResult)
        }

        Awaitility.await().atMost(300, SECONDS)
                .pollInterval(3, SECONDS).until(
                {
                    def jobSucceedTaskCount = sql """select SucceedTaskCount from jobs("type"="insert") where Name='${jobName}'"""
                    log.info("jobSucceedTaskCount: " + jobSucceedTaskCount)
                    jobSucceedTaskCount.size() == 1 && jobSucceedTaskCount.get(0).get(0) >= '2'
                }
        )

        // check incremental data
        qt_select """ SELECT * FROM ${tableName} order by name asc """

        sql """
        DROP JOB IF EXISTS where jobname =  '${jobName}'
        """

        def jobCountRsp = sql """select count(1) from jobs("type"="insert")  where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
