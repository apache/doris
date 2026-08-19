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

suite("test_streaming_postgres_job_precreated_target",
        "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_precreated_target"
    def currentDb = (sql "select database()")[0][0]
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.streaming_precreated_enum_target force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pgPort = context.config.otherConfigs.get("pg_14_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3Endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driverUrl = "https://${bucket}.${s3Endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pgPort}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.streaming_precreated_enum_source"""
            sql """DROP TYPE IF EXISTS ${pgSchema}.streaming_precreated_target_enum"""
            sql """CREATE TYPE ${pgSchema}.streaming_precreated_target_enum AS ENUM ('ready', 'done')"""
            sql """
                CREATE TABLE ${pgDB}.${pgSchema}.streaming_precreated_enum_source (
                    id INTEGER PRIMARY KEY,
                    searchable ${pgSchema}.streaming_precreated_target_enum
                )
            """
            sql """
                INSERT INTO ${pgDB}.${pgSchema}.streaming_precreated_enum_source
                VALUES (1, 'ready')
            """
        }

        sql """
            CREATE TABLE ${currentDb}.streaming_precreated_enum_target (
                id INT NOT NULL,
                searchable STRING NULL
            ) ENGINE=OLAP
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS AUTO
            PROPERTIES ("replication_num" = "1")
        """

        sql """
            CREATE JOB ${jobName}
            ON STREAMING
            FROM POSTGRES (
                "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pgPort}/${pgDB}",
                "driver_url" = "${driverUrl}",
                "driver_class" = "org.postgresql.Driver",
                "user" = "${pgUser}",
                "password" = "${pgPassword}",
                "database" = "${pgDB}",
                "schema" = "${pgSchema}",
                "include_tables" = "streaming_precreated_enum_source",
                "offset" = "initial",
                "table.streaming_precreated_enum_source.target_table" = "streaming_precreated_enum_target"
            )
            TO DATABASE ${currentDb} (
                "table.create.properties.replication_num" = "1"
            )
        """

        try {
            Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until {
                def rows = sql """
                    SELECT id, searchable
                    FROM ${currentDb}.streaming_precreated_enum_target
                    WHERE id = 1
                """
                rows.size() == 1 && rows[0][0].toString() == "1" && rows[0][1] == "ready"
            }
        } catch (Exception ex) {
            log.info("show job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            log.info("show task: " + (sql """select * from tasks("type"="insert") where JobName='${jobName}'"""))
            throw ex
        }

        order_qt_select_precreated_target """
            SELECT id, searchable FROM ${currentDb}.streaming_precreated_enum_target
        """

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    }
}
