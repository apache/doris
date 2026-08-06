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

suite("test_streaming_postgres_job_date_pk", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_date_pk_name"
    def currentDb = (sql "select database()")[0][0]
    def tableDate = "events_pg_date_pk"
    def tableComposite = "events_pg_date_id_pk"
    def pgDB = "postgres"
    def pgSchema = "cdc_test"
    def pgUser = "postgres"
    def pgPassword = "123456"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${tableDate} force"""
    sql """drop table if exists ${currentDb}.${tableComposite} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String pg_port = context.config.otherConfigs.get("pg_14_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${tableDate}"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${tableDate} (
                  "event_date" date PRIMARY KEY,
                  "payload" varchar(200)
                )"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableDate} (event_date, payload) VALUES ('2025-01-01', 'A1');"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableDate} (event_date, payload) VALUES ('2025-01-02', 'B1');"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableDate} (event_date, payload) VALUES ('2025-01-03', 'C1');"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableDate} (event_date, payload) VALUES ('2025-01-04', 'D1');"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableDate} (event_date, payload) VALUES ('2025-01-05', 'E1');"""

            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${tableComposite}"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${tableComposite} (
                  "event_date" date NOT NULL,
                  "id" int4 NOT NULL,
                  "payload" varchar(200),
                  PRIMARY KEY ("event_date", "id")
                )"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableComposite} (event_date, id, payload) VALUES ('2025-02-01', 1, 'A2');"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableComposite} (event_date, id, payload) VALUES ('2025-02-02', 2, 'B2');"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableComposite} (event_date, id, payload) VALUES ('2025-02-03', 3, 'C2');"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableComposite} (event_date, id, payload) VALUES ('2025-02-04', 4, 'D2');"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableComposite} (event_date, id, payload) VALUES ('2025-02-05', 5, 'E2');"""
        }

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
                    "include_tables" = "${tableDate},${tableComposite}",
                    "offset" = "initial",
                    "snapshot_split_size" = "1",
                    "snapshot_parallelism" = "2"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(1, SECONDS).until(
                    {
                        def jobSuccendCount = sql """ select SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                        log.info("jobSuccendCount: " + jobSuccendCount)
                        jobSuccendCount.size() == 1 && '5' <= jobSuccendCount.get(0).get(0)
                    }
            )
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex
        }

        qt_select_snapshot_date_pk """ SELECT * FROM ${tableDate} order by event_date asc """
        qt_select_snapshot_composite_pk """ SELECT * FROM ${tableComposite} order by event_date asc, id asc """

        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableDate} (event_date, payload) VALUES ('2025-01-06', 'F1');"""
            sql """UPDATE ${pgDB}.${pgSchema}.${tableDate} SET payload = 'B1_upd' WHERE event_date = '2025-01-02';"""
            sql """DELETE FROM ${pgDB}.${pgSchema}.${tableDate} WHERE event_date = '2025-01-01';"""

            sql """INSERT INTO ${pgDB}.${pgSchema}.${tableComposite} (event_date, id, payload) VALUES ('2025-02-06', 6, 'F2');"""
            sql """UPDATE ${pgDB}.${pgSchema}.${tableComposite} SET payload = 'B2_upd' WHERE event_date = '2025-02-02' AND id = 2;"""
            sql """DELETE FROM ${pgDB}.${pgSchema}.${tableComposite} WHERE event_date = '2025-02-01' AND id = 1;"""
        }

        sleep(60000)

        qt_select_binlog_date_pk """ SELECT * FROM ${tableDate} order by event_date asc """
        qt_select_binlog_composite_pk """ SELECT * FROM ${tableComposite} order by event_date asc, id asc """

        def jobInfo = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
        assert jobInfo.get(0).get(0) == "RUNNING"

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    }
}
