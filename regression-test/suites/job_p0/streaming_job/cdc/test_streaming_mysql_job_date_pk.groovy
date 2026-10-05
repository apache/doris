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

suite("test_streaming_mysql_job_date_pk", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_date_pk_name"
    def currentDb = (sql "select database()")[0][0]
    def tableDate = "events_mysql_date_pk"
    def tableComposite = "events_mysql_date_id_pk"
    def mysqlDb = "test_cdc_db"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${tableDate} force"""
    sql """drop table if exists ${currentDb}.${tableComposite} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${tableDate}"""
            sql """CREATE TABLE ${mysqlDb}.${tableDate} (
                  `event_date` date NOT NULL,
                  `payload` varchar(200) DEFAULT NULL,
                  PRIMARY KEY (`event_date`)
                ) ENGINE=InnoDB"""
            sql """INSERT INTO ${mysqlDb}.${tableDate} (event_date, payload) VALUES ('2025-01-01', 'A1')"""
            sql """INSERT INTO ${mysqlDb}.${tableDate} (event_date, payload) VALUES ('2025-01-02', 'B1')"""
            sql """INSERT INTO ${mysqlDb}.${tableDate} (event_date, payload) VALUES ('2025-01-03', 'C1')"""
            sql """INSERT INTO ${mysqlDb}.${tableDate} (event_date, payload) VALUES ('2025-01-04', 'D1')"""
            sql """INSERT INTO ${mysqlDb}.${tableDate} (event_date, payload) VALUES ('2025-01-05', 'E1')"""

            sql """DROP TABLE IF EXISTS ${mysqlDb}.${tableComposite}"""
            sql """CREATE TABLE ${mysqlDb}.${tableComposite} (
                  `event_date` date NOT NULL,
                  `id` int NOT NULL,
                  `payload` varchar(200) DEFAULT NULL,
                  PRIMARY KEY (`event_date`, `id`)
                ) ENGINE=InnoDB"""
            sql """INSERT INTO ${mysqlDb}.${tableComposite} (event_date, id, payload) VALUES ('2025-02-01', 1, 'A2')"""
            sql """INSERT INTO ${mysqlDb}.${tableComposite} (event_date, id, payload) VALUES ('2025-02-02', 2, 'B2')"""
            sql """INSERT INTO ${mysqlDb}.${tableComposite} (event_date, id, payload) VALUES ('2025-02-03', 3, 'C2')"""
            sql """INSERT INTO ${mysqlDb}.${tableComposite} (event_date, id, payload) VALUES ('2025-02-04', 4, 'D2')"""
            sql """INSERT INTO ${mysqlDb}.${tableComposite} (event_date, id, payload) VALUES ('2025-02-05', 5, 'E2')"""
        }

        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM MYSQL (
                    "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/${mysqlDb}",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "com.mysql.cj.jdbc.Driver",
                    "user" = "root",
                    "password" = "123456",
                    "database" = "${mysqlDb}",
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

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """INSERT INTO ${mysqlDb}.${tableDate} (event_date, payload) VALUES ('2025-01-06', 'F1')"""
            sql """UPDATE ${mysqlDb}.${tableDate} SET payload = 'B1_upd' WHERE event_date = '2025-01-02'"""
            sql """DELETE FROM ${mysqlDb}.${tableDate} WHERE event_date = '2025-01-01'"""

            sql """INSERT INTO ${mysqlDb}.${tableComposite} (event_date, id, payload) VALUES ('2025-02-06', 6, 'F2')"""
            sql """UPDATE ${mysqlDb}.${tableComposite} SET payload = 'B2_upd' WHERE event_date = '2025-02-02' AND id = 2"""
            sql """DELETE FROM ${mysqlDb}.${tableComposite} WHERE event_date = '2025-02-01' AND id = 1"""
        }

        sleep(60000)

        qt_select_binlog_date_pk """ SELECT * FROM ${tableDate} order by event_date asc """
        qt_select_binlog_composite_pk """ SELECT * FROM ${tableComposite} order by event_date asc, id asc """

        def jobInfo = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
        assert jobInfo.get(0).get(0) == "RUNNING"

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    }
}
