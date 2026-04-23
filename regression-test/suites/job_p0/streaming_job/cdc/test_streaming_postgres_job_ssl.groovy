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

suite("test_streaming_postgres_job_ssl", "p0,external,pg,external_docker,external_docker_pg,nondatalake") {
    def jobName = "test_streaming_postgres_job_name_ssl"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "user_info_pg_normal1_ssl"
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

        // create test
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            // sql """CREATE SCHEMA IF NOT EXISTS ${pgSchema}"""
            def sslInfo = sql """SHOW ssl"""
            log.info("sslInfo: " + sslInfo)
            sql """DROP TABLE IF EXISTS ${pgDB}.${pgSchema}.${table1}"""
            sql """CREATE TABLE ${pgDB}.${pgSchema}.${table1} (
                  "name" varchar(200),
                  "age" int2,
                  PRIMARY KEY ("name")
                )"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (name, age) VALUES ('A1', 1);"""
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (name, age) VALUES ('B1', 2);"""
        }

        try {
            sql """DROP FILE "ca.pem" FROM ${currentDb} PROPERTIES ("catalog" = "streaming_job")"""
        } catch (Exception ignored) {
            // ignore
        }

        sql """CREATE FILE "ca.pem"
            IN ${currentDb}
            PROPERTIES
            (
                "url" = "https://qa-build.oss-cn-beijing.aliyuncs.com/jianxu/root.crt",
                "catalog" = "streaming_job"
            )
        """

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
                    "ssl_mode" = "verify-ca",
                    "ssl_rootcert" = "FILE:ca.pem"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """
        def showAllTables = sql """ show tables from ${currentDb}"""
        log.info("showAllTables: " + showAllTables)
        // check table created
        def showTables = sql """ show tables from ${currentDb} like '${table1}'; """
        assert showTables.size() == 1

        // check job running
        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(1, SECONDS).until(
                    {
                        def jobSuccendCount = sql """ select SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                        log.info("jobSuccendCount: " + jobSuccendCount)
                        // check job status and succeed task count larger than 2
                        jobSuccendCount.size() == 1 && '2' <= jobSuccendCount.get(0).get(0)
                    }
            )
        } catch (Exception ex){
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex;
        }

        // check snapshot data
        qt_select_snapshot_table1 """ SELECT * FROM ${table1} order by name asc """

        // mock incremental into
        connect("${pgUser}", "${pgPassword}", "jdbc:postgresql://${externalEnvIp}:${pg_port}/${pgDB}") {
            sql """INSERT INTO ${pgDB}.${pgSchema}.${table1} (name,age) VALUES ('Doris',18);"""
            def xminResult = sql """SELECT xmin, xmax , * FROM ${pgSchema}.${table1} WHERE name = 'Doris'; """
            log.info("xminResult: " + xminResult)
            sql """UPDATE ${pgDB}.${pgSchema}.${table1} SET age = 10 WHERE name = 'B1';"""
            sql """DELETE FROM ${pgDB}.${pgSchema}.${table1} WHERE name = 'A1';"""
        }

        sleep(60000); // wait for cdc incremental data

        // check incremental data
        qt_select_binlog_table1 """ SELECT * FROM ${table1} order by name asc """

        def jobInfo = sql """
        select loadStatistic, status from jobs("type"="insert") where Name='${jobName}'
        """
        log.info("jobInfo: " + jobInfo)
        def loadStat = parseJson(jobInfo.get(0).get(0))
        assert loadStat.scannedRows == 5
        assert loadStat.loadBytes == 246
        assert jobInfo.get(0).get(1) == "RUNNING"

        sql """
            DROP JOB IF EXISTS where jobname =  '${jobName}'
        """

        try {
            sql """DROP FILE "ca.pem" FROM ${currentDb} PROPERTIES ("catalog" = "streaming_job")"""
        } catch (Exception ignored) {
            // ignore
        }

        def jobCountRsp = sql """select count(1) from jobs("type"="insert")  where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
