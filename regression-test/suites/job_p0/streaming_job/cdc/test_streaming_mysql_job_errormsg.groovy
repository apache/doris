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

suite("test_streaming_mysql_job_errormsg", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_errormsg"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "user_info_errormsg"
    def mysqlDb = "test_cdc_db"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""

    // Pre-create table2
    sql """
        CREATE TABLE IF NOT EXISTS ${currentDb}.${table1} (
            `name` varchar(2) NOT NULL,
            `age` int NOT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`name`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`name`) BUCKETS AUTO
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

        // create test
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table1}"""
            sql """CREATE TABLE ${mysqlDb}.${table1} (
                  `name` varchar(200) NOT NULL,
                  `age` varchar(8) NOT NULL,
                  PRIMARY KEY (`name`)
                ) ENGINE=InnoDB"""
            sql """INSERT INTO ${mysqlDb}.${table1} (name, age) VALUES ('ABCDEFG1', 'abc');"""
            sql """INSERT INTO ${mysqlDb}.${table1} (name, age) VALUES ('ABCDEFG2', '123');"""
        }

        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM MYSQL (
                    "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "com.mysql.cj.jdbc.Driver",
                    "user" = "root",
                    "password" = "123456",
                    "database" = "${mysqlDb}",
                    "include_tables" = "${table1}", 
                    "offset" = "initial"
                )
                TO DATABASE ${currentDb} (
                  "table.create.properties.replication_num" = "1"
                )
            """

        // check job running
        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(1, SECONDS).until(
                    {
                        def jobFailCount = sql """ select FailedTaskCount, CanceledTaskCount, SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                        log.info("jobFailCount: " + jobFailCount)
                        // check job status and faile task count larger than 1
                        jobFailCount.size() == 1 && ('1' <= jobFailCount.get(0).get(0) || '1' <= jobFailCount.get(0).get(1))
                    }
            )
        } catch (Exception ex){
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex;
        }

        def jobFailMsg = sql """select errorMsg  from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING'"""
        log.info("jobFailMsg: " + jobFailMsg)
        // stream load error: [DATA_QUALITY_ERROR]too many filtered rows
        assert jobFailMsg.get(0).get(0).contains("stream load error")


        // add max_filter_ratio to 1
        sql """ALTER JOB ${jobName}
        FROM MYSQL
        TO DATABASE ${currentDb} (
            "load.max_filter_ratio" = "1"
        )"""

        sql """RESUME JOB where jobname = '${jobName}'"""

        // check job running
        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(1, SECONDS).until(
                    {
                        def jobSuccendCount = sql """ select SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                        log.info("jobSuccendCount: " + jobSuccendCount)
                        // check job status and succeed task count larger than 1
                        jobSuccendCount.size() == 1 && '1' <= jobSuccendCount.get(0).get(0)
                    }
            )
        } catch (Exception ex){
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex;
        }

        def jobInfo = sql """
        select loadStatistic, status from jobs("type"="insert") where Name='${jobName}'
        """
        log.info("jobInfo: " + jobInfo)
        def loadStat = parseJson(jobInfo.get(0).get(0));
        assert loadStat.scannedRows == 2
        assert loadStat.loadBytes == 115
        assert loadStat.filteredRows == 1
        assert jobInfo.get(0).get(1) == "RUNNING"

        qt_select_snapshot_table1 """ SELECT * FROM ${table1} order by name asc """

        sql """
            DROP JOB IF EXISTS where jobname =  '${jobName}'
        """

        def jobCountRsp = sql """select count(1) from jobs("type"="insert")  where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
