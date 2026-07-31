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

suite("test_streaming_mysql_job_ssl", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_name_ssl"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "user_info_mysql_normal1_ssl"
    def mysqlDb = "test_cdc_db_ssl"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""


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
                  `age` int DEFAULT NULL,
                  PRIMARY KEY (`name`)
                ) ENGINE=InnoDB"""
            sql """INSERT INTO ${mysqlDb}.${table1} (name, age) VALUES ('A1', 1);"""
            sql """INSERT INTO ${mysqlDb}.${table1} (name, age) VALUES ('B1', 2);"""
        }

        try {
            sql """DROP FILE "mysql_ca.pem" FROM ${currentDb} PROPERTIES ("catalog" = "streaming_job")"""
        } catch (Exception ignored) {
            // ignore
        }

        sql """CREATE FILE "mysql_ca.pem"
            IN ${currentDb}
            PROPERTIES
            (
                "url" = "https://qa-build.oss-cn-beijing.aliyuncs.com/jianxu/root.crt",
                "catalog" = "streaming_job"
            )
        """

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
                    "offset" = "initial",
                    "ssl_mode" = "verify-ca",
                    "ssl_rootcert" = "FILE:mysql_ca.pem"
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
                        jobSuccendCount.size() == 1 && jobSuccendCount.get(0).get(0).toString().toLong() >= 2L
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
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """INSERT INTO ${mysqlDb}.${table1} (name,age) VALUES ('Doris',18);"""
            sql """UPDATE ${mysqlDb}.${table1} SET age = 10 WHERE name = 'B1';"""
            sql """DELETE FROM ${mysqlDb}.${table1} WHERE name = 'A1';"""
        }

        // wait for cdc incremental data
        try {
            Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
                def names = (sql """ SELECT name FROM ${table1} ORDER BY name ASC """).collect { it[0] }
                names.contains('Doris') && !names.contains('A1')
            })
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex
        }

        // check incremental data
        qt_select_binlog_table1 """ SELECT * FROM ${table1} order by name asc """

        def jobInfo = sql """
        select status from jobs("type"="insert") where Name='${jobName}'
        """
        log.info("jobInfo: " + jobInfo)
        assert jobInfo.get(0).get(0) == "RUNNING"

        sql """
            DROP JOB IF EXISTS where jobname =  '${jobName}'
        """

        try {
            sql """DROP FILE "mysql_ca.pem" FROM ${currentDb} PROPERTIES ("catalog" = "streaming_job")"""
        } catch (Exception ignored) {
            // ignore
        }

        def jobCountRsp = sql """select count(1) from jobs("type"="insert")  where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
