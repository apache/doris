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

suite("test_streaming_mysql_job", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "user_info_normal1"
    def table2 = "user_info_normal2"
    def table3 = "user_info_normal3_empty"
    def mysqlDb = "test_cdc_db"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${table1} force"""
    sql """drop table if exists ${currentDb}.${table2} force"""
    sql """drop table if exists ${currentDb}.${table3} force"""

    // Pre-create table2
    sql """
        CREATE TABLE IF NOT EXISTS ${currentDb}.${table2} (
            `name` varchar(200) NULL,
            `age` int NULL
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
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table2}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${table3}"""
            sql """CREATE TABLE ${mysqlDb}.${table1} (
                  `name` varchar(200) NOT NULL,
                  `age` int DEFAULT NULL,
                  PRIMARY KEY (`name`)
                ) ENGINE=InnoDB"""
            sql """INSERT INTO ${mysqlDb}.${table1} (name, age) VALUES ('A1', 1);"""
            sql """INSERT INTO ${mysqlDb}.${table1} (name, age) VALUES ('B1', 2);"""
            sql """CREATE TABLE ${mysqlDb}.${table2} (
                  `name` varchar(200) NOT NULL,
                  `age` int DEFAULT NULL,
                  PRIMARY KEY (`name`)
                ) ENGINE=InnoDB"""
            // mock snapshot data
            sql """INSERT INTO ${mysqlDb}.${table2} (name, age) VALUES ('A2', 1);"""
            sql """INSERT INTO ${mysqlDb}.${table2} (name, age) VALUES ('B2', 2);"""
            sql """CREATE TABLE ${mysqlDb}.${table3} (
                  `name` varchar(200) NOT NULL,
                  `age` int DEFAULT NULL,
                  PRIMARY KEY (`name`)
                ) ENGINE=InnoDB"""
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
                    "include_tables" = "${table3},${table1},${table2}", 
                    "offset" = "initial"
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
        def showTables2 = sql """ show tables from ${currentDb} like '${table2}'; """
        assert showTables2.size() == 1
        def showTables3 = sql """ show tables from ${currentDb} like '${table3}'; """
        assert showTables3.size() == 1

        // check table schema correct
        def showTbl1 = sql """show create table ${currentDb}.${table1}"""
        def createTalInfo = showTbl1[0][1];
        assert createTalInfo.contains("`name` varchar(600)");
        assert createTalInfo.contains("`age` int");
        assert createTalInfo.contains("UNIQUE KEY(`name`)");
        assert createTalInfo.contains("DISTRIBUTED BY HASH(`name`) BUCKETS AUTO");

        // check job running
        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(1, SECONDS).until(
                    {
                        def jobSuccendCount = sql """ select SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                        log.info("jobSuccendCount: " + jobSuccendCount)
                        // check job status and succeed task count larger than 2
                        jobSuccendCount.size() == 1 && '3' <= jobSuccendCount.get(0).get(0)
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
        qt_select_snapshot_table2 """ SELECT * FROM ${table2} order by name asc """
        qt_select_snapshot_table3 """ SELECT * FROM ${table3} order by name asc """

        // mock mysql incremental into
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """INSERT INTO ${mysqlDb}.${table1} (name,age) VALUES ('Doris',18);"""
            sql """UPDATE ${mysqlDb}.${table1} SET age = 10 WHERE name = 'B1';"""
            sql """DELETE FROM ${mysqlDb}.${table1} WHERE name = 'A1';"""
        }

        sleep(60000); // wait for cdc incremental data

        // check incremental data
        qt_select_binlog_table1 """ SELECT * FROM ${table1} order by name asc """

        def jobInfo = sql """
        select loadStatistic, status from jobs("type"="insert") where Name='${jobName}'
        """
        log.info("jobInfo: " + jobInfo)
        def loadStat = parseJson(jobInfo.get(0).get(0))
        assert loadStat.scannedRows == 7
        assert loadStat.loadBytes == 338
        assert jobInfo.get(0).get(1) == "RUNNING"

        // mock mysql incremental into again
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """INSERT INTO ${mysqlDb}.${table1} (name,age) VALUES ('Apache',40);"""
        }

        sleep(60000); // wait for cdc incremental data

        // check incremental data
        qt_select_next_binlog_table1 """ SELECT * FROM ${table1} order by name asc """

        sql """
            DROP JOB IF EXISTS where jobname =  '${jobName}'
        """

        def jobCountRsp = sql """select count(1) from jobs("type"="insert")  where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
