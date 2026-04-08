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

suite("test_streaming_mysql_job_all_type", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_job_all_type_name"
    def currentDb = (sql "select database()")[0][0]
    def table1 = "streaming_all_types_nullable_with_pk"
    def mysqlDb = "test_cdc_db"

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
            sql """
            create table ${mysqlDb}.${table1} (
              `tinyint_u` tinyint unsigned primary key,
              `smallint_u` smallint unsigned,
              `mediumint_u` mediumint unsigned,
              `int_u` int unsigned,
              `bigint_u` bigint unsigned,
              `decimal1_u` decimal unsigned,
              `decimal2_u` decimal(9, 2) unsigned,
              `decimal3_u` decimal(18, 5) unsigned,
              `decimal4_u` decimal(38, 10) unsigned,
              `decimal5_u` decimal(65, 30) unsigned,
              `double_u` double unsigned,
              `float_u` float unsigned,
              `boolean` boolean,
              `tinyint` tinyint,
              `smallint` smallint,
              `mediumint` mediumint,
              `int` int,
              `bigint` bigint,
              `double` double,
              `float` float,
              `decimal1` decimal,
              `decimal2` decimal(9, 2),
              `decimal3` decimal(18, 5) ,
              `decimal4` decimal(38, 10),
              `decimal5` decimal(65, 30),
              `year` year,
              `time1` time,
              `time2` time(3),
              `time3` time(6),
              `date` date,
              `datetime` datetime,
              `timestamp1` timestamp null,
              `timestamp2` timestamp(3) null,
              `timestamp3` timestamp(6) null,
              `char` char(5),
              `varchar` varchar(10),
              `text` text,
              `blob` blob,
              `json` json,
              `set` set('Option1', 'Option2', 'Option3'),
              `bit` bit(6),
              `binary` binary(12),
              `varbinary` varbinary(12),
              `enum` enum('Value1', 'Value2', 'Value3')
            ) engine=innodb charset=utf8;
            """
            // mock snapshot data
            sql """
            insert into ${mysqlDb}.${table1} values (1,120,50000,1000000,9000000000,1000,12345.67,987654.12345,123456789.1234567890,99999999.123456789012345678901234567890,123.45,12.34,true,-5,-300,-20000,-500000,-8000000000,-123.45,-12.34,-1000,-1234.56,-98765.43210,-123456789.1234567890,-99999999.123456789012345678901234567890,2023,'08:30:00','08:30:00.123','08:30:00.123456','2023-06-15','2023-06-15 08:30:00','2023-06-15 08:30:00','2023-06-15 08:30:00.123','2023-06-15 08:30:00.123456','abc','user_001','normal text content','simple blob data','{"id":1,"name":"userA"}','Option1',b'110011','binary_data1','varbin_01','Value1');
            """
        }

        sql """CREATE JOB ${jobName}
                ON STREAMING
                FROM MYSQL (
                    "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}?serverTimezone=UTC",
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
                        def jobSuccendCount = sql """ select SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                        log.info("jobSuccendCount: " + jobSuccendCount)
                        // check job status and succeed task count larger than 2
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

        qt_desc_all_types_null """desc ${currentDb}.${table1};"""
        qt_select_all_types_null """select * from ${currentDb}.${table1} order by 1;"""

       // mock mysql incremental into
       connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
           sql """insert into ${mysqlDb}.${table1} values (2,100,100000,1000000000,100000000000,12345,12345.67,123456789.12345,12345678901234567890.1234567890,123456789012345678901234567890.123456789012345678901234567890,12345.6789,123.456,true,-10,-1000,-100000,-100000000,-1000000000000,-12345.6789,-123.456,-12345,-12345.67,-123456789.12345,-12345678901234567890.1234567890,-123456789012345678901234567890.123456789012345678901234567890,2024,'12:34:56','12:34:56.789','12:34:56.789123','2024-01-01','2024-01-01 12:34:56','2024-01-01 12:34:56','2024-01-01 12:34:56.789','2024-01-01 12:34:56.789123','hello','hello123','this is a text field','this is a blob','{"id":10,"name":"mock"}','Option2',b'101010','bin_data_123','varbin_data','Value2');"""
       }

       sleep(60000); // wait for cdc incremental data

       // check incremental data
       qt_select_all_types_null2 """select * from ${currentDb}.${table1} order by 1;"""


       sql """
           DROP JOB IF EXISTS where jobname =  '${jobName}'
       """

       def jobCountRsp = sql """select count(1) from jobs("type"="insert")  where Name ='${jobName}'"""
       assert jobCountRsp.get(0).get(0) == 0
    }
}
