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

suite("test_streaming_mysql_job_lag",
      "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {

    def jobName = "test_streaming_mysql_job_lag"
    def currentDb = (sql "select database()")[0][0]
    def mysqlDb = "test_cdc_db"
    def mysqlTable = "user_info_lag"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${mysqlTable}"""
            sql """CREATE TABLE ${mysqlDb}.${mysqlTable} (
                      `name` varchar(200) NOT NULL,
                      `age` int DEFAULT NULL,
                      PRIMARY KEY (`name`)
                   ) ENGINE=InnoDB"""
            sql """INSERT INTO ${mysqlDb}.${mysqlTable} (name, age) VALUES ('Alice', 10)"""
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
                    "include_tables" = "${mysqlTable}",
                    "offset" = "latest"
                )
                TO DATABASE ${currentDb} (
                    "table.create.properties.replication_num" = "1"
                )
            """

        try {
            // insert incremental data to trigger binlog consumption
            connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
                sql """INSERT INTO ${mysqlDb}.${mysqlTable} (name, age) VALUES ('Bob', 20)"""
            }

            // wait for binlog data consumed and lag is available
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(1, SECONDS).until({
                        def jobInfo = sql """ select SucceedTaskCount, Lag from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                        log.info("jobInfo: " + jobInfo)
                        if (jobInfo.size() != 1 || Integer.parseInt(jobInfo[0][0] as String) < 1) {
                            return false
                        }
                        def lagValue = jobInfo[0][1] as String
                        log.info("lag value: " + lagValue)
                        return lagValue != null && lagValue != "" && lagValue.isNumber()
                    })
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("lag show job: " + showjob)
            log.info("lag show task: " + showtask)
            throw ex
        }

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        def jobCountRsp = sql """select count(1) from jobs("type"="insert") where Name = '${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
