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

suite("test_streaming_mysql_job_priv", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def tableName = "test_streaming_mysql_job_priv_tbl"
    def jobName = "test_streaming_mysql_job_priv_name"

    def currentDb = (sql "select database()")[0][0]
    def mysqlDb = "test_cdc_db"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${tableName} force"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

        // create user
        def user = "test_streaming_mysql_job_priv_user"
        def pwd = '123456'
        def dbName = context.config.getDbNameByFile(context.file)
        sql """DROP USER IF EXISTS '${user}'"""
        sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
        def tokens = context.config.jdbcUrl.split('/')
        def url = tokens[0] + "//" + tokens[2] + "/" + dbName + "?"
        sql """grant select_priv on ${dbName}.* to ${user}"""

        if (isCloudMode()){
            // Cloud requires USAGE_PRIV to show clusters.
            def clusters = sql_return_maparray """show clusters"""
            log.info("show cluster res: " + clusters)
            assertNotNull(clusters)

            for (item in clusters) {
                log.info("cluster item: " + item.is_current + ", " + item.cluster)
                if (item.is_current.equalsIgnoreCase("TRUE")) {
                    sql """GRANT USAGE_PRIV ON CLUSTER `${item.cluster}` TO ${user}""";
                    break
                }
            }
        }

        // create job with select priv user
        connect(user, "${pwd}", url) {
            expectExceptionLike({
                sql """CREATE JOB ${jobName}
                    ON STREAMING
                    FROM MYSQL (
                        "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}",
                        "driver_url" = "${driver_url}",
                        "driver_class" = "com.mysql.cj.jdbc.Driver",
                        "user" = "root",
                        "password" = "123456",
                        "database" = "${mysqlDb}",
                        "include_tables" = "${tableName}", 
                        "offset" = "initial"
                    )
                    TO DATABASE ${currentDb} (
                      "table.create.properties.replication_num" = "1"
                    )
                    """
            }, "you need (at least one of) the (LOAD) privilege")
        }

        def jobCount = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
        assert jobCount.size() == 0

        // create mysql test
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${tableName}"""
            sql """CREATE TABLE ${mysqlDb}.${tableName} (
                  `name` varchar(200) NOT NULL,
                  `age` int DEFAULT NULL,
                  PRIMARY KEY (`name`)
                ) ENGINE=InnoDB"""
            sql """INSERT INTO ${mysqlDb}.${tableName} (name, age) VALUES ('A1', 1);"""
            sql """INSERT INTO ${mysqlDb}.${tableName} (name, age) VALUES ('B1', 2);"""
        }

        // create streaming job by load_priv and create_priv
        sql """grant load_priv,create_priv on ${dbName}.* to ${user}"""
        connect(user, "${pwd}", url) {
            sql """CREATE JOB ${jobName}
                    ON STREAMING
                    FROM MYSQL (
                        "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}?allowPublicKeyRetrieval=true&useSSL=false",
                        "driver_url" = "${driver_url}",
                        "driver_class" = "com.mysql.cj.jdbc.Driver",
                        "user" = "root",
                        "password" = "123456",
                        "database" = "${mysqlDb}",
                        "include_tables" = "${tableName}", 
                        "offset" = "initial"
                    )
                    TO DATABASE ${currentDb} (
                      "table.create.properties.replication_num" = "1"
                    )
                """
        }

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
        } catch (Exception ex) {
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex;
        }

        def jobResult = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
        log.info("show jobResult: " + jobResult)

        // create a new mysql user only has select priv
        def newMysqlUser = "mysql_job_priv"
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """DROP USER IF EXISTS '${newMysqlUser}'@'%'"""
            sql """CREATE USER '${newMysqlUser}'@'%' IDENTIFIED WITH mysql_native_password BY 'test123'"""
            sql """GRANT ALL PRIVILEGES ON ${mysqlDb}.* TO '${newMysqlUser}'@'%'"""
            sql """FLUSH PRIVILEGES"""
        }

        sql "PAUSE JOB where jobname =  '${jobName}'";

        sql """ALTER JOB ${jobName}
                FROM MYSQL (
                    "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}?allowPublicKeyRetrieval=true&useSSL=false",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "com.mysql.cj.jdbc.Driver",
                    "user" = "${newMysqlUser}",
                    "password" = "test123",
                    "database" = "${mysqlDb}",
                    "include_tables" = "${tableName}", 
                    "offset" = "latest"
                )
                TO DATABASE ${currentDb}"""

        sql "RESUME JOB where jobname =  '${jobName}'";

        // mock binlog data to ensure successful modification of user and password.
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """INSERT INTO ${mysqlDb}.${tableName} (name,age) VALUES ('DorisTestPriv',28);"""
        }

       try {
           Awaitility.await().atMost(300, SECONDS)
                   .pollInterval(1, SECONDS).until(
                   {
                       def jobStatus = sql """ select status, ErrorMsg from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                       log.info("jobStatus: " + jobStatus)
                       // check job status
                       jobStatus.size() == 1 && 'PAUSED' == jobStatus.get(0).get(0) && jobStatus.get(0).get(1).contains("Failed to fetch meta")
                   }
           )
       } catch (Exception ex){
           def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
           def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
           log.info("show job: " + showjob)
           log.info("show task: " + showtask)
           throw ex;
       }

        // grant binlog priv to mysqluser
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '${newMysqlUser}'@'%'"""
            sql """FLUSH PRIVILEGES"""
        }

        def jobSucceedTaskCnt = sql """ select SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
        log.info("jobSucceedTaskCnt: " + jobSucceedTaskCnt)

        try {
            Awaitility.await().atMost(300, SECONDS)
                    .pollInterval(1, SECONDS).until(
                    {
                        def jobStatus = sql """ select status, SucceedTaskCount from jobs("type"="insert") where Name = '${jobName}' and ExecuteType='STREAMING' """
                        log.info("jobStatus: " + jobStatus)
                        // check job status running and increase a success task
                        jobStatus.size() == 1 && 'RUNNING' == jobStatus.get(0).get(0) && jobStatus.get(0).get(1) > jobSucceedTaskCnt.get(0).get(0)
                    }
            )
        } catch (Exception ex){
            def showjob = sql """select * from jobs("type"="insert") where Name='${jobName}'"""
            def showtask = sql """select * from tasks("type"="insert") where JobName='${jobName}'"""
            log.info("show job: " + showjob)
            log.info("show task: " + showtask)
            throw ex;
        }

        // check incremental data
        qt_select """ SELECT * FROM ${tableName} order by name asc """

        sql """DROP USER IF EXISTS '${user}'"""
        sql """
        DROP JOB IF EXISTS where jobname =  '${jobName}'
        """

        def jobCountRsp = sql """select count(1) from jobs("type"="insert")  where Name ='${jobName}'"""
        assert jobCountRsp.get(0).get(0) == 0
    }
}
