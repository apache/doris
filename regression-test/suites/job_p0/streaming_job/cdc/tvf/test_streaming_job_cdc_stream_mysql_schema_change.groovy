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

suite("test_streaming_job_cdc_stream_mysql_schema_change", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_job_cdc_stream_mysql_schema_change"
    def currentDb = (sql "select database()")[0][0]
    def dorisTable = "test_streaming_job_cdc_stream_mysql_sc_tbl"
    def mysqlDb = "test_cdc_db"
    def mysqlTable = "test_streaming_job_cdc_stream_mysql_sc_src"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """DROP TABLE IF EXISTS ${currentDb}.${dorisTable} FORCE"""

    sql """
        CREATE TABLE IF NOT EXISTS ${currentDb}.${dorisTable} (
            `name` varchar(200) NULL,
            `age` int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`name`)
        DISTRIBUTED BY HASH(`name`) BUCKETS AUTO
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysqlPort = context.config.otherConfigs.get("mysql_57_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3Endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driverUrl = "https://${bucket}.${s3Endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

        def dumpJobState = {
            log.info("job: " + (sql """SELECT * FROM jobs("type"="insert") WHERE Name='${jobName}'"""))
            log.info("tasks: " + (sql """SELECT * FROM tasks("type"="insert") WHERE JobName='${jobName}'"""))
            log.info("data: " + (sql """SELECT name, age FROM ${currentDb}.${dorisTable} ORDER BY name"""))
        }

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysqlPort}") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${mysqlTable}"""
            sql """CREATE TABLE ${mysqlDb}.${mysqlTable} (
                      `name` varchar(200) NOT NULL,
                      `age` int DEFAULT NULL,
                      PRIMARY KEY (`name`)
                    ) ENGINE=InnoDB"""
            sql """INSERT INTO ${mysqlDb}.${mysqlTable} (name, age) VALUES ('A1', 1)"""
        }

        sql """
            CREATE JOB ${jobName}
            ON STREAMING DO INSERT INTO ${currentDb}.${dorisTable} (name, age)
            SELECT name, age FROM cdc_stream(
                "type" = "mysql",
                "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysqlPort}",
                "driver_url" = "${driverUrl}",
                "driver_class" = "com.mysql.cj.jdbc.Driver",
                "user" = "root",
                "password" = "123456",
                "database" = "${mysqlDb}",
                "table" = "${mysqlTable}",
                "offset" = "initial",
                "snapshot_split_key" = "name"
            )
        """

        try {
            Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                def rows = sql """SELECT age FROM ${currentDb}.${dorisTable} WHERE name='A1'"""
                rows.size() == 1 && rows[0][0] == 1
            })
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysqlPort}") {
            sql """ALTER TABLE ${mysqlDb}.${mysqlTable} ADD COLUMN city varchar(30) DEFAULT NULL"""
            sql """INSERT INTO ${mysqlDb}.${mysqlTable} (name, age, city) VALUES ('B1', 2, 'hz')"""
        }

        try {
            Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                def rows = sql """SELECT age FROM ${currentDb}.${dorisTable} WHERE name='B1'"""
                rows.size() == 1 && rows[0][0] == 2
            })
        } catch (Exception ex) {
            dumpJobState()
            throw ex
        }

        assert (sql """SELECT Status FROM jobs("type"="insert") WHERE Name='${jobName}'""")[0][0] == "RUNNING"
        def createTable = (sql """SHOW CREATE TABLE ${currentDb}.${dorisTable}""")[0][1] as String
        assert !createTable.contains("`city`")
        order_qt_final_rows """ SELECT * FROM ${currentDb}.${dorisTable} ORDER BY name """

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    }
}
