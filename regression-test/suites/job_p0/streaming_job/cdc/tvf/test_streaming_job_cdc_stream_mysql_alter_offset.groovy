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

/**
 * Test ALTER JOB with JSON binlog offset for cdc_stream TVF path.
 *
 * Scenario:
 *   1. Create job with initial offset, wait for snapshot sync.
 *   2. Get current binlog position, insert new data.
 *   3. PAUSE -> ALTER with JSON binlog offset via PROPERTIES -> RESUME.
 *   4. Verify new data synced.
 */
suite("test_streaming_job_cdc_stream_mysql_alter_offset", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_job_cdc_stream_mysql_alter_offset_name"
    def currentDb = (sql "select database()")[0][0]
    def dorisTable = "cdc_stream_alter_offset_tbl"
    def mysqlDb = "test_cdc_db"
    def mysqlTable = "cdc_stream_alter_offset_src"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${dorisTable} force"""

    sql """
        CREATE TABLE IF NOT EXISTS ${currentDb}.${dorisTable} (
            `id`   int NULL,
            `name` varchar(200) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS AUTO
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

        // prepare source table with snapshot data
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${mysqlTable}"""
            sql """CREATE TABLE ${mysqlDb}.${mysqlTable} (
                      `id` int NOT NULL,
                      `name` varchar(200) DEFAULT NULL,
                      PRIMARY KEY (`id`)
                  ) ENGINE=InnoDB"""
            sql """INSERT INTO ${mysqlDb}.${mysqlTable} VALUES (1, 'alice'), (2, 'bob')"""
        }

        // Step 1: Create job with initial offset via cdc_stream TVF
        sql """
            CREATE JOB ${jobName}
            ON STREAMING DO INSERT INTO ${currentDb}.${dorisTable} (id, name)
            SELECT id, name FROM cdc_stream(
                "type"         = "mysql",
                "jdbc_url"     = "jdbc:mysql://${externalEnvIp}:${mysql_port}",
                "driver_url"   = "${driver_url}",
                "driver_class" = "com.mysql.cj.jdbc.Driver",
                "user"         = "root",
                "password"     = "123456",
                "database"     = "${mysqlDb}",
                "table"        = "${mysqlTable}",
                "offset"       = "initial"
            )
        """

        // wait for snapshot sync
        try {
            Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
                def cnt = sql """select SucceedTaskCount from jobs("type"="insert") where Name='${jobName}' and ExecuteType='STREAMING'"""
                log.info("SucceedTaskCount: " + cnt)
                cnt.size() == 1 && (cnt.get(0).get(0) as int) >= 1
            })
        } catch (Exception ex) {
            log.info("job: " + (sql """select * from jobs("type"="insert") where Name='${jobName}'"""))
            log.info("tasks: " + (sql """select * from tasks("type"="insert") where JobName='${jobName}'"""))
            throw ex
        }

        // verify snapshot data
        Awaitility.await().atMost(60, SECONDS).pollInterval(2, SECONDS).until({
            def result = sql """SELECT count(*) FROM ${currentDb}.${dorisTable}"""
            return result[0][0] >= 2
        })

        // Step 2: Get current binlog position and insert new data
        def binlogFile = ""
        def binlogPos = ""
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            def masterStatus = sql """SHOW MASTER STATUS"""
            binlogFile = masterStatus[0][0]
            binlogPos = masterStatus[0][1].toString()
            log.info("Binlog position for ALTER: file=${binlogFile}, pos=${binlogPos}")
            sql """INSERT INTO ${mysqlDb}.${mysqlTable} VALUES (10, 'alter_tvf_1')"""
            sql """INSERT INTO ${mysqlDb}.${mysqlTable} VALUES (11, 'alter_tvf_2')"""
        }

        // Step 3: PAUSE -> ALTER with JSON binlog offset -> RESUME
        sql "PAUSE JOB where jobname = '${jobName}'"
        Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).until({
            def jobStatus = sql """select status from jobs("type"="insert") where Name='${jobName}'"""
            return jobStatus[0][0] == "PAUSED"
        })
        def offsetJson = """{"file":"${binlogFile}","pos":"${binlogPos}"}"""
        log.info("ALTER TVF job offset: ${offsetJson}")
        sql """ALTER JOB ${jobName}
                PROPERTIES('offset' = '${offsetJson}')
            """

        // verify currentOffset changed in show jobs
        def jobInfo = sql """select currentOffset from jobs("type"="insert") where Name='${jobName}'"""
        log.info("currentOffset after ALTER: " + jobInfo[0][0])

        sql "RESUME JOB where jobname = '${jobName}'"

        // Step 4: Verify new data synced
        Awaitility.await().atMost(120, SECONDS).pollInterval(2, SECONDS).until({
            def result = sql """SELECT count(*) FROM ${currentDb}.${dorisTable} WHERE id IN (10, 11)"""
            return result[0][0] >= 2
        })
        def alterRows = sql """SELECT * FROM ${currentDb}.${dorisTable} WHERE id IN (10, 11) order by id"""
        log.info("alterRows: " + alterRows)
        assert alterRows.size() == 2

        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        sql """drop table if exists ${currentDb}.${dorisTable} force"""

        // cleanup MySQL source table
        connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
            sql """DROP TABLE IF EXISTS ${mysqlDb}.${mysqlTable}"""
        }
    }
}
