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

suite("test_streaming_mysql_job_server_id", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    def mysql_port = context.config.otherConfigs.get("mysql_57_port")
    def externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    def s3_endpoint = getS3Endpoint()
    def bucket = getS3BucketName()
    def driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

    def currentDb = (sql "select database()")[0][0]
    def mysqlDb = "test_server_id_db"
    def srcTable = "user_server_id"

    // Prepare source schema + initial data once
    connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
        sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
        sql """DROP TABLE IF EXISTS ${mysqlDb}.${srcTable}"""
        sql """CREATE TABLE ${mysqlDb}.${srcTable} (
              `name` varchar(200) NOT NULL,
              `age` int DEFAULT NULL,
              PRIMARY KEY (`name`)
            ) ENGINE=InnoDB"""
        sql """INSERT INTO ${mysqlDb}.${srcTable} VALUES ('A', 1), ('B', 2)"""
    }

    def buildCreateJob = { String jobName, String extraProps ->
        """CREATE JOB ${jobName}
            ON STREAMING
            FROM MYSQL (
                "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}",
                "driver_url" = "${driver_url}",
                "driver_class" = "com.mysql.cj.jdbc.Driver",
                "user" = "root",
                "password" = "123456",
                "database" = "${mysqlDb}",
                "include_tables" = "${srcTable}",
                ${extraProps}
            )
            TO DATABASE ${currentDb} (
              "table.create.properties.replication_num" = "1"
            )
        """
    }

    def assertCreateFails = { String jobName, String extraProps, String expectFragment ->
        sql "DROP JOB IF EXISTS where jobname = '${jobName}'"
        Exception thrown = null
        try {
            sql buildCreateJob(jobName, extraProps)
        } catch (Exception ex) {
            thrown = ex
        }
        assert thrown != null, "CREATE JOB ${jobName} should have failed"
        def msg = String.valueOf(thrown.message).toLowerCase()
        assert msg.contains(expectFragment.toLowerCase()),
                "${jobName}: expected error containing '${expectFragment}', got: ${thrown.message}"
    }

    def runHappyPath = { String jobName, String extraProps ->
        sql "DROP JOB IF EXISTS where jobname = '${jobName}'"
        sql "DROP TABLE IF EXISTS ${currentDb}.${srcTable} FORCE"
        try {
            sql buildCreateJob(jobName, extraProps + ', "offset" = "initial"')
            Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
                def cnt = sql """
                    select SucceedTaskCount from jobs("type"="insert")
                    where Name = '${jobName}' and ExecuteType='STREAMING'
                """
                cnt.size() == 1 && Integer.parseInt(cnt.get(0).get(0).toString()) >= 1
            })
            def rows = sql """SELECT name FROM ${currentDb}.${srcTable} ORDER BY name"""
            assert rows.size() == 2, "${jobName}: expected 2 rows, got ${rows.size()}"
        } finally {
            sql "DROP JOB IF EXISTS where jobname = '${jobName}'"
        }
    }

    // ─── Section 1: FE validator rejects bad server_id at CREATE ─────────────
    assertCreateFails("test_serverid_reject_format",
            '"offset" = "initial", "server_id" = "abc"', "server_id")
    assertCreateFails("test_serverid_reject_zero",
            '"offset" = "initial", "server_id" = "0"', "server_id")
    assertCreateFails("test_serverid_reject_backward",
            '"offset" = "initial", "server_id" = "5408-5400"', "server_id")
    assertCreateFails("test_serverid_reject_width",
            '"offset" = "initial", "server_id" = "99500", "snapshot_parallelism" = "2"',
            "snapshot_parallelism")

    // ─── Section 2: happy path — job runs, data syncs ────────────────────────
    runHappyPath("test_serverid_single", '"server_id" = "99001"')
    runHappyPath("test_serverid_range",
            '"server_id" = "99100-99103", "snapshot_parallelism" = "4", "snapshot_split_size" = "1"')
    runHappyPath("test_serverid_default", '"snapshot_parallelism" = "2"')
}
