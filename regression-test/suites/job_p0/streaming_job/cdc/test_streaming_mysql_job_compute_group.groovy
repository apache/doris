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

// Coverage for the non-TVF (source-to-target) path: verifies compute_group is
// rejected in non-cloud mode, persisted on CREATE JOB ... FROM MYSQL in cloud
// mode, and that the bound job runs end-to-end, exercising JdbcSourceOffsetProvider
// RPCs and StreamingMultiTblTask.sendWriteRequest. Lifecycle checks (empty /
// invalid / ALTER / PAUSE) are covered by test_streaming_insert_job_compute_group.
suite("test_streaming_mysql_job_compute_group",
        "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    def jobName = "test_streaming_mysql_cg_job"
    def currentDb = (sql "select database()")[0][0]
    def tableName = "mysql_cg_normal1"
    def mysqlDb = "test_cdc_cg_db"

    sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
    sql """drop table if exists ${currentDb}.${tableName} force"""

    // Non-cloud mode: compute_group must be rejected regardless of MySQL availability
    if (!isCloudMode()) {
        test {
            sql """CREATE JOB ${jobName}
                    PROPERTIES ("compute_group" = "any_group")
                    ON STREAMING
                    FROM MYSQL (
                        "jdbc_url" = "jdbc:mysql://127.0.0.1:3316",
                        "driver_url" = "nop",
                        "driver_class" = "com.mysql.cj.jdbc.Driver",
                        "user" = "root",
                        "password" = "nop",
                        "database" = "${mysqlDb}",
                        "include_tables" = "${tableName}"
                    )
                    TO DATABASE ${currentDb} (
                      "table.create.properties.replication_num" = "1"
                    )
                """
            exception "only supported in cloud mode"
        }
        return
    }

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String mysql_port = context.config.otherConfigs.get("mysql_57_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"

    def clusterRows = sql "show clusters"
    assert clusterRows.size() >= 1 : "cloud mode expects at least one cluster"
    def cg = clusterRows.get(0).get(0)

    connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysql_port}") {
        sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
        sql """DROP TABLE IF EXISTS ${mysqlDb}.${tableName}"""
        sql """CREATE TABLE ${mysqlDb}.${tableName} (
              `name` varchar(200) NOT NULL,
              `age` int DEFAULT NULL,
              PRIMARY KEY (`name`)
            ) ENGINE=InnoDB"""
        sql """INSERT INTO ${mysqlDb}.${tableName} (name, age) VALUES ('A1', 1)"""
        sql """INSERT INTO ${mysqlDb}.${tableName} (name, age) VALUES ('B1', 2)"""
    }

    try {
        sql """CREATE JOB ${jobName}
                PROPERTIES ("compute_group" = "${cg}")
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

        def props = sql """select properties from jobs("type"="insert") where Name='${jobName}'"""
        assert props.get(0).get(0).contains("\"compute_group\":\"${cg}\"")

        try {
            Awaitility.await().atMost(300, SECONDS).pollInterval(1, SECONDS).until({
                def cnt = sql """select SucceedTaskCount from jobs("type"="insert") where Name='${jobName}'"""
                cnt.size() == 1 && Integer.parseInt(cnt.get(0).get(0).toString()) >= 1
            })
        } catch (Exception ex) {
            log.info("job: " + sql("""select * from jobs("type"="insert") where Name='${jobName}'"""))
            log.info("task: " + sql("""select * from tasks("type"="insert") where JobName='${jobName}'"""))
            throw ex
        }

        def rows = (sql """SELECT count(*) FROM ${currentDb}.${tableName}""").get(0).get(0) as long
        assertTrue(rows >= 2, "expected snapshot rows in target table")
    } finally {
        sql """DROP JOB IF EXISTS where jobname = '${jobName}'"""
        sql """drop table if exists ${currentDb}.${tableName} force"""
    }
}
