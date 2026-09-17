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

suite("test_streaming_mysql_job_reload_source_schema",
        "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    def jobName = "test_streaming_mysql_job_reload_source_schema"
    def currentDb = (sql "SELECT DATABASE()")[0][0]
    def mysqlDb = "test_cdc_db"
    def user = "test_streaming_mysql_reload_user"
    String mysqlPort = context.config.otherConfigs.get("mysql_57_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String mysqlUrl = "jdbc:mysql://${externalEnvIp}:${mysqlPort}?allowPublicKeyRetrieval=true&useSSL=false"
    String driverUrl = "https://${getS3BucketName()}.${getS3Endpoint()}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"
    def tokens = context.config.jdbcUrl.split('/')
    def dorisUrl = tokens[0] + "//" + tokens[2] + "/" + currentDb + "?"

    sql """DROP JOB IF EXISTS WHERE jobname = '${jobName}'"""
    sql "DROP TABLE IF EXISTS test_streaming_mysql_reload_tbl FORCE"
    sql """DROP USER IF EXISTS '${user}'"""
    sql """CREATE USER '${user}' IDENTIFIED BY '123456'"""
    // Keep ALTER privilege absent throughout recovery; the administrator repairs the target.
    sql """GRANT select_priv,load_priv,create_priv ON ${currentDb}.* TO ${user}"""
    if (isCloudMode()) {
        def clusters = sql_return_maparray "SHOW CLUSTERS"
        for (item in clusters) {
            if (item.is_current.equalsIgnoreCase("TRUE")) {
                sql """GRANT USAGE_PRIV ON CLUSTER `${item.cluster}` TO ${user}"""
                break
            }
        }
    }

    connect("root", "123456", mysqlUrl) {
        sql """CREATE DATABASE IF NOT EXISTS ${mysqlDb}"""
        sql """DROP TABLE IF EXISTS ${mysqlDb}.test_streaming_mysql_reload_tbl"""
        sql """CREATE TABLE ${mysqlDb}.test_streaming_mysql_reload_tbl (
                   id INT NOT NULL PRIMARY KEY,
                   name VARCHAR(100)
               ) ENGINE=InnoDB"""
        sql """INSERT INTO ${mysqlDb}.test_streaming_mysql_reload_tbl VALUES (1, 'before_ddl')"""
    }

    try {
        connect(user, "123456", dorisUrl) {
            sql """CREATE JOB ${jobName}
                   PROPERTIES ("max_interval" = "3")
                   ON STREAMING
                   FROM MYSQL (
                       "jdbc_url" = "${mysqlUrl}",
                       "driver_url" = "${driverUrl}",
                       "driver_class" = "com.mysql.cj.jdbc.Driver",
                       "user" = "root",
                       "password" = "123456",
                       "database" = "${mysqlDb}",
                       "include_tables" = "test_streaming_mysql_reload_tbl",
                       "offset" = "initial"
                   )
                   TO DATABASE ${currentDb} (
                       "table.create.properties.replication_num" = "1"
                   )"""
        }
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
            def jobs = sql """SELECT SucceedTaskCount FROM jobs("type"="insert") WHERE Name='${jobName}'"""
            jobs.size() == 1 && (jobs[0][0] as long) >= 1
        })
        Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
            def rows = sql "SELECT name FROM test_streaming_mysql_reload_tbl WHERE id = 1"
            rows.size() == 1 && rows[0][0] == "before_ddl"
        })

        connect("root", "123456", mysqlUrl) {
            sql """ALTER TABLE ${mysqlDb}.test_streaming_mysql_reload_tbl ADD COLUMN recovery_value VARCHAR(50)"""
            sql """INSERT INTO ${mysqlDb}.test_streaming_mysql_reload_tbl
                   VALUES (2, 'after_ddl', 'before_resume')"""
        }
        String failureMessage = ""
        Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
            def jobs = sql """SELECT Status, ErrorMsg FROM jobs("type"="insert") WHERE Name='${jobName}'"""
            log.info("schema change failure: " + jobs)
            if (jobs.size() != 1 || jobs[0][0] != "PAUSED") {
                return false
            }
            failureMessage = parseJson(jobs[0][1].toString()).msg.toString()
            failureMessage.contains("Failed to execute Doris DDL")
                    && failureMessage.contains("ADD COLUMN")
                    && failureMessage.contains("recovery_value")
                    && failureMessage.contains("ALTER TABLE command denied")
        })

        // Use the complete offset from ErrorMsg, including any event/row restart fields.
        def offsetMatcher = failureMessage =~ /Source offset: (\{.*\})$/
        assert offsetMatcher.find() : "Missing source offset in ErrorMsg: ${failureMessage}"
        String recoveryOffset = offsetMatcher.group(1)
        def parsedOffset = parseJson(recoveryOffset)
        assert parsedOffset.file && (parsedOffset.pos as long) > 0 : "Invalid source offset: ${recoveryOffset}"

        // Convert the failure pause into an explicit manual pause so auto-resume cannot race
        // with the administrator repairing the target schema.
        Awaitility.await().atMost(180, SECONDS).pollInterval(1, SECONDS).until({
            try {
                def status = (sql """SELECT Status FROM jobs("type"="insert") WHERE Name='${jobName}'""")[0][0]
                if (status == "PAUSED") {
                    sql """RESUME JOB WHERE jobname = '${jobName}'"""
                }
                sql """PAUSE JOB WHERE jobname = '${jobName}'"""
                return (sql """SELECT Status FROM jobs("type"="insert") WHERE Name='${jobName}'""")[0][0] == "PAUSED"
            } catch (Exception e) {
                log.info("retrying manual pause after schema change failure: " + e.getMessage())
                return false
            }
        })

        sql "ALTER TABLE test_streaming_mysql_reload_tbl ADD COLUMN recovery_value VARCHAR(50)"
        Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
            (sql "DESC test_streaming_mysql_reload_tbl").any { it[0] == "recovery_value" }
        })
        sql """ALTER JOB ${jobName}
               PROPERTIES (
                   "offset" = '${recoveryOffset}',
                   "reload_source_schema" = "true"
               )"""
        sql """RESUME JOB WHERE jobname = '${jobName}'"""

        connect("root", "123456", mysqlUrl) {
            sql """INSERT INTO ${mysqlDb}.test_streaming_mysql_reload_tbl
                   VALUES (3, 'after_resume', 'reloaded_schema')"""
        }
        // Check both the first row after the failed DDL and subsequent incremental data.
        Awaitility.await().atMost(180, SECONDS).pollInterval(2, SECONDS).until({
            def rows = sql "SELECT id, name, recovery_value FROM test_streaming_mysql_reload_tbl ORDER BY id"
            rows == [[1, "before_ddl", null], [2, "after_ddl", "before_resume"],
                     [3, "after_resume", "reloaded_schema"]]
        })
    } finally {
        sql """DROP JOB IF EXISTS WHERE jobname = '${jobName}'"""
        sql """DROP USER IF EXISTS '${user}'"""
    }
}
