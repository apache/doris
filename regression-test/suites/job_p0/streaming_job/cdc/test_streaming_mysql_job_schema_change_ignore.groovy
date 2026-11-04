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

suite("test_streaming_mysql_job_schema_change_ignore", "p0,external,mysql,external_docker,external_docker_mysql,nondatalake") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }
    String mysqlPort = context.config.otherConfigs.get("mysql_57_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String driverUrl = "https://${getS3BucketName()}.${getS3Endpoint()}/regression/jdbc_driver/mysql-connector-j-8.4.0.jar"
    def currentDb = (sql "SELECT DATABASE()")[0][0]
    sql "DROP JOB IF EXISTS WHERE jobname='test_mysql_schema_ignore'"
    sql "DROP TABLE IF EXISTS mysql_schema_ignore FORCE"
    connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysqlPort}") {
        sql "CREATE DATABASE IF NOT EXISTS test_cdc_db"
        sql "DROP TABLE IF EXISTS test_cdc_db.mysql_schema_ignore"
        sql """CREATE TABLE test_cdc_db.mysql_schema_ignore (
            id INT PRIMARY KEY, name VARCHAR(50), removed VARCHAR(50))"""
        sql "INSERT INTO test_cdc_db.mysql_schema_ignore VALUES (1, 'initial', 'old')"
    }
    def succeedCount = {
        (sql """SELECT SucceedTaskCount FROM jobs("type"="insert") WHERE Name='test_mysql_schema_ignore'""")[0][0].toLong()
    }
    def waitForCommit = { long previous ->
        Awaitility.await().atMost(180, SECONDS).pollInterval(1, SECONDS).until({ succeedCount() > previous })
    }
    def waitForRow = { int id ->
        Awaitility.await().atMost(180, SECONDS).pollInterval(1, SECONDS).until({
            (sql "SELECT COUNT(*) FROM mysql_schema_ignore WHERE id=${id}")[0][0].toLong() == 1
        })
    }
    sql """CREATE JOB test_mysql_schema_ignore PROPERTIES ("max_interval" = "5") ON STREAMING
        FROM MYSQL (
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysqlPort}",
            "driver_url" = "${driverUrl}", "driver_class" = "com.mysql.cj.jdbc.Driver",
            "user" = "root", "password" = "123456", "database" = "test_cdc_db",
            "include_tables" = "mysql_schema_ignore", "offset" = "initial",
            "schema_change_behavior" = "ignore"
        ) TO DATABASE ${currentDb} ("table.create.properties.replication_num" = "1")"""
    waitForRow(1)
    waitForCommit(Math.max(1L, succeedCount()))
    connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysqlPort}") {
        sql "ALTER TABLE test_cdc_db.mysql_schema_ignore ADD COLUMN skipped INT"
        sql "ALTER TABLE test_cdc_db.mysql_schema_ignore DROP COLUMN removed"
        sql "ALTER TABLE test_cdc_db.mysql_schema_ignore MODIFY COLUMN name VARCHAR(150)"
        sql "INSERT INTO test_cdc_db.mysql_schema_ignore VALUES (2, 'ignored', 20)"
    }
    waitForRow(2)
    waitForCommit(succeedCount())
    qt_ignored_columns """SELECT column_name FROM information_schema.columns
        WHERE table_schema='${currentDb}' AND table_name='mysql_schema_ignore' ORDER BY column_name"""
    sql "PAUSE JOB WHERE jobname='test_mysql_schema_ignore'"
    sql "RESUME JOB WHERE jobname='test_mysql_schema_ignore'"
    connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysqlPort}") {
        sql "INSERT INTO test_cdc_db.mysql_schema_ignore VALUES (3, 'rebuilt', 30)"
    }
    waitForRow(3)
    waitForCommit(succeedCount())
    sql "PAUSE JOB WHERE jobname='test_mysql_schema_ignore'"
    sql """ALTER JOB test_mysql_schema_ignore FROM MYSQL ("schema_change_behavior" = "evolve")
        TO DATABASE ${currentDb}"""
    long beforeResume = succeedCount()
    sql "RESUME JOB WHERE jobname='test_mysql_schema_ignore'"
    waitForCommit(beforeResume)
    connect("root", "123456", "jdbc:mysql://${externalEnvIp}:${mysqlPort}") {
        sql "ALTER TABLE test_cdc_db.mysql_schema_ignore ADD COLUMN new_value INT"
        sql "INSERT INTO test_cdc_db.mysql_schema_ignore VALUES (4, 'evolved', 40, 400)"
    }
    waitForRow(4)
    waitForCommit(succeedCount())
    qt_evolved_columns """SELECT column_name FROM information_schema.columns
        WHERE table_schema='${currentDb}' AND table_name='mysql_schema_ignore' ORDER BY column_name"""
    qt_evolved_rows "SELECT id, name, removed, new_value FROM mysql_schema_ignore ORDER BY id"
    qt_running """SELECT Status FROM jobs("type"="insert") WHERE Name='test_mysql_schema_ignore'"""
    sql "DROP JOB IF EXISTS WHERE jobname='test_mysql_schema_ignore'"
}
